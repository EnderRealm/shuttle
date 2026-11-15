//go:build cgo

package reap

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

// The real table answers for another process of this uid with its parent and
// its exec path, which kern.procargs2 would refuse for another uid.
func TestUIDProcessesReadsParentAndExecPath(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	var got UserProcess
	waitUntil(t, func() bool {
		procs, err := uidProcesses(os.Getuid())
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(procs, func(p UserProcess) bool { return p.PID == cmd.Process.Pid })
		if i >= 0 {
			got = procs[i]
		}
		return i >= 0 && got.Path == "/bin/sleep"
	})
	if got.PPID != os.Getpid() || got.Comm != "sleep" || got.Start.IsZero() {
		t.Fatalf("listed %+v, want sleep under pid %d", got, os.Getpid())
	}
	if path, err := execPath(1); err != nil || path != "/sbin/launchd" {
		t.Fatalf("execPath(1) = %q %v, want root's launchd read across uids", path, err)
	}
}

// resolved is dir with its symlinks resolved: the test temp dir sits under
// /var, an alias of /private/var.
func resolved(t *testing.T, dir string) string {
	t.Helper()
	out, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// legacyClaim claims a hosted segment as one launched before agent users
// was: it holds no agent user.
func legacyClaim(t *testing.T, s *ledger.Store, ticket, repo string) *ledger.WeftRun {
	t.Helper()
	root := resolved(t, t.TempDir())
	run, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticket, Project: "demo", Repo: repo, Branch: ticket,
		Worktree: filepath.Join(root, "worktree"), RunDirRoot: filepath.Join(root, "runs"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(run.Segments[0].RunDir.String, 0o700); err != nil {
		t.Fatal(err)
	}
	return run
}

// weftRow is a hosted segment launched before agent users, left running by a
// previous daemon: it holds no agent user, and its recorded pid is gone.
func weftRow(t *testing.T, s *ledger.Store, ticket, repo string) *ledger.WeftRun {
	t.Helper()
	run := legacyClaim(t, s, ticket, repo)
	gone := exec.Command("/usr/bin/true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkWeftRunning(run.ID, 1, gone.Process.Pid, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	return run
}

// assertRepositoryHeld checks the queue's restart step releases nothing and a
// new claim on repo is refused while run holds it.
func assertRepositoryHeld(t *testing.T, s *ledger.Store, run *ledger.WeftRun, repo string) {
	t.Helper()
	released, _, _, err := s.RestartWeftQueue(ledger.WeftDispatch{}, func(string, string) (string, error) { return "", nil })
	if err != nil || len(released) != 0 {
		t.Fatalf("queue restart released %+v, %v", released, err)
	}
	_, err = s.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after", Project: "demo", Repo: repo, Branch: "demo/after",
		Worktree: filepath.Join(t.TempDir(), "after"), RunDirRoot: t.TempDir(),
	})
	var busy *ledger.RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != run.ID {
		t.Fatalf("claim behind a held pre-agent-user segment = %v, want the repository busy", err)
	}
}

// A segment launched before agent users holds no user to kill, so a detached
// descendant of it can outlive its group: the pass holds the row, naming the
// worktree and the marker that releases it, and the repository stays
// reserved. Once the operator creates the marker, the next pass closes the
// row and the reconcile lets the repository go.
func TestRestartHoldsAPreAgentUserSegmentUntilItsReleaseMarker(t *testing.T) {
	s := newStore(t)
	repo := "/tmp/shuttle-contain-repo"
	run := weftRow(t, s, "demo/restart", repo)
	ledgerRun := run.Segments[0].LedgerRunID
	marker := filepath.Join(run.Segments[0].RunDir.String, releaseMarker)
	outcomes := restart(t, s)
	if len(outcomes) != 1 || outcomes[0].RunID != ledgerRun || outcomes[0].Action != ActionUnresolved ||
		!strings.Contains(outcomes[0].Detail, "worktree "+run.Worktree+" stays reserved") ||
		!strings.Contains(outcomes[0].Detail, "create "+marker+" and restart the daemon") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertInFlight(t, s, ledgerRun)
	assertRepositoryHeld(t, s, run, repo)

	if again := restart(t, s); len(again) != 1 || again[0].Action != ActionUnresolved {
		t.Fatalf("second pass without the marker = %+v", again)
	}
	assertInFlight(t, s, ledgerRun)

	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomes = restart(t, s)
	if len(outcomes) != 1 || outcomes[0].RunID != ledgerRun || outcomes[0].Action != ActionDead ||
		!strings.Contains(outcomes[0].Detail, "closed on the operator's release marker "+marker) {
		t.Fatalf("outcomes with the marker = %+v", outcomes)
	}
	assertUnknown(t, s, ledgerRun)
	if err := s.ReconcileWeftUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after", Project: "demo", Repo: repo, Branch: "demo/after",
		Worktree: filepath.Join(t.TempDir(), "after"), RunDirRoot: t.TempDir(),
	}); err != nil {
		t.Fatalf("repository not released once the row closed on its marker: %v", err)
	}
}

// A live pre-agent-user worker, established by its --sandbox-profile, is
// still killed by process group; the row is held after the kill all the same,
// since the group being empty is not the segment being gone.
func TestRestartKillsALivePreAgentUserWorkerAndHoldsItsRow(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	repo := "/tmp/shuttle-contain-repo"
	run := legacyClaim(t, s, "demo/restart", repo)
	profile := filepath.Join(run.Segments[0].RunDir.String, sandboxProfileFile)
	cmd := spawn(t, []string{"sh", "-c", "sleep 300 & wait", "weft-worker", sandboxProfileFlag, profile}, true)
	pid := cmd.Process.Pid
	waitUntil(t, func() bool {
		got, err := processArgv(pid)
		return err == nil && carries(got, sandboxProfileFlag, profile)
	})
	if err := s.MarkWeftRunning(run.ID, 1, pid, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	sent := recordSignals(t)
	outcomes := restart(t, s)
	ledgerRun := run.Segments[0].LedgerRunID
	if len(outcomes) != 1 || outcomes[0].Action != ActionUnresolved ||
		!strings.Contains(outcomes[0].Detail, "create "+filepath.Join(run.Segments[0].RunDir.String, releaseMarker)) {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertSignals(t, sent, signal(-pid, syscall.SIGTERM))
	waitGone(t, "worker", pid)
	assertInFlight(t, s, ledgerRun)
	assertRepositoryHeld(t, s, run, repo)
}

// A ledger read that cannot say whether a row is a pre-agent-user segment
// stops the pass with the row held, nothing signalled and nothing closed.
func TestRestartHoldsARowWhoseLegacyLookupFails(t *testing.T) {
	s := newStore(t)
	run := weftRow(t, s, "demo/restart", "/tmp/shuttle-contain-repo")
	ledgerRun := run.Segments[0].LedgerRunID
	l := &failingLegacyLedger{Store: s}
	outcomes, err := Restart(l)
	if err == nil || !strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("restart error = %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != ActionUnresolved ||
		!strings.Contains(outcomes[0].Detail, "pre-agent-user segment: database is locked") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	if len(l.marked) != 0 {
		t.Fatalf("MarkUnknown closed %v", l.marked)
	}
	assertInFlight(t, s, ledgerRun)
}

// A pre-agent-user row with no absolute run_dir has nowhere to carry the
// marker, and is held rather than closed as identify would close any other.
func TestLegacyRowWithNoRunDirIsHeld(t *testing.T) {
	for _, dir := range []string{"", "runs/r1"} {
		run := &ledger.Run{ID: "r1"}
		run.RunDir.String, run.RunDir.Valid = dir, dir != ""
		action, detail := legacyRow(run, "/w", ActionForeign, "no run_dir recorded")
		if action != ActionUnresolved || !strings.Contains(detail, "worktree /w stays reserved") {
			t.Fatalf("run_dir %q: %s %q", dir, action, detail)
		}
	}
}

// failingLegacyLedger is the real ledger with WeftLegacyCodingWorktree
// failing, and a record of every row MarkUnknown was asked to close.
type failingLegacyLedger struct {
	*ledger.Store
	marked []string
}

func (l *failingLegacyLedger) WeftLegacyCodingWorktree(string) (string, bool, error) {
	return "", false, errors.New("database is locked")
}

func (l *failingLegacyLedger) MarkUnknown(id string) error {
	l.marked = append(l.marked, id)
	return l.Store.MarkUnknown(id)
}

func waitUntil(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// agentRow is a hosted segment running as weftagent1, claimed as uid 601,
// left by a previous daemon: its worker, a live group leader carrying the
// segment's --run and --segment, a table in which weftagent1 runs a setsid'd
// orphan, and a directory in which weftagent1 is still uid 601.
func agentRow(t *testing.T, s *ledger.Store, argv func(runID string) []string) (*ledger.WeftRun, int, *fakeTable) {
	t.Helper()
	root := resolved(t, t.TempDir())
	run, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/agent", Project: "demo", Repo: "/tmp/shuttle-agent-repo", Branch: "demo/agent",
		Worktree: filepath.Join(root, "worktree"), RunDirRoot: filepath.Join(root, "runs"),
		AgentUser: ledger.WeftAgentUser{Name: "weftagent1", UID: 601},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := spawn(t, argv(run.ID), true)
	waitUntil(t, func() bool {
		got, err := processArgv(cmd.Process.Pid)
		return err == nil && carries(got, weftRunFlag, run.ID)
	})
	if err := s.MarkWeftRunning(run.ID, 1, cmd.Process.Pid, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	f := newFakeTable(map[string][]UserProcess{"weftagent1": {setsidOrphan, distnoted}})
	prior := killUser
	killUser = f.killer().KillUser
	t.Cleanup(func() { killUser = prior })
	fakeDirectory(t, map[string]int{"weftagent1": 601})
	return run, cmd.Process.Pid, f
}

func workerArgv(runID string, segment int) []string {
	return []string{"sh", "-c", "sleep 300 & wait", "weft-worker", "segment", weftRunFlag, runID, weftSegmentFlag, strconv.Itoa(segment)}
}

// The daemon-restart pass over a segment that ran as an agent user: every
// process of the user is killed before the worker's group is signalled, the
// worker is established by its run and segment with no sandbox profile, and
// the row closes once the user has nothing left, a setsid'd orphan included.
func TestRestartKillsTheAgentUserBeforeTheWorkerGroup(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	run, pid, f := agentRow(t, s, func(id string) []string { return workerArgv(id, 1) })
	var order []string
	was := sendSignal
	sendSignal = func(p int, sig syscall.Signal) error {
		order = append(order, strconv.Itoa(len(f.calls))+" "+sig.String())
		return was(p, sig)
	}
	t.Cleanup(func() { sendSignal = was })
	outcomes := restart(t, s)
	ledgerRun := run.Segments[0].LedgerRunID
	if len(outcomes) != 1 || outcomes[0].RunID != ledgerRun || outcomes[0].Action != ActionKilled ||
		!strings.Contains(outcomes[0].Detail, "agent user weftagent1: killed 1 process(es) 702(/bin/sleep)") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	if left := f.left("weftagent1"); !slices.Equal(left, []UserProcess{distnoted}) {
		t.Fatalf("weftagent1 still runs %v", left)
	}
	if len(order) == 0 || !strings.HasPrefix(order[0], "3 ") {
		t.Fatalf("signals at table calls %v, want the worker group signalled only after the user's kill and its listing", order)
	}
	if n := slices.Index(f.calls, "kill weftagent1"); n < 0 || strings.Count(strings.Join(f.calls, ","), "kill weftagent1") < 2 {
		t.Fatalf("calls = %v, want the user killed before the group and again after it", f.calls)
	}
	waitGone(t, "worker", pid)
	assertUnknown(t, s, ledgerRun)
}

// A user whose process outlives the kill holds the row kill_failed, so the
// repository stays reserved and the user stays held; the worker's group is
// left for the next pass, whose kill of the user comes first again.
func TestRestartHoldsARowWhoseAgentUserOutlivesTheKill(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	run, pid, f := agentRow(t, s, func(id string) []string { return workerArgv(id, 1) })
	f.stubborn[setsidOrphan.PID] = true
	sent := recordSignals(t)
	outcomes := restart(t, s)
	ledgerRun := run.Segments[0].LedgerRunID
	if len(outcomes) != 1 || outcomes[0].Action != ActionKillFailed ||
		!strings.Contains(outcomes[0].Detail, "702(/bin/sleep)") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertSignals(t, sent)
	if !alive(pid) {
		t.Fatalf("worker %d was signalled", pid)
	}
	assertInFlight(t, s, ledgerRun)
	released, _, _, err := s.RestartWeftQueue(ledger.WeftDispatch{}, func(string, string) (string, error) { return "", nil })
	if err != nil || len(released) != 0 {
		t.Fatalf("queue restart released %+v, %v", released, err)
	}
	if agent, ok, err := s.WeftSegmentAgent(ledgerRun); !ok || err != nil || agent.AgentUser != "weftagent1" {
		t.Fatalf("held row's agent = %+v %v %v", agent, ok, err)
	}
}

// An account renamed, or its name taken by another account, while its
// segment's row is left in flight with a process still running as its uid:
// the pass kills nothing as the recorded name and holds the row kill_failed,
// its worker unsignalled. The uid stays held whatever the account is called
// now, so with spare capacity dispatch hands the renamed account to no
// segment, while another free user still is.
func TestRestartHoldsTheUIDOfARenamedAgentUser(t *testing.T) {
	requireIdentity(t)
	for _, tt := range []struct {
		name      string
		directory map[string]int
		want      string
	}{
		{name: "renamed", directory: map[string]int{"weftagent9": 601, "weftagent2": 602},
			want: "agent user weftagent1 was uid 601 when its segment was claimed and no longer resolves (user: unknown user weftagent1); the uid stays held"},
		{name: "replaced", directory: map[string]int{"weftagent1": 777, "weftagent9": 601, "weftagent2": 602},
			want: "agent user weftagent1 resolves to uid 777, not the uid 601 its segment was claimed with, which stays held"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t)
			run, pid, f := agentRow(t, s, func(id string) []string { return workerArgv(id, 1) })
			fakeDirectory(t, tt.directory)
			sent := recordSignals(t)
			outcomes := restart(t, s)
			ledgerRun := run.Segments[0].LedgerRunID
			if len(outcomes) != 1 || outcomes[0].Action != ActionKillFailed || !strings.Contains(outcomes[0].Detail, tt.want) {
				t.Fatalf("outcomes = %+v, want kill_failed with %q", outcomes, tt.want)
			}
			assertUntouched(t, s, ledgerRun, pid, f, sent)
			got := dispatchBeside(t, s, []ledger.WeftAgentUser{{Name: "weftagent9", UID: 601}, {Name: "weftagent2", UID: 602}})
			if !slices.Equal(got, []ledger.WeftAgentUser{{Name: "weftagent2", UID: 602}, {}}) {
				t.Fatalf("dispatched as %v, want weftagent2 and the second entry waiting: weftagent9 is uid 601, held by the row", got)
			}
			assertHeld(t, s, ledgerRun)
		})
	}
}

// A segment claimed under schema 11 records no uid, and is left in flight
// with a process still running as its account across the upgrade. When the
// account was renamed before the upgrade, the first pass cannot resolve the
// recorded name: the row is held unresolved, nothing killed, and since its
// uid could be any configured user's, dispatch hands out none, though slots
// are spare. Naming the account back does not resolve it later, since a name
// that comes back need not be the same account; the operator's release
// marker closes it. When the account was renamed only after the upgrade, the
// first pass recorded its uid, which stays held by uid under the new name.
func TestRestartHoldsTheUIDOfASchemaElevenSegment(t *testing.T) {
	requireIdentity(t)
	users := []ledger.WeftAgentUser{{Name: "weftagent9", UID: 601}, {Name: "weftagent2", UID: 602}}
	renamed := map[string]int{"weftagent9": 601, "weftagent2": 602}
	t.Run("renamed before the upgrade", func(t *testing.T) {
		s, run, pid, f := schemaElevenAgentRow(t)
		fakeDirectory(t, renamed)
		sent := recordSignals(t)
		ledgerRun := run.Segments[0].LedgerRunID
		marker := filepath.Join(run.Segments[0].RunDir.String, releaseMarker)
		outcomes := restart(t, s)
		want := "agent user weftagent1: " + unknownUID + "; once no process of the account it ran as is left, under whatever name it now has, create " +
			marker + " and restart the daemon"
		if len(outcomes) != 1 || outcomes[0].Action != ActionUnresolved || outcomes[0].Detail != want {
			t.Fatalf("outcomes = %+v, want unresolved with %q", outcomes, want)
		}
		assertUntouched(t, s, ledgerRun, pid, f, sent)
		if got := dispatchBeside(t, s, users); !slices.Equal(got, []ledger.WeftAgentUser{{}, {}}) {
			t.Fatalf("dispatched as %v while the row's uid is unknown, want both entries waiting", got)
		}

		fakeDirectory(t, map[string]int{"weftagent1": 601, "weftagent2": 602})
		if outcomes := restart(t, s); len(outcomes) != 1 || outcomes[0].Action != ActionUnresolved || len(f.calls) != 0 {
			t.Fatalf("outcomes = %+v with calls %v, want the row still held, its name not looked up again", outcomes, f.calls)
		}
		assertInFlight(t, s, ledgerRun)

		if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(marker, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		outcomes = restart(t, s)
		if len(outcomes) != 1 || outcomes[0].Action != ActionKilled ||
			!strings.Contains(outcomes[0].Detail, "agent user weftagent1: uid unknown since the upgrade, not killed; closed on the operator's release marker "+marker) {
			t.Fatalf("outcomes = %+v, want the row closed on the marker", outcomes)
		}
		if len(f.calls) != 0 {
			t.Fatalf("calls = %v, want nothing killed as the recorded name", f.calls)
		}
		waitGone(t, "worker", pid)
		assertUnknown(t, s, ledgerRun)
	})
	t.Run("renamed after the upgrade", func(t *testing.T) {
		s, run, pid, f := schemaElevenAgentRow(t)
		f.stubborn[setsidOrphan.PID] = true
		ledgerRun := run.Segments[0].LedgerRunID
		if outcomes := restart(t, s); len(outcomes) != 1 || outcomes[0].Action != ActionKillFailed ||
			!strings.Contains(outcomes[0].Detail, "702(/bin/sleep)") {
			t.Fatalf("outcomes = %+v, want the survivor to hold the row", outcomes)
		}
		if a, ok, err := s.WeftSegmentAgent(ledgerRun); !ok || err != nil || a.AgentUID != (sql.NullInt64{Int64: 601, Valid: true}) {
			t.Fatalf("held row's agent = %+v %v %v, want uid 601 recorded on the upgrade", a, ok, err)
		}
		fakeDirectory(t, renamed)
		f.calls = nil
		sent := recordSignals(t)
		if outcomes := restart(t, s); len(outcomes) != 1 || outcomes[0].Action != ActionKillFailed ||
			!strings.Contains(outcomes[0].Detail, "agent user weftagent1 was uid 601 when its segment was claimed and no longer resolves") {
			t.Fatalf("outcomes = %+v, want the renamed account's row held", outcomes)
		}
		if len(f.calls) != 0 || !slices.Equal(f.left("weftagent1"), []UserProcess{setsidOrphan, distnoted}) {
			t.Fatalf("calls = %v leaving %v, want nothing killed after the rename", f.calls, f.left("weftagent1"))
		}
		assertSignals(t, sent)
		if !alive(pid) {
			t.Fatalf("worker %d was signalled", pid)
		}
		if got := dispatchBeside(t, s, users); !slices.Equal(got, []ledger.WeftAgentUser{{Name: "weftagent2", UID: 602}, {}}) {
			t.Fatalf("dispatched as %v, want weftagent2 and the second entry waiting: weftagent9 is uid 601, held by the row", got)
		}
		assertHeld(t, s, ledgerRun)
	})
}

// schemaElevenAgentRow is agentRow on a ledger the previous binary wrote:
// claimed at schema 11, before uids were recorded, and opened again by this
// one, which migrates it to 12.
func schemaElevenAgentRow(t *testing.T) (*ledger.Store, *ledger.WeftRun, int, *fakeTable) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	run, pid, f := agentRow(t, s, func(id string) []string { return workerArgv(id, 1) })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`ALTER TABLE weft_segments DROP COLUMN agent_uid`,
		`UPDATE schema_meta SET value='11' WHERE key='schema_version'`} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = ledger.Open(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, run, pid, f
}

// assertUntouched checks a held agent row's pass killed and listed nothing
// as its recorded name, left uid 601's processes and its worker alone, and
// kept the row in flight and its repository reserved.
func assertUntouched(t *testing.T, s *ledger.Store, ledgerRun string, pid int, f *fakeTable, sent *[]string) {
	t.Helper()
	if len(f.calls) != 0 {
		t.Fatalf("calls = %v, want nothing killed or listed as the recorded name", f.calls)
	}
	if left := f.left("weftagent1"); !slices.Equal(left, []UserProcess{setsidOrphan, distnoted}) {
		t.Fatalf("uid 601 runs %v, want its processes untouched", left)
	}
	assertSignals(t, sent)
	if !alive(pid) {
		t.Fatalf("worker %d was signalled", pid)
	}
	assertInFlight(t, s, ledgerRun)
	released, _, _, err := s.RestartWeftQueue(ledger.WeftDispatch{}, func(string, string) (string, error) { return "", nil })
	if err != nil || len(released) != 0 {
		t.Fatalf("queue restart released %+v, %v", released, err)
	}
}

// assertHeld checks the row is still running once dispatch has claimed
// others beside it.
func assertHeld(t *testing.T, s *ledger.Store, ledgerRun string) {
	t.Helper()
	if row, err := s.Get(ledgerRun); err != nil || row.Status != ledger.StatusRunning {
		t.Fatalf("held row = %+v %v, want it still running", row, err)
	}
}

// dispatchBeside admits two launches beside the held row, with capacity to
// spare for both, and dispatches them with users. It answers the agent user
// each entry's segment was claimed with, none for an entry left waiting on
// capacity.
func dispatchBeside(t *testing.T, s *ledger.Store, users []ledger.WeftAgentUser) []ledger.WeftAgentUser {
	t.Helper()
	entries := admitLaunches(t, s, 2)
	root := t.TempDir()
	d := ledger.WeftDispatch{SharedRoot: filepath.Join(root, "shared"), RunDirRoot: filepath.Join(root, "runs"),
		AgentUsers: users, Check: func(*ledger.WeftEntry) (string, string) { return "", "" }}
	if c, err := s.WeftCapacity(); err != nil || c.Cap-c.Running < 2 {
		t.Fatalf("capacity = %+v %v, want room for both entries", c, err)
	}
	if _, _, err := s.DispatchWeft(d); err != nil {
		t.Fatal(err)
	}
	q, err := s.WeftQueue(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]*ledger.WeftEntry{}
	for _, e := range q.Entries {
		states[e.ID] = e
	}
	got := make([]ledger.WeftAgentUser, len(entries))
	for i, entry := range entries {
		e := states[entry.ID]
		switch {
		case e == nil:
			t.Fatalf("entry %s left the queue", entry.ID)
		case !e.RunID.Valid:
			if e.State != ledger.WeftEntryWaiting || e.WaitingReason.String != ledger.WeftWaitCapacity {
				t.Fatalf("entry = %+v, want it dispatched or waiting on capacity", e)
			}
			continue
		}
		run, err := s.GetWeftRun(e.RunID.String)
		if err != nil {
			t.Fatal(err)
		}
		seg := run.Segments[0]
		got[i] = ledger.WeftAgentUser{Name: seg.AgentUser.String, UID: int(seg.AgentUID.Int64)}
	}
	return got
}

// admitLaunches binds n repositories and admits one launch entry for each.
func admitLaunches(t *testing.T, s *ledger.Store, n int) []*ledger.WeftEntry {
	t.Helper()
	root := t.TempDir()
	bindings := map[string]ledger.WeftBinding{}
	var repos []string
	for i := range n {
		repo := filepath.Join(root, fmt.Sprintf("repo%d", i))
		if err := os.MkdirAll(repo, 0o755); err != nil {
			t.Fatal(err)
		}
		repos = append(repos, repo)
		bindings[fmt.Sprintf("p%d", i)] = ledger.WeftBinding{Digest: "tk", RepositoryID: sandbox.RepositoryID(repo), Repository: repo}
	}
	if _, _, err := s.SyncWeftCatalog(bindings); err != nil {
		t.Fatal(err)
	}
	var entries []*ledger.WeftEntry
	if err := s.WeftTx(func(tx *ledger.WeftTx) error {
		for i, repo := range repos {
			e, err := tx.Admit(ledger.WeftAdmission{Kind: ledger.WeftEntryLaunch, Ticket: fmt.Sprintf("p%d/t-%04d", i, i),
				Project: fmt.Sprintf("p%d", i), RepositoryID: sandbox.RepositoryID(repo), Repository: repo,
				TokenID: "token", RequestKey: fmt.Sprintf("k%d", i)})
			if err != nil {
				return err
			}
			entries = append(entries, e)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return entries
}

// A live pid carrying another segment's --segment is not the worker and is
// never signalled; the user is still killed, since it is this row's alone.
func TestRestartNeverSignalsAWorkerOfAnotherSegment(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	run, pid, f := agentRow(t, s, func(id string) []string { return workerArgv(id, 2) })
	sent := recordSignals(t)
	outcomes := restart(t, s)
	if len(outcomes) != 1 || outcomes[0].Action != ActionForeign ||
		!strings.Contains(outcomes[0].Detail, "does not carry --run "+run.ID+" and --segment 1") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertSignals(t, sent)
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	if left := f.left("weftagent1"); !slices.Equal(left, []UserProcess{distnoted}) {
		t.Fatalf("weftagent1 still runs %v", left)
	}
}

// A ledger read that cannot say which agent user a row holds stops the pass
// with the row held and nothing killed.
func TestRestartHoldsARowWhoseAgentLookupFails(t *testing.T) {
	s := newStore(t)
	run := weftRow(t, s, "demo/restart", "/tmp/shuttle-contain-repo")
	ledgerRun := run.Segments[0].LedgerRunID
	l := &failingAgentLedger{Store: s}
	outcomes, err := Restart(l)
	if err == nil || !strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("restart error = %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != ActionUnresolved || !strings.Contains(outcomes[0].Detail, "agent user: database is locked") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertInFlight(t, s, ledgerRun)
}

type failingAgentLedger struct{ *ledger.Store }

func (l *failingAgentLedger) WeftSegmentAgent(string) (ledger.WeftAgentSegment, bool, error) {
	return ledger.WeftAgentSegment{}, false, errors.New("database is locked")
}
