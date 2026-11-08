//go:build cgo

package reap

import (
	"errors"
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

// weftRow is a hosted segment launched before agent users, left running by a
// previous daemon: it holds no agent user, and its recorded pid is gone.
func weftRow(t *testing.T, s *ledger.Store, ticket, repo string) *ledger.WeftRun {
	t.Helper()
	root := resolved(t, t.TempDir())
	run, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticket, Project: "demo", Repo: repo, Branch: ticket,
		Worktree: filepath.Join(root, "worktree"), RunDirRoot: filepath.Join(root, "runs"),
	})
	if err != nil {
		t.Fatal(err)
	}
	gone := exec.Command("/usr/bin/true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkWeftRunning(run.ID, 1, gone.Process.Pid, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	return run
}

// A segment launched before agent users holds no user, so the pass closes it
// on its process group alone, as any other row, and only the closed row lets
// the repository go.
func TestRestartClosesAPreAgentUserSegmentOnItsGroup(t *testing.T) {
	s := newStore(t)
	repo := "/tmp/shuttle-contain-repo"
	run := weftRow(t, s, "demo/restart", repo)
	outcomes, err := Restart(s)
	if err != nil {
		t.Fatal(err)
	}
	ledgerRun := run.Segments[0].LedgerRunID
	if len(outcomes) != 1 || outcomes[0].RunID != ledgerRun || outcomes[0].Action != ActionDead {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertUnknown(t, s, ledgerRun)
	if err := s.ReconcileWeftUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after", Project: "demo", Repo: repo, Branch: "demo/after",
		Worktree: filepath.Join(t.TempDir(), "after"), RunDirRoot: t.TempDir(),
	}); err != nil {
		t.Fatalf("repository not released once the row closed: %v", err)
	}
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

// agentRow is a hosted segment running as weftagent1 left by a previous
// daemon: its worker, a live group leader carrying the segment's --run and
// --segment, and a table in which weftagent1 runs a setsid'd orphan.
func agentRow(t *testing.T, s *ledger.Store, argv func(runID string) []string) (*ledger.WeftRun, int, *fakeTable) {
	t.Helper()
	root := resolved(t, t.TempDir())
	run, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/agent", Project: "demo", Repo: "/tmp/shuttle-agent-repo", Branch: "demo/agent",
		Worktree: filepath.Join(root, "worktree"), RunDirRoot: filepath.Join(root, "runs"), AgentUser: "weftagent1",
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
