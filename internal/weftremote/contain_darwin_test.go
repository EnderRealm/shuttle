//go:build cgo

package weftremote

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"
	"golang.org/x/sys/unix"

	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/sandbox"
)

// holderScript is a worker's model process escaping its process group: under
// a seatbelt profile granting the segment's TMPDIR, as srt's coding profile
// does, a child calls setsid and sleeps, and its pid is written to
// $TMPDIR/holder.pid once it has left the group.
const holderScript = `rd=$(cd "$TMPDIR" && pwd -P)
printf '(version 1)\n(allow default)\n(deny file-write*)\n(allow file-write* (subpath "%s"))\n' "$rd" > "$TMPDIR/holder.sb"
/usr/bin/sandbox-exec -f "$TMPDIR/holder.sb" /usr/bin/perl -e '
pipe(my $r, my $w) or die; my $p = fork(); die unless defined $p;
if ($p) { close $w; my $pid = <$r>; open(my $f, ">", "$ENV{TMPDIR}/holder.pid") or die; print $f $pid; close $f; exit 0 }
close $r; close STDIN; close STDOUT; close STDERR; require POSIX; POSIX::setsid(); print $w "$$\n"; close $w; sleep 300
' </dev/null >/dev/null 2>&1
`

// sweptHolder wraps the settlement's sweep: on entry it reads the holder's
// pid from runDir's TMPDIR, establishes the holder alive by its kernel start
// time and runs during, all before the real sweep; after it, runs after. It
// answers the pid, read once the run is over.
func sweptHolder(t *testing.T, runDir string, during, after func(pid int)) *int {
	t.Helper()
	pid := new(int)
	prior := containSegment
	containSegment = func(b reap.Boundary) (reap.Containment, error) {
		data, err := os.ReadFile(filepath.Join(sandbox.SegmentTmpDir(runDir), "holder.pid"))
		if err == nil {
			*pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		}
		if err != nil {
			t.Errorf("holder pid: %v", err)
			return prior(b)
		}
		start, err := reap.ProcessStart(*pid)
		if err != nil {
			t.Errorf("holder %d was not alive when the sweep began: %v", *pid, err)
			return prior(b)
		}
		// By cleanup the sweep has killed the holder and its pid may belong to
		// another process, so only the process that started then is killed.
		t.Cleanup(func() {
			if now, err := reap.ProcessStart(*pid); err == nil && now.Equal(start) {
				syscall.Kill(*pid, syscall.SIGKILL)
			}
		})
		if during != nil {
			during(*pid)
		}
		c, err := prior(b)
		if after != nil {
			after(*pid)
		}
		return c, err
	}
	t.Cleanup(func() { containSegment = prior })
	return pid
}

// claimSecond is a second run claiming the fixture's repository.
func (f *supervisorFixture) claimSecond(ticket string) error {
	_, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticket, Project: "demo", Repo: f.repo, Branch: ticket,
		Worktree:   filepath.Join(f.root, strings.ReplaceAll(ticket, "/", "-")),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	return err
}

// requireBusy fails unless err is the repository held by runID.
func requireBusy(t *testing.T, err error, runID string) {
	t.Helper()
	var busy *ledger.RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != runID {
		t.Errorf("second claim = %v, want the repository held by %s", err, runID)
	}
}

// A detached sandboxed descendant outlives its worker's process group however
// the segment ends — the worker exiting after its terminal event, an operator
// stop, the segment timeout. Each settlement sweeps it away first, and only
// then is the repository free for a second run.
func TestSupervisorContainsDetachedHoldersBeforeReleasingRepository(t *testing.T) {
	tests := []struct {
		name    string
		script  func(worktree string) string
		stop    bool
		timeout int
	}{
		{name: "exit", script: func(worktree string) string { return fixtureWorkerScript(worktree) + holderScript }},
		{name: "stop", stop: true, script: func(string) string {
			return "#!/bin/sh\n" + v1WorkerGuard + holderScript + "read -r _\nexit 0\n"
		}},
		{name: "timeout", timeout: 1, script: func(string) string {
			return "#!/bin/sh\n" + v1WorkerGuard + holderScript + "sleep 300\n"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSupervisorFixture(t, false)
			logs := &syncBuffer{}
			f.supervisor.log = log.New(logs, "", 0)
			if tt.timeout > 0 {
				f.cfg.Weft.SegmentTimeoutSeconds = tt.timeout
			}
			ticket := "demo/contain-" + tt.name
			worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, ticket)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(tt.script(worktree)), 0o700); err != nil {
				t.Fatal(err)
			}
			run := f.claim(t, ticket)
			runDir := run.Segments[0].RunDir.String
			held := func(int) { requireBusy(t, f.claimSecond("demo/during-contain"), run.ID) }
			swept := sweptHolder(t, runDir, held, held)
			done := make(chan error, 1)
			go func() { done <- f.supervisor.run(run.ID) }()
			if tt.stop {
				waitFor(t, func() bool {
					_, err := os.Stat(filepath.Join(sandbox.SegmentTmpDir(runDir), "holder.pid"))
					return err == nil
				})
				f.supervisor.Stop(run.ID)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			pid := *swept
			if !strings.Contains(logs.String(), "containment stopped 1 ["+strconv.Itoa(pid)+"(perl)]") {
				t.Fatalf("log does not name the contained holder %d:\n%s", pid, logs.String())
			}
			waitFor(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
			if err := f.claimSecond("demo/after-contain"); err != nil {
				t.Fatalf("repository was not released after containment: %v", err)
			}
		})
	}
}

// bystanderScript is the model process of a segment on another repository
// escaping its worker as holderScript's does, under a seatbelt profile
// granting only that segment's own TMPDIR and worktree, which the worker
// creates as it does its checkout; its pid is written to $TMPDIR/holder.pid.
func bystanderScript(worktree string) string {
	return `rd=$(cd "$TMPDIR" && pwd -P)
mkdir -p '` + worktree + `' && wt=$(cd '` + worktree + `' && pwd -P)
printf '(version 1)\n(allow default)\n(deny file-write*)\n(allow file-write* (subpath "%s") (subpath "%s"))\n' "$rd" "$wt" > "$TMPDIR/holder.sb"
/usr/bin/sandbox-exec -f "$TMPDIR/holder.sb" /usr/bin/perl -e '
pipe(my $r, my $w) or die; my $p = fork(); die unless defined $p;
if ($p) { close $w; my $pid = <$r>; open(my $f, ">", "$ENV{TMPDIR}/holder.pid") or die; print $f $pid; close $f; exit 0 }
close $r; close STDIN; close STDOUT; close STDERR; require POSIX; POSIX::setsid(); print $w "$$\n"; close $w; sleep 300
' </dev/null >/dev/null 2>&1
`
}

// Settling a segment sweeps only its own boundary: a segment running
// concurrently on another repository, whose detached model process is
// granted neither the settling segment's run dir, canary nor worktree, is
// left running, while the settling segment's own detached holder is killed.
// The bystander is then shown to be a real holder of its own segment's
// boundary, contained when that segment settles.
func TestSupervisorSettlementLeavesAConcurrentSegmentAlone(t *testing.T) {
	f := newSupervisorFixture(t, false)
	logs := &syncBuffer{}
	f.supervisor.log = log.New(logs, "", 0)
	other := filepath.Join(f.root, "other")
	if err := os.MkdirAll(filepath.Join(other, ".git", "worktrees"), 0o755); err != nil {
		t.Fatal(err)
	}
	ticket, bystanderTicket := "demo/settling", "other/bystander"
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, ticket)
	if err != nil {
		t.Fatal(err)
	}
	bystanderWorktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, other, bystanderTicket)
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" + v1WorkerGuard + `case " $* " in *" ` + bystanderTicket + ` "*)
` + bystanderScript(bystanderWorktree) + `read -r _
exit 0 ;;
esac
mkdir -p '` + worktree + `'
` + strings.TrimPrefix(fixtureWorkerScript(worktree), "#!/bin/sh\n"+v1WorkerGuard) + holderScript
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	run := f.claim(t, ticket)
	bystander, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: bystanderTicket, Project: "other", Repo: other, Branch: bystanderTicket,
		Worktree: bystanderWorktree, RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	if err != nil {
		t.Fatal(err)
	}
	runDir := run.Segments[0].RunDir.String
	bystanderPIDFile := filepath.Join(sandbox.SegmentTmpDir(bystander.Segments[0].RunDir.String), "holder.pid")
	var bystanderPID int
	var bystanderStart time.Time
	alive := func(int) {
		if now, err := reap.ProcessStart(bystanderPID); err != nil || !now.Equal(bystanderStart) {
			t.Errorf("bystander %d was not left alive by the settling sweep: %v", bystanderPID, err)
		}
	}
	// sweptHolder reads the settling segment's holder on every sweep, so the
	// bystander's own settlement is routed past it to the real sweep.
	sweep := containSegment
	swept := sweptHolder(t, runDir, alive, alive)
	settling := containSegment
	containSegment = func(b reap.Boundary) (reap.Containment, error) {
		if b.Control == runDir {
			return settling(b)
		}
		return sweep(b)
	}

	// The bystander's run is stopped and joined before sweptHolder restores
	// containSegment and the fixture is torn down, however the test ends.
	bystanderDone := make(chan error, 1)
	bystanderExited := make(chan struct{})
	go func() {
		bystanderDone <- f.supervisor.run(bystander.ID)
		close(bystanderExited)
	}()
	t.Cleanup(func() {
		f.supervisor.Stop(bystander.ID)
		<-bystanderExited
	})
	waitFor(t, func() bool {
		data, err := os.ReadFile(bystanderPIDFile)
		if err != nil || len(data) == 0 || data[len(data)-1] != '\n' {
			return false
		}
		bystanderPID, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil
	})
	bystanderStart, err = reap.ProcessStart(bystanderPID)
	if err != nil {
		t.Fatalf("bystander %d: %v", bystanderPID, err)
	}
	t.Cleanup(func() {
		if now, err := reap.ProcessStart(bystanderPID); err == nil && now.Equal(bystanderStart) {
			syscall.Kill(bystanderPID, syscall.SIGKILL)
		}
	})

	if err := f.supervisor.run(run.ID); err != nil {
		t.Fatalf("%v\n%s", err, logs.String())
	}
	pid := *swept
	if pid == 0 {
		t.Fatalf("the settling segment's sweep never ran:\n%s", logs.String())
	}
	waitFor(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
	if now, err := reap.ProcessStart(bystanderPID); err != nil || !now.Equal(bystanderStart) {
		t.Fatalf("bystander %d did not survive the settlement: %v", bystanderPID, err)
	}
	contained := ": containment stopped 1 [" + strconv.Itoa(pid) + "(perl)], released 0 [], killed 1 [" + strconv.Itoa(pid) + "(perl)]"
	if !strings.Contains(logs.String(), contained) || strings.Count(logs.String(), ": containment ") != 1 {
		t.Fatalf("containment log does not name only the settling holder %d:\n%s", pid, logs.String())
	}

	f.supervisor.Stop(bystander.ID)
	if err := <-bystanderDone; err != nil {
		t.Fatalf("%v\n%s", err, logs.String())
	}
	if !strings.Contains(logs.String(), ": containment stopped 1 ["+strconv.Itoa(bystanderPID)+"(perl)]") {
		t.Fatalf("the bystander's own settlement did not contain it:\n%s", logs.String())
	}
}

// A boundary the sweep cannot establish empty leaves the segment unsettled:
// its row stays in flight and the repository stays reserved.
func TestSupervisorRetainsReservationWhileTheBoundaryIsNotEmpty(t *testing.T) {
	f := newSupervisorFixture(t, false)
	logs := &syncBuffer{}
	f.supervisor.log = log.New(logs, "", 0)
	prior := containSegment
	var swept reap.Boundary
	containSegment = func(b reap.Boundary) (reap.Containment, error) {
		swept = b
		return reap.Containment{Stopped: []int{4242}}, errors.New("1 holder(s) outlived SIGKILL")
	}
	t.Cleanup(func() { containSegment = prior })
	run := f.claim(t, "demo/worker")
	err := f.supervisor.run(run.ID)
	if err == nil || !strings.Contains(err.Error(), "reservation retained") {
		t.Fatalf("run error = %v, want the reservation retained", err)
	}
	runDir := run.Segments[0].RunDir.String
	if want := reap.SegmentBoundary(runDir); swept != want {
		t.Fatalf("swept %+v, want %+v", swept, want)
	}
	if !strings.Contains(logs.String(), "containment stopped 1 [4242]") {
		t.Fatalf("log does not name the stopped holder:\n%s", logs.String())
	}
	row, err := f.store.Get(run.Segments[0].LedgerRunID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != ledger.StatusRunning {
		t.Fatalf("segment row = %s, want it left running", row.Status)
	}
	_, err = f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/second", Project: "demo", Repo: f.repo,
		Branch: "demo/second", Worktree: filepath.Join(f.root, "second"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	var busy *ledger.RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != run.ID {
		t.Fatalf("claim behind an unsettled boundary = %v", err)
	}
}

// liveHolder is a model process escaping its worker under the real coding
// profile: the child calls setsid, its pid is recorded in the segment's TMPDIR
// (the second argument), and it rewrites a file in the worktree (the first)
// every 50ms for as long as it lives.
const liveHolder = `my ($wt, $tmp) = @ARGV; pipe(my $r, my $w) or die; my $p = fork(); die unless defined $p;
if ($p) { close $w; my $pid = <$r>; open(my $f, ">", "$tmp/holder.pid") or die "$tmp: $!"; print $f $pid; close $f; exit 0 }
close $r; close STDIN; close STDOUT; close STDERR; require POSIX; POSIX::setsid(); print $w "$$\n"; close $w;
while (1) { open(my $f, ">", "$wt/alive") and close $f; select(undef, undef, undef, 0.05) }`

// written reports whether something writes path within a second of its
// removal.
func written(path string) bool {
	os.Remove(path)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// The Studio check, through a real settlement: the segment's preflight runs
// real srt, and the worker launches a model process under the coding profile
// srt generated for it, which detaches and keeps writing the worktree. When
// the worker exits the reservation is held while the holder still writes,
// and still held once the sweep has killed it, until settlement; only then
// is the repository free, with nothing left writing the worktree.
func TestLiveSRTSettlementContainsADetachedHolder(t *testing.T) {
	if os.Getenv("SHUTTLE_LIVE_SRT") != "1" {
		t.Skip("set SHUTTLE_LIVE_SRT=1 to settle a segment whose real srt sandbox left a detached child")
	}
	srt := os.Getenv("SHUTTLE_SRT_BIN")
	if srt == "" {
		srt = "/opt/homebrew/bin/srt"
	}
	f := newSupervisorFixtureIn(t, "/var/tmp", false)
	f.cfg.Paths.SRTBin = srt
	logs := &syncBuffer{}
	f.supervisor.log = log.New(logs, "", 0)
	ticket := "demo/live-contain"
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, ticket)
	if err != nil {
		t.Fatal(err)
	}
	// The worktree exists before the profile is generated, as a resumed
	// segment's does, so srt resolves the grant to it.
	admin := filepath.Join(f.repo, ".git", "worktrees", filepath.Base(worktree))
	for _, dir := range []string{worktree, admin} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(worktree, ".git"), []byte("gitdir: "+admin), 0o644); err != nil {
		t.Fatal(err)
	}
	holder := filepath.Join(f.root, "holder.pl")
	if err := os.WriteFile(holder, []byte(liveHolder), 0o600); err != nil {
		t.Fatal(err)
	}
	launch := `profile=; prev=
for a in "$@"; do [ "$prev" = --sandbox-profile ] && profile=$a; prev=$a; done
` + srt + ` -s "$profile" -c "/usr/bin/perl ` + holder + ` ` + worktree + ` $TMPDIR" </dev/null >/dev/null 2>&1 || exit 3
while [ ! -s "$TMPDIR/holder.pid" ]; do /bin/sleep 0.05; done
`
	script := "#!/bin/sh\n" + v1WorkerGuard + launch + strings.TrimPrefix(fixtureWorkerScript(worktree), "#!/bin/sh\n"+v1WorkerGuard)
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	run := f.claim(t, ticket)
	alive := filepath.Join(worktree, "alive")
	swept := sweptHolder(t, run.Segments[0].RunDir.String, func(pid int) {
		if !written(alive) {
			t.Errorf("holder %d was not writing the worktree when settlement began", pid)
		}
		requireBusy(t, f.claimSecond("demo/during-live-contain"), run.ID)
	}, func(pid int) {
		if !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			t.Errorf("holder %d outlived the sweep", pid)
		}
		requireBusy(t, f.claimSecond("demo/during-live-contain"), run.ID)
	})
	if err := f.supervisor.run(run.ID); err != nil {
		t.Fatalf("%v\n%s", err, logs.String())
	}
	if *swept == 0 {
		t.Fatalf("the sweep never ran:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "containment stopped 1 ["+strconv.Itoa(*swept)+"(perl)]") {
		t.Fatalf("log does not name the contained holder %d:\n%s", *swept, logs.String())
	}
	if err := f.claimSecond("demo/after-live-contain"); err != nil {
		t.Fatalf("repository was not released after settlement: %v", err)
	}
	if written(alive) {
		t.Fatal("the worktree is still written after settlement")
	}
}

// Markers the fixture worker acts on: start deadTreeScript, or probe it.
var (
	spawnLine = map[string]any{"@": "SPAWN"}
	probeLine = map[string]any{"@": "PROBE"}
)

// readTree is the processes deadTreeScript printed to path, each established
// alive by its kernel start time.
func readTree(path string) (map[int]time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	tree := map[int]time.Time{}
	for _, field := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			return nil, err
		}
		start, err := reap.ProcessStart(pid)
		if err != nil {
			return nil, fmt.Errorf("tree process %d is not alive: %w", pid, err)
		}
		tree[pid] = start
	}
	if len(tree) != 2 {
		return nil, fmt.Errorf("%s names %d processes, want 2", path, len(tree))
	}
	return tree, nil
}

// treeLive reports whether pid is still the live, unreaped process that
// started at start.
func treeLive(pid int, start time.Time) bool {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || kp.Proc.P_pid != int32(pid) || kp.Proc.P_stat == zombieProcess {
		return false
	}
	sec, nsec := kp.Proc.P_starttime.Unix()
	return time.Unix(sec, nsec).Equal(start)
}

// treeGone reports whether no process of tree is live, allowing the moment
// launchd takes to reap a killed orphan.
func treeGone(tree map[int]time.Time) bool {
	deadline := time.Now().Add(2 * time.Second)
	for {
		live := false
		for pid, start := range tree {
			live = live || treeLive(pid, start)
		}
		if !live {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// killTree kills whatever of tree is still the process it named.
func killTree(tree map[int]time.Time) {
	for pid, start := range tree {
		if treeLive(pid, start) {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// A worker that dies without a terminal event leaves the unsandboxed children
// it started — a git push and the remote helper under it — running outside its
// process group and outside any sandbox grant, but still in the session the
// worker led. Settlement sweeps that session while the worker's zombie still
// holds its id, and waits for it to be empty before it settles the segment and
// frees its repository. A child the sweep cannot stop is reported, and the segment
// stays unsettled while it lives: its lost landing's reconcile is never
// dispatched.
func TestDeadSegmentChildrenContained(t *testing.T) {
	t.Run("killed", func(t *testing.T) {
		f := newSupervisorFixture(t, false)
		logs := &syncBuffer{}
		f.supervisor.log = log.New(logs, "", 0)
		run := f.claim(t, "demo/dead-tree")
		runDir := run.Segments[0].RunDir.String
		treeFile := filepath.Join(runDir, "tree.pids")
		script := "#!/bin/sh\n" + v1WorkerGuard + deadTreeScript + " > " + treeFile + "\nexit 9\n"
		if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
		var tree map[int]time.Time
		t.Cleanup(func() { killTree(tree) })
		prior := containSession
		containSession = func(sid int) (reap.Containment, error) {
			var err error
			if tree, err = readTree(treeFile); err != nil {
				t.Errorf("when the sweep began: %v", err)
			}
			if kp, err := unix.SysctlKinfoProc("kern.proc.pid", sid); err != nil || kp.Proc.P_pid != int32(sid) ||
				kp.Proc.P_stat != zombieProcess {
				t.Errorf("worker %d is not an unreaped zombie when its session is swept: %+v %v", sid, kp, err)
			}
			for pid := range tree {
				s, serr := unix.Getsid(pid)
				g, gerr := unix.Getpgid(pid)
				if serr != nil || gerr != nil || s != sid || g == sid {
					t.Errorf("child %d: session %d (%v), group %d (%v); want session %d in a group of its own", pid, s, serr, g, gerr, sid)
				}
			}
			requireBusy(t, f.claimSecond("demo/during-tree"), run.ID)
			c, err := prior(sid)
			if !treeGone(tree) {
				t.Errorf("the dead segment's children %v outlived the sweep", slices.Collect(maps.Keys(tree)))
			}
			requireBusy(t, f.claimSecond("demo/during-tree"), run.ID)
			return c, err
		}
		t.Cleanup(func() { containSession = prior })
		if err := f.supervisor.run(run.ID); err != nil {
			t.Fatalf("%v\n%s", err, logs.String())
		}
		if tree == nil {
			t.Fatalf("the session sweep never ran:\n%s", logs.String())
		}
		got, err := f.store.GetWeftRun(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Segments[0].Failure.String != ledger.ClassDied {
			t.Fatalf("segment = %+v, want it failed %s", got.Segments[0], ledger.ClassDied)
		}
		if !strings.Contains(logs.String(), ": session containment stopped 2 [") {
			t.Fatalf("log does not report the contained tree:\n%s", logs.String())
		}
		for pid := range tree {
			if !strings.Contains(logs.String(), strconv.Itoa(pid)+"(sleep)") {
				t.Fatalf("log does not name child %d:\n%s", pid, logs.String())
			}
		}
		if err := f.claimSecond("demo/after-tree"); err != nil {
			t.Fatalf("repository was not released once the tree was gone: %v", err)
		}
	})

	t.Run("unstoppable", func(t *testing.T) {
		h := newLandingHost(t)
		id := "weft/example-ready-a0b1"
		h.ticket(id, ticket.StatusReady, 2)
		lost := through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_intent", "push")
		h.script(id, 1, append(lost, spawnLine, dieLine))
		treeFile := filepath.Join(h.control, slugOf(id)+".tree")
		var (
			mu   sync.Mutex
			tree map[int]time.Time
		)
		prior := containSession
		containSession = func(int) (reap.Containment, error) {
			mu.Lock()
			defer mu.Unlock()
			read, err := readTree(treeFile)
			if err != nil {
				t.Errorf("when the sweep began: %v", err)
				return reap.Containment{}, err
			}
			tree = read
			c := reap.Containment{Names: map[int]string{}}
			for pid := range tree {
				c.Stopped = append(c.Stopped, pid)
				c.Names[pid] = "sleep"
			}
			return c, fmt.Errorf("%d process(es) still live", len(tree))
		}
		runID := h.waitDispatched(entryOf(h.launch(id, "unstoppable"))["entry_id"].(string))
		// The unsettled segment is settled by hand once its tree is killed, so
		// the host's teardown finds nothing running.
		t.Cleanup(func() {
			containSession = prior
			mu.Lock()
			killTree(tree)
			mu.Unlock()
			run := h.run(runID)
			if _, err := h.service.segments.settle(run, run.Segments[0], ledger.ClassDied, "torn down", nil); err != nil {
				t.Error(err)
			}
		})
		waitFor(t, func() bool {
			return strings.Contains(h.logs.String(), "session containment: 2 process(es) still live; reservation retained")
		})
		if !strings.Contains(h.logs.String(), ": session containment stopped 2 [") {
			t.Fatalf("log does not report the children it could not stop:\n%s", h.logs.String())
		}
		mu.Lock()
		for pid, start := range tree {
			if !treeLive(pid, start) {
				t.Errorf("child %d is gone; the sweep was to leave it live", pid)
			}
		}
		mu.Unlock()
		run := h.run(runID)
		if len(run.Segments) != 1 || h.reaped(run) || run.SettlementState == ledger.WeftSettlementReconciling ||
			holderOf(h.repositoryOf("weft")) != "run "+runID {
			t.Fatalf("segment settled beside a live child: %+v", run)
		}
		for _, e := range h.queue()["entries"].([]any) {
			if e.(map[string]any)["kind"] == "reconcile" {
				t.Fatalf("reconcile dispatched beside a live child: %v", e)
			}
		}
		if strings.Contains(h.argv(), "reconcile --contract") {
			t.Fatalf("reconcile launched beside a live child: %s", h.argv())
		}
	})
}

// The Studio's lost landing: the worker dies at its push intent while the
// push it started — a child leading a process group of its own in the
// worker's session, holding no sandbox grant — is still running. Reconcile is
// dispatched only once the sweep has killed that child, so what it observes is
// the state the dead segment left once it stopped acting: push not done, which
// it takes under a fresh intent and lands.
func TestReconcileWaitsForDeadSegmentChild(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	lost := through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_intent", "push")
	h.script(id, 1, append(lost, spawnLine, dieLine))
	observed := [3]string{"done", "not_done", "not_done"}
	h.script(id, 2, append(workerLines{probeLine}, reconcileLines(t, observed, fixtureHead, fixtureBase, "push", "mark_done")...))
	treeFile := filepath.Join(h.control, slugOf(id)+".tree")
	var (
		mu    sync.Mutex
		tree  map[int]time.Time
		swept int
	)
	t.Cleanup(func() {
		mu.Lock()
		killTree(tree)
		mu.Unlock()
	})
	prior := containSession
	containSession = func(sid int) (reap.Containment, error) {
		mu.Lock()
		defer mu.Unlock()
		if swept++; swept > 1 {
			return prior(sid)
		}
		var err error
		if tree, err = readTree(treeFile); err != nil {
			t.Errorf("when the dead segment's sweep began: %v", err)
		}
		if strings.Contains(h.argv(), "reconcile --contract") {
			t.Errorf("reconcile launched before the dead segment's sweep: %s", h.argv())
		}
		c, err := prior(sid)
		if !treeGone(tree) {
			t.Errorf("the dead segment's children %v outlived the sweep", slices.Collect(maps.Keys(tree)))
		}
		return c, err
	}
	t.Cleanup(func() { containSession = prior })
	runID := h.waitDispatched(entryOf(h.launch(id, "lost-push"))["entry_id"].(string))

	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	run = h.run(runID)
	if len(run.Segments) != 2 || run.Segments[0].Failure.String != ledger.ClassDied ||
		run.Segments[1].Kind != ledger.WeftSegmentReconcile {
		t.Fatalf("reconciled run = %+v", run)
	}
	probe, err := os.ReadFile(filepath.Join(h.control, slugOf(id)+"-2.probe"))
	if err != nil || string(probe) != "gone\n" {
		t.Fatalf("when the reconcile observed, the dead segment's children were %q (%v)", probe, err)
	}
	if !strings.Contains(h.logs.String(), ": session containment stopped 2 [") {
		t.Fatalf("log does not report the contained tree:\n%s", h.logs.String())
	}
	// Only a step with a row when the reconcile observed carries its reading:
	// mark_done had no intent until the reconcile took it.
	for _, step := range run.Steps {
		if !step.Done() {
			t.Fatalf("%s is not done after the reconcile: %+v", step.Step, run.Steps)
		}
		if step.Step == "push" && (step.Observed.String != observed[1] || step.ObservedSegment.Int64 != 2) {
			t.Fatalf("push observed %q by segment %d, want %q by the reconcile: %+v",
				step.Observed.String, step.ObservedSegment.Int64, observed[1], run.Steps)
		}
	}
}
