//go:build cgo

package reap

import (
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
)

// detachScript forks; the child leaves its parent's session with setsid, as a
// model process escaping the worker's process group does, writes its pid to
// stdout's pipe through the parent, and sleeps. The parent exits at once, so
// the child is reparented to launchd and is not this test's to reap.
const detachScript = `pipe(my $r, my $w) or die; my $p = fork(); die unless defined $p;
if ($p) { close $w; my $pid = <$r>; print $pid; exit 0 }
close $r; close STDIN; close STDOUT; close STDERR;
require POSIX; POSIX::setsid(); print $w "$$\n"; close $w; sleep 120`

// resolved is dir with its symlinks resolved: seatbelt matches a profile's
// paths against the real path, and the test temp dir sits under /var, an alias
// of /private/var.
func resolved(t *testing.T, dir string) string {
	t.Helper()
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return real
}

// boundaryFixture is a segment's run directory with its canary, and a
// worktree beside it, every path resolved.
func boundaryFixture(t *testing.T) Boundary {
	t.Helper()
	root := resolved(t, t.TempDir())
	b := SegmentBoundary(filepath.Join(root, "run"), filepath.Join(root, "worktree"))
	for _, dir := range []string{b.Canary, b.Worktree} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// startDetached runs detachScript under a seatbelt profile that denies every
// write but the subpaths granted, and answers the detached child's pid. With
// no profile the child is not sandboxed at all.
func startDetached(t *testing.T, profile bool, grants ...string) int {
	t.Helper()
	return startScript(t, []string{"/usr/bin/perl", "-e", detachScript}, profile, grants...)
}

// startScript runs argv, a perl script that prints a detached child's pid, as
// startDetached does.
func startScript(t *testing.T, argv []string, profile bool, grants ...string) int {
	t.Helper()
	if profile {
		rules := "(version 1)\n(allow default)\n(deny file-write*)\n"
		for _, g := range grants {
			rules += fmt.Sprintf("(allow file-write* (subpath %q))\n", g)
		}
		path := filepath.Join(resolved(t, t.TempDir()), "holder.sb")
		if err := os.WriteFile(path, []byte(rules), 0o600); err != nil {
			t.Fatal(err)
		}
		argv = append([]string{"/usr/bin/sandbox-exec", "-f", path}, argv...)
	}
	out, err := exec.Command(argv[0], argv[1:]...).Output()
	if err != nil {
		t.Fatalf("start detached child: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("detached child pid %q: %v", out, err)
	}
	start, err := processStart(pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if now, err := processStart(pid); err == nil && now.Equal(start) {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	if sid, err := syscall.Getsid(pid); err != nil || sid != pid {
		t.Fatalf("child %d session = %d, %v; want its own", pid, sid, err)
	}
	return pid
}

// assertRunning checks pid is alive and not left stopped.
func assertRunning(t *testing.T, pid int) {
	t.Helper()
	_, stopped, err := processState(pid)
	if err != nil {
		t.Fatalf("pid %d: %v", pid, err)
	}
	if stopped {
		t.Fatalf("pid %d was left stopped", pid)
	}
}

// A sandboxed child that called setsid still carries its profile, and with it
// the segment's grant: the sweep finds it by the canary or by the worktree,
// stops it and kills it.
func TestContainKillsDetachedSandboxedHolders(t *testing.T) {
	b := boundaryFixture(t)
	byCanary := startDetached(t, true, b.Canary)
	byWorktree := startDetached(t, true, b.Worktree)
	c, err := Contain(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{byCanary, byWorktree} {
		if !slices.Contains(c.Stopped, pid) || !slices.Contains(c.Killed, pid) {
			t.Errorf("containment %s does not stop and kill %d", c, pid)
		}
		waitGone(t, "detached holder", pid)
	}
	again, err := Contain(b)
	if err != nil || again.Found() {
		t.Fatalf("second sweep = %s, %v; want an empty boundary", again, err)
	}
}

// An unsandboxed process, a sandboxed one without the grant, and one
// sandboxed more broadly than the segment — allowed the control as well — are
// none of them the segment's, and are never signalled.
func TestContainLeavesProcessesWithoutTheGrantAlone(t *testing.T) {
	b := boundaryFixture(t)
	other := resolved(t, t.TempDir())
	unsandboxed := startDetached(t, false)
	ungranted := startDetached(t, true, other)
	broader := startDetached(t, true, b.Control)
	c, err := Contain(b)
	if err != nil {
		t.Fatal(err)
	}
	if c.Found() {
		t.Fatalf("containment = %s, want nothing signalled", c)
	}
	for _, pid := range []int{unsandboxed, ungranted, broader} {
		assertRunning(t, pid)
	}
}

// A control that does not exist answers denied for every process, so a sweep
// against it would read empty whatever is running: it fails instead,
// signalling nothing.
func TestContainFailsClosedOnAMissingControl(t *testing.T) {
	b := boundaryFixture(t)
	holder := startDetached(t, true, b.Canary)
	b.Control += "-missing"
	c, err := Contain(b)
	if err == nil || c.Found() {
		t.Fatalf("containment = %s, %v; want a refusal that signals nothing", c, err)
	}
	assertRunning(t, holder)
}

// vandalScript is detachScript whose child, once detached, takes its canary
// away — removing it, or putting a symlink or a file in its place — before
// it reports its pid and sleeps. Told to loop, it goes on recreating and
// removing the canary for as long as it runs.
const vandalScript = `my ($canary, $how) = @ARGV; pipe(my $r, my $w) or die; my $p = fork(); die unless defined $p;
if ($p) { close $w; my $pid = <$r>; print $pid; exit 0 }
close $r; close STDIN; close STDOUT; close STDERR; require POSIX; POSIX::setsid();
rmdir $canary or die;
if ($how eq "symlink") { symlink("/tmp", $canary) or die }
if ($how eq "file") { open(my $f, ">", $canary) or die; close $f }
print $w "$$\n"; close $w;
if ($how eq "loop") { while (1) { mkdir $canary; rmdir $canary } }
sleep 120`

// The canary is the root of the subpath the segment grants, so a holder can
// remove it or replace it. The sweep recreates it, finds the holder by it all
// the same, and succeeds once the holder is dead.
func TestContainRecreatesACanaryTheHolderTookAway(t *testing.T) {
	for _, how := range []string{"rmdir", "symlink", "file"} {
		t.Run(how, func(t *testing.T) {
			b := boundaryFixture(t)
			if err := os.RemoveAll(b.Worktree); err != nil {
				t.Fatal(err)
			}
			holder := startScript(t, []string{"/usr/bin/perl", "-e", vandalScript, b.Canary, how}, true, b.Canary)
			if info, err := os.Lstat(b.Canary); how == "rmdir" && !errors.Is(err, os.ErrNotExist) ||
				how != "rmdir" && (err != nil || info.IsDir()) {
				t.Fatalf("canary after the holder took it away = %v, %v", info, err)
			}
			c, err := Contain(b)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(c.Stopped, holder) || !slices.Contains(c.Killed, holder) {
				t.Fatalf("containment %s does not stop and kill %d", c, holder)
			}
			waitGone(t, "detached holder", holder)
			info, err := os.Lstat(b.Canary)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
				t.Fatalf("canary after the sweep = %v, %v; want a 0700 directory", info, err)
			}
		})
	}
}

// A holder that keeps removing and recreating its canary, with no worktree to
// find it by instead, makes every grant check race the canary's identity.
// The sweep may catch it in a window where the canary held still and kill
// it, or fail closed once the grace period runs out; it never reports the
// boundary empty while the holder lives.
func TestContainNeverReportsEmptyWhileAHolderChurnsItsCanary(t *testing.T) {
	shortGrace(t)
	b := boundaryFixture(t)
	if err := os.RemoveAll(b.Worktree); err != nil {
		t.Fatal(err)
	}
	holder := startScript(t, []string{"/usr/bin/perl", "-e", vandalScript, b.Canary, "loop"}, true, b.Canary)
	c, err := Contain(b)
	if err != nil {
		t.Logf("sweep failed closed: %v (%s)", err, c)
		if alive(holder) && slices.Contains(c.Released, holder) {
			if _, stopped, err := processState(holder); err == nil && !stopped {
				t.Fatalf("containment %s released the live holder %d", c, holder)
			}
		}
		return
	}
	if !slices.Contains(c.Stopped, holder) || !slices.Contains(c.Killed, holder) {
		t.Fatalf("containment %s reported the boundary empty with holder %d not killed", c, holder)
	}
	waitGone(t, "churning holder", holder)
}

// A listed process can fork a child and exit at the instant it is grant-
// checked: the departed pid answers as a non-holder, while the child it
// forked holds the grant and is not in the listing. The sweep must not read
// that as the boundary being empty. The handoff happens on every scan here,
// so it covers the initial scan and the final one alike, and the sweep can
// never succeed while the latest child still holds the grant.
func TestContainFailsClosedWhenAListedHolderHandsOffDuringEveryScan(t *testing.T) {
	shortGrace(t)
	b := boundaryFixture(t)
	const head = 4242
	start := time.Unix(1000, 0)
	// gone is set as the head is grant-checked: the fork-and-exit that leaves
	// the child it spawned — the real holder — out of every listing.
	gone := false
	for _, restore := range []func(){
		func(v func() ([]member, error)) func() { p := listUser; listUser = v; return func() { listUser = p } }(
			func() ([]member, error) { return []member{{PID: head, Start: start, Comm: "holder"}}, nil }),
		func(v func(int) (bool, error)) func() {
			p := checkSandboxed
			checkSandboxed = v
			return func() { checkSandboxed = p }
		}(
			func(int) (bool, error) { gone = true; return false, errProcessGone }),
		func(v func(int, string) (bool, error)) func() {
			p := checkWriteDenied
			checkWriteDenied = v
			return func() { checkWriteDenied = p }
		}(
			func(int, string) (bool, error) { return false, errProcessGone }),
		func(v func(int) (member, bool, error)) func() {
			p := readState
			readState = v
			return func() { readState = p }
		}(
			func(pid int) (member, bool, error) {
				if gone {
					return member{}, false, errProcessGone
				}
				return member{PID: pid, Start: start}, false, nil
			}),
	} {
		t.Cleanup(restore)
	}
	c, err := Contain(b)
	if err == nil {
		t.Fatalf("containment reported the boundary empty while a descendant held the grant: %s", c)
	}
	if len(c.Killed) != 0 || len(c.Stopped) != 0 {
		t.Fatalf("containment signalled %s; the holder was never in a listing", c)
	}
}

// Once the handoff chain ends — the last child exits and nothing holds the
// grant — a scan is stable and empty, and the sweep succeeds.
func TestContainSucceedsOnceTheHandoffChainEnds(t *testing.T) {
	b := boundaryFixture(t)
	for _, restore := range []func(){
		func(v func() ([]member, error)) func() { p := listUser; listUser = v; return func() { listUser = p } }(
			func() ([]member, error) { return nil, nil }),
	} {
		t.Cleanup(restore)
	}
	c, err := Contain(b)
	if err != nil || c.Found() {
		t.Fatalf("containment = %s, %v; want an empty boundary", c, err)
	}
}

// A grant lookup that errors establishes nothing: the sweep fails and
// signals nothing, rather than reporting the boundary empty.
func TestContainFailsClosedWhenALookupErrors(t *testing.T) {
	b := boundaryFixture(t)
	holder := startDetached(t, true, b.Canary)
	prior := checkSandboxed
	checkSandboxed = func(pid int) (bool, error) {
		if pid == holder {
			return false, errors.New("sandbox_check: returned -1")
		}
		return prior(pid)
	}
	t.Cleanup(func() { checkSandboxed = prior })
	c, err := Contain(b)
	if err == nil || !strings.Contains(err.Error(), "returned -1") || c.Found() {
		t.Fatalf("containment = %s, %v; want the lookup error and nothing signalled", c, err)
	}
	assertRunning(t, holder)
}

// A pid that, once stopped, is not the process the scan found — the pid was
// handed on between the listing and the signal — is continued and never
// killed. While it keeps answering as a fresh holder the boundary never
// settles, and the sweep fails rather than reporting it empty.
func TestContainReleasesAPidHandedOnBeforeTheStop(t *testing.T) {
	shortGrace(t)
	b := boundaryFixture(t)
	holder := startDetached(t, true, b.Canary)
	prior := readState
	readState = func(pid int) (member, bool, error) {
		m, stopped, err := prior(pid)
		if pid == holder && err == nil {
			m.Start = m.Start.Add(time.Second)
		}
		return m, stopped, err
	}
	t.Cleanup(func() { readState = prior })
	c, err := Contain(b)
	if err == nil {
		t.Fatalf("containment = %s, want a failure", c)
	}
	if !slices.Contains(c.Released, holder) || slices.Contains(c.Killed, holder) {
		t.Fatalf("containment = %s, want %d released and not killed", c, holder)
	}
	assertRunning(t, holder)
}

// weftRow is a hosted coding segment left running by a previous daemon: its
// recorded pid is gone and its run directory carries the canary.
func weftRow(t *testing.T, s *ledger.Store, ticket, repo string) (*ledger.WeftRun, Boundary) {
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
	b := SegmentBoundary(run.Segments[0].RunDir.String, run.Worktree)
	for _, dir := range []string{b.Canary, b.Worktree} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return run, b
}

// The daemon-restart pass: a segment whose worker is long gone left a
// detached holder of its grant. The pass kills it before it closes the row,
// and only the closed row lets the repository go.
func TestRestartContainsADetachedHolderBeforeTheRowCloses(t *testing.T) {
	s := newStore(t)
	repo := "/tmp/shuttle-contain-repo"
	run, b := weftRow(t, s, "demo/restart", repo)
	holder := startDetached(t, true, b.Canary)
	outcomes, err := Restart(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != ActionDead ||
		!strings.Contains(outcomes[0].Detail, "killed 1 ["+strconv.Itoa(holder)+"(perl)]") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	waitGone(t, "detached holder", holder)
	ledgerRun := run.Segments[0].LedgerRunID
	assertUnknown(t, s, ledgerRun)
	if err := s.ReconcileWeftUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after", Project: "demo", Repo: repo, Branch: "demo/after",
		Worktree: filepath.Join(t.TempDir(), "after"), RunDirRoot: t.TempDir(),
	}); err != nil {
		t.Fatalf("repository not released once the boundary was empty: %v", err)
	}
}

// A boundary the pass cannot establish empty holds the row in flight, and
// the queue's restart step keeps the repository reserved behind it.
func TestRestartHoldsTheRepositoryWhileTheBoundaryIsNotEmpty(t *testing.T) {
	s := newStore(t)
	repo := "/tmp/shuttle-contain-repo"
	run, _ := weftRow(t, s, "demo/restart", repo)
	prior := contain
	contain = func(Boundary) (Containment, error) {
		return Containment{Stopped: []int{4242}}, errors.New("1 holder(s) outlived SIGKILL")
	}
	t.Cleanup(func() { contain = prior })
	outcomes, err := Restart(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != ActionKillFailed ||
		!strings.Contains(outcomes[0].Detail, "outlived SIGKILL") {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	assertInFlight(t, s, run.Segments[0].LedgerRunID)
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
		t.Fatalf("claim behind an unsettled boundary = %v, want the repository busy", err)
	}
}

// failingCodingLedger is the real ledger with WeftCodingWorktree failing, and
// a record of every row MarkUnknown was asked to close.
type failingCodingLedger struct {
	*ledger.Store
	marked []string
}

func (l *failingCodingLedger) WeftCodingWorktree(string) (string, bool, error) {
	return "", false, errors.New("database is locked")
}

func (l *failingCodingLedger) MarkUnknown(id string) error {
	l.marked = append(l.marked, id)
	return l.Store.MarkUnknown(id)
}

// A ledger read that cannot say whether a row is a coding segment stops the
// pass with no sweep run. The row's outcome is held unresolved, never left
// with the closing action its process group earned, so the daemon's reconcile
// does not release a repository whose boundary nobody swept.
func TestRestartHoldsARowWhoseCodingLookupFails(t *testing.T) {
	s := newStore(t)
	run, _ := weftRow(t, s, "demo/restart", "/tmp/shuttle-contain-repo")
	ledgerRun := run.Segments[0].LedgerRunID
	l := &failingCodingLedger{Store: s}
	outcomes, err := Restart(l)
	if err == nil || !strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("restart error = %v, want the ledger read error", err)
	}
	if len(outcomes) != 1 || outcomes[0].RunID != ledgerRun || outcomes[0].Action != ActionUnresolved ||
		!strings.Contains(outcomes[0].Detail, "containment: database is locked") {
		t.Fatalf("outcomes = %+v, want the row held unresolved", outcomes)
	}
	if len(l.marked) != 0 {
		t.Fatalf("MarkUnknown closed %v with no sweep run", l.marked)
	}
	assertInFlight(t, s, ledgerRun)
}
