//go:build cgo

package weftremote

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

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
	if want := reap.SegmentBoundary(runDir, run.Worktree); swept != want {
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
	f := newSupervisorFixture(t, false)
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
