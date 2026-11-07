//go:build cgo

package weftremote

import (
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
)

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
		f := newSupervisorFixture(t)
		logs := &syncBuffer{}
		f.supervisor.log = log.New(logs, "", 0)
		run := f.claim(t, "demo/dead-tree")
		runDir := run.Segments[0].RunDir.String
		treeFile := filepath.Join(runDir, "tree.pids")
		script := "#!/bin/sh\n" + workerGuard + deadTreeScript + " > " + treeFile + "\nexit 9\n"
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
