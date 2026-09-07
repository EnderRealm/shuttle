package reap

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"shuttle/internal/ledger"
)

// newStore opens a ledger on a real file, as the ledger's own tests do: one
// writer over a WAL file is what production runs.
func newStore(t *testing.T) *ledger.Store {
	t.Helper()
	s, err := ledger.Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// claim writes one in-flight row for the agent, stamped from at, and marks it
// running against pid and runDir when pid is non-zero. slot only has to be
// unique.
func claim(t *testing.T, s *ledger.Store, slot string, at time.Time, pid int, runDir string) *ledger.Run {
	t.Helper()
	s.SetClock(func() time.Time { return at })
	ts, err := time.Parse(time.RFC3339, slot)
	if err != nil {
		t.Fatalf("parse slot %q: %v", slot, err)
	}
	run, ok, err := s.Claim("daily", ts, ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim %s = (%t, %v), want the row", slot, ok, err)
	}
	if pid != 0 {
		if err := s.MarkRunning(run.ID, pid, runDir); err != nil {
			t.Fatalf("mark running: %v", err)
		}
	}
	return run
}

// spawn starts a process that outlives the test unless something kills it. It
// is reaped in the background because production's target is the orphan of a
// dead daemon, reaped by init, while a test's is this process's own child and
// would otherwise linger as a zombie that kill 0 still finds.
func spawn(t *testing.T, argv []string, setpgid bool) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	if setpgid {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %v: %v", argv, err)
	}
	pid := cmd.Process.Pid
	// Captured before the reaping goroutine can free the pid: cleanup has to
	// recheck the identity this process started with, since by then a pid the
	// pass killed can be alive again as something else entirely.
	start, startErr := processStart(pid)
	go cmd.Wait()
	t.Cleanup(func() {
		if startErr != nil {
			t.Logf("start time of pid %d is unreadable (%v); killing nothing", pid, startErr)
			return
		}
		if setpgid {
			// The whole group, or a descendant that ignored the pass's SIGTERM
			// outlives the test — but only while the group still holds the
			// leader this test started: a group the pass already emptied has an
			// id the kernel may have handed on.
			members, err := groupMembers(pid)
			if err != nil {
				t.Logf("process group %d is unreadable (%v); killing nothing", pid, err)
				return
			}
			if holds(members, member{PID: pid, Start: start}) {
				syscall.Kill(-pid, syscall.SIGKILL)
			}
			return
		}
		if now, err := processStart(pid); err == nil && now.Equal(start) {
			cmd.Process.Kill()
		}
	})
	return cmd
}

// ownedArgv is a shell leader carrying the runtime's settings argument for
// runDir (§7.4), which is the identity the pass matches a live pid against. The
// script must be more than one command: a shell given a single one execs over
// itself and the argv naming the run goes with it.
func ownedArgv(runDir, script string) []string {
	return []string{"sh", "-c", script, "sh", settingsFlag, filepath.Join(runDir, settingsFile)}
}

// recordSignals captures every signal the pass sends, which is what a test that
// cares about a group never being signalled twice has to assert on: the outcome
// alone cannot tell a SIGTERM that worked from a SIGKILL that followed it.
func recordSignals(t *testing.T) *[]string {
	t.Helper()
	var sent []string
	was := sendSignal
	sendSignal = func(pid int, sig syscall.Signal) error {
		sent = append(sent, fmt.Sprintf("%d %s", pid, sig))
		return was(pid, sig)
	}
	t.Cleanup(func() { sendSignal = was })
	return &sent
}

// signal renders one entry of what recordSignals collected.
func signal(pid int, sig syscall.Signal) string { return fmt.Sprintf("%d %s", pid, sig) }

func assertSignals(t *testing.T, sent *[]string, want ...string) {
	t.Helper()
	if strings.Join(*sent, ", ") != strings.Join(want, ", ") {
		t.Fatalf("signals sent = %v, want %v", *sent, want)
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// killPID cleans up a process that can outlive its group leader, which spawn's
// group kill no longer reaches once the leader is gone. Only while it is still
// the process registered here: a pid the pass already killed may have been
// handed on by cleanup time, and being alive says nothing about who it is.
func killPID(t *testing.T, pid int) {
	t.Helper()
	start, startErr := processStart(pid)
	t.Cleanup(func() {
		if startErr != nil {
			t.Logf("start time of pid %d is unreadable (%v); killing nothing", pid, startErr)
			return
		}
		if now, err := processStart(pid); err == nil && now.Equal(start) {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	})
}

// waitGone polls: a process the pass killed can still be an unreaped zombie
// for a moment after the signal.
func waitGone(t *testing.T, what string, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if !alive(pid) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s (pid %d) is still alive", what, pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requireIdentity skips a test whose answer depends on a pid's identity being
// readable. Only darwin can read it (kern.proc.pid, kern.procargs2,
// kern.proc.pgrp), so everywhere else the lookups establish nothing: the pass
// signals nothing and leaves the row in flight as unresolved.
func requireIdentity(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("process identity is readable on darwin only")
	}
}

// shortGrace keeps a test that does kill from sitting through the production
// grace period when the kill does not work.
func shortGrace(t *testing.T) {
	t.Helper()
	was := killGrace
	killGrace = 500 * time.Millisecond
	t.Cleanup(func() { killGrace = was })
}

func restart(t *testing.T, s *ledger.Store) []Outcome {
	t.Helper()
	outcomes, err := Restart(s)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	return outcomes
}

// assertReaped checks the row was closed as unknown and that nothing is left
// in flight: the restarted daemon has no row to adopt and its first tick sees
// no stale overlap.
func assertReaped(t *testing.T, s *ledger.Store, ids ...string) {
	t.Helper()
	for _, id := range ids {
		assertUnknown(t, s, id)
	}
	rows, err := s.InFlight()
	if err != nil {
		t.Fatalf("in flight: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("in flight after the pass = %d row(s), want none", len(rows))
	}
}

// assertInFlight checks the row was left running: a kill the pass could not
// establish keeps the overlap guard holding until a later daemon start closes it.
func assertInFlight(t *testing.T, s *ledger.Store, id string) {
	t.Helper()
	run, err := s.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if run.Status != ledger.StatusRunning {
		t.Fatalf("status = %s, want %s", run.Status, ledger.StatusRunning)
	}
	rows, err := s.InFlight()
	if err != nil {
		t.Fatalf("in flight: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("in flight = %+v, want only run %s", rows, id)
	}
}

func assertUnknown(t *testing.T, s *ledger.Store, id string) {
	t.Helper()
	run, err := s.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if run.Status != ledger.StatusUnknown {
		t.Fatalf("status = %s, want %s", run.Status, ledger.StatusUnknown)
	}
	if !run.EndedAt.Valid {
		t.Fatalf("run %s: ended_at unset on an unknown row", id)
	}
}

func assertOutcome(t *testing.T, got Outcome, run *ledger.Run, action Action, pid int) {
	t.Helper()
	if got.RunID != run.ID || got.Agent != run.Agent || got.PID != pid {
		t.Fatalf("outcome = %+v, want run %s agent %s pid %d", got, run.ID, run.Agent, pid)
	}
	if got.Action != action {
		t.Fatalf("action = %s (%s), want %s", got.Action, got.Detail, action)
	}
}

func TestRestartClosesAClaimedRowWithNoPID(t *testing.T) {
	s := newStore(t)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), 0, "")

	outcomes := restart(t, s)
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %+v, want the one row", outcomes)
	}
	assertOutcome(t, outcomes[0], run, ActionNoPID, 0)
	if outcomes[0].Detail != "" {
		t.Fatalf("detail = %q, want none", outcomes[0].Detail)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartSignalsNothingForADeadPID(t *testing.T) {
	// The group listing decides it: a leader gone is only the run gone when the
	// group it led is empty too, and only darwin can answer that.
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	// Killed and reaped before the pass, so the pid is gone from the kernel:
	// the row is closed and no signal is sent to anything.
	pid := deadPID(t)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, t.TempDir())

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionDead, pid)
	assertSignals(t, sent)
	assertReaped(t, s, run.ID)
}

// deadPID starts a process in its own group, kills it and reaps it, so the pid
// is gone from the kernel by the time the pass looks at it.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}
	cmd.Wait()
	return pid
}

func TestRestartKillsTheProcessGroupOfALiveRun(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	// A leader with a descendant, the shape a run has: the CLI spawns its own
	// tree, so the group is what has to go rather than the leader alone.
	dir := t.TempDir()
	child := filepath.Join(t.TempDir(), "child.pid")
	at := time.Now()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & echo $! > "+child+"; wait"), true)
	leader := cmd.Process.Pid
	descendant := readPID(t, child)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionKilled, leader)
	if outcomes[0].Detail != "" {
		t.Fatalf("detail = %q, want the kill to have been clean", outcomes[0].Detail)
	}
	waitGone(t, "the group leader", leader)
	waitGone(t, "the descendant", descendant)
	assertReaped(t, s, run.ID)
}

// A descendant that ignores the SIGTERM outlives a leader that does not, so the
// group SIGKILL may not be conditional on the leader still being there.
func TestRestartKillsAGroupMemberThatOutlivesTheSIGTERM(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	child := filepath.Join(t.TempDir(), "child.pid")
	// The trap keeps the descendant in the group through the group SIGTERM,
	// and the loop outlives the sleep that same signal kills.
	script := `sh -c 'trap "" TERM; while :; do sleep 0.2; done' >/dev/null 2>&1 &
echo $! > ` + child + `
sleep 300 &
wait
`
	at := time.Now()
	cmd := spawn(t, ownedArgv(dir, script), true)
	leader := cmd.Process.Pid
	descendant := readPID(t, child)
	killPID(t, descendant)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionKilled, leader)
	if outcomes[0].Detail != "" {
		t.Fatalf("detail = %q, want the kill to have been clean", outcomes[0].Detail)
	}
	assertSignals(t, sent, signal(-leader, syscall.SIGTERM), signal(-leader, syscall.SIGKILL))
	waitGone(t, "the group leader", leader)
	waitGone(t, "the descendant", descendant)
	assertReaped(t, s, run.ID)
}

// The other half of that: a group the SIGTERM emptied is never signalled again.
// These are orphans reaped by init the instant they exit, so once the group has
// been seen empty its id is free for reuse and a SIGKILL to it can land on an
// unrelated group.
func TestRestartSendsNoSIGKILLToAGroupTheSIGTERMEmptied(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	at := time.Now()
	// sh forwards nothing, but the group SIGTERM reaches the sleep too and the
	// shell dies on its wait, so the group empties on the first signal.
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	leader := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionKilled, leader)
	assertSignals(t, sent, signal(-leader, syscall.SIGTERM))
	waitGone(t, "the group leader", leader)
	assertReaped(t, s, run.ID)
}

// A group id reused between two probes: the run's own members are gone and an
// unrelated process answers for the id. It is never signalled again — never
// having observed the group empty is not continuity — and it cannot be told from
// a survivor the run forked either, so the pass fails closed and leaves the row
// in flight rather than reporting a kill it did not establish.
func TestRestartFailsClosedWhenOnlyStrangersHoldTheGroup(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	at := time.Now()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	leader := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)
	// The snapshot is the real group, so the SIGTERM goes to the run's own
	// processes; every later listing answers with the id held by a process that
	// started after them, which is what the reuse looks like from here.
	was := listGroup
	calls := 0
	listGroup = func(pgid int) ([]member, error) {
		calls++
		if calls == 1 {
			return was(pgid)
		}
		return []member{{PID: pgid, Start: time.Now()}}, nil
	}
	t.Cleanup(func() { listGroup = was })

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionUnresolved, leader)
	if !strings.Contains(outcomes[0].Detail, "unrecognised member(s) after SIGTERM") {
		t.Fatalf("detail = %q, want the unrecognised-members reason and the stage", outcomes[0].Detail)
	}
	assertSignals(t, sent, signal(-leader, syscall.SIGTERM))
	waitGone(t, "the group leader", leader)
	assertInFlight(t, s, run.ID)
}

// The group id already held by another process when the pass takes its snapshot:
// the listing does not hold the leader identify established, so nothing at all is
// signalled. The snapshot is only trusted because that process is still in it.
func TestRestartSignalsNothingWhenTheSnapshotLacksTheLeader(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	at := time.Now()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	leader := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)
	// The id answers, held by a process that carries the leader's pid with
	// another start time: that is what took the id, not the run's own leader.
	was := listGroup
	listGroup = func(pgid int) ([]member, error) {
		return []member{{PID: pgid, Start: time.Now().Add(time.Hour)}}, nil
	}
	t.Cleanup(func() { listGroup = was })

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionUnresolved, leader)
	assertSignals(t, sent)
	if !alive(leader) {
		t.Fatalf("pid %d was signalled", leader)
	}
	// Nothing was signalled and the members answering for the id cannot be
	// authenticated, so the row keeps the overlap guard rather than closing.
	assertInFlight(t, s, run.ID)
}

// The other side of that check: the substituted listing is empty, so the group
// went between identify's probe and the snapshot. There was never anything to
// signal, the run is gone and the row closes.
func TestRestartSignalsNothingWhenTheSnapshotIsEmpty(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	at := time.Now()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	leader := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)
	was := listGroup
	listGroup = func(int) ([]member, error) { return nil, nil }
	t.Cleanup(func() { listGroup = was })

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionDead, leader)
	if outcomes[0].Detail != "group emptied before signalling" {
		t.Fatalf("detail = %q, want the emptied-before-signalling reason", outcomes[0].Detail)
	}
	assertSignals(t, sent)
	if !alive(leader) {
		t.Fatalf("pid %d was signalled", leader)
	}
	assertReaped(t, s, run.ID)
}

// A SIGTERM handler that forks a survivor and exits: the group is left holding a
// process that was not in the snapshot. The pass cannot authenticate it as the
// run's, so it is never signalled and the kill counts as failed — closing the row
// would drop the overlap guard while that process is still alive.
func TestRestartFailsClosedWhenTheSIGTERMForksASurvivor(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	at := time.Now()
	leader, survivor := spawnForkSurvivor(t, dir)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionUnresolved, leader)
	if !strings.Contains(outcomes[0].Detail, "unrecognised member(s) after SIGTERM") {
		t.Fatalf("detail = %q, want the unrecognised-members reason and the stage", outcomes[0].Detail)
	}
	assertSignals(t, sent, signal(-leader, syscall.SIGTERM))
	// The leader is gone, so spawn's cleanup no longer covers the group: the
	// survivor is killed by its own pid.
	forked := readPID(t, survivor)
	killPID(t, forked)
	if !alive(forked) {
		t.Fatalf("the forked survivor (pid %d) was signalled", forked)
	}
	assertInFlight(t, s, run.ID)
}

// The next daemon start over that row: the leader the ledger recorded is gone,
// but the survivor it left behind still holds the group. A dead leader is not
// proof the run is gone, so the row keeps the overlap guard and nothing is
// signalled at a pid whose group cannot be authenticated — and once the survivor
// goes the group is empty and the row finally closes.
func TestRestartRechecksADeadLeaderWhoseSurvivorHoldsTheGroup(t *testing.T) {
	requireIdentity(t)
	shortGrace(t)
	s := newStore(t)
	dir := t.TempDir()
	at := time.Now()
	leader, survivor := spawnForkSurvivor(t, dir)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)

	assertOutcome(t, restart(t, s)[0], run, ActionUnresolved, leader)
	forked := readPID(t, survivor)
	killPID(t, forked)
	waitGone(t, "the group leader", leader)

	sent := recordSignals(t)
	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionUnresolved, leader)
	if !strings.Contains(outcomes[0].Detail, "leader gone") {
		t.Fatalf("detail = %q, want the leader-gone reason", outcomes[0].Detail)
	}
	assertSignals(t, sent)
	if !alive(forked) {
		t.Fatalf("the forked survivor (pid %d) was signalled", forked)
	}
	assertInFlight(t, s, run.ID)

	if err := syscall.Kill(forked, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the survivor: %v", err)
	}
	waitGone(t, "the forked survivor", forked)
	assertOutcome(t, restart(t, s)[0], run, ActionDead, leader)
	assertSignals(t, sent)
	assertReaped(t, s, run.ID)
}

// A leader that died leaving a descendant behind, which is the run whose CLI
// exited while a tool child lives on. Nothing is signalled at any stage: the
// descendant is in the run's group but carries none of the run's identity, so
// the pass cannot establish it and the row stays in flight.
func TestRestartSignalsNothingForADeadLeaderWithALiveDescendant(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	child := filepath.Join(t.TempDir(), "child.pid")
	at := time.Now()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & echo $! > "+child+"; wait"), true)
	leader := cmd.Process.Pid
	descendant := readPID(t, child)
	killPID(t, descendant)
	// The leader alone, by pid rather than by group: the descendant is
	// reparented and stays in the group the ledger recorded.
	if err := syscall.Kill(leader, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the leader: %v", err)
	}
	waitGone(t, "the group leader", leader)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, leader, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionUnresolved, leader)
	if !strings.Contains(outcomes[0].Detail, "leader gone") {
		t.Fatalf("detail = %q, want the leader-gone reason", outcomes[0].Detail)
	}
	assertSignals(t, sent)
	if !alive(descendant) {
		t.Fatalf("the descendant (pid %d) was signalled", descendant)
	}
	assertInFlight(t, s, run.ID)
}

// spawnForkSurvivor starts a run leader whose SIGTERM handler forks a fresh
// process into the group and exits: the survivor is a member no snapshot the
// pass took has ever seen. It returns the leader's pid and the path the
// survivor records its own pid at, once the trap has fired.
func spawnForkSurvivor(t *testing.T, runDir string) (int, string) {
	t.Helper()
	tmp := t.TempDir()
	survivor := filepath.Join(tmp, "survivor.pid")
	child := filepath.Join(tmp, "child.pid")
	// The backgrounded sleep dies on the group SIGTERM; the one the trap forks
	// inherits the group. The child's pid file is the readiness signal: written
	// after the trap is installed, so the pass cannot land a SIGTERM on a shell
	// that would still die on it.
	script := `trap 'sleep 300 & echo $! > ` + survivor + `; exit 0' TERM
sleep 300 & echo $! > ` + child + `
wait
`
	cmd := spawn(t, ownedArgv(runDir, script), true)
	readPID(t, child)
	return cmd.Process.Pid, survivor
}

func TestRestartNeverSignalsAReusedPID(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	// The recorded pid is live and leads its own group, but the process the
	// kernel handed it to started long after this run did: it is not the run's
	// process and a group kill aimed at it is unrecoverable.
	dir := t.TempDir()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	pid := cmd.Process.Pid
	at := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", at, pid, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionForeign, pid)
	window := fmt.Sprintf("%s..%s",
		at.Add(-startSlack).Format(time.RFC3339), at.Add(startSlack).Format(time.RFC3339))
	if !strings.Contains(outcomes[0].Detail, "process started") || !strings.Contains(outcomes[0].Detail, window) {
		t.Fatalf("detail = %q, want the process start and the window %s", outcomes[0].Detail, window)
	}
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartNeverSignalsAPIDThatDoesNotLeadItsGroup(t *testing.T) {
	s := newStore(t)
	// Started without Setpgid, so its group is this test's: the run's own
	// process is always a leader (§7.4), and signalling this group would
	// reach every unrelated process in it.
	dir := t.TempDir()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), false)
	pid := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionForeign, pid)
	if !strings.Contains(outcomes[0].Detail, "process-group leader") {
		t.Fatalf("detail = %q, want the group-leader reason", outcomes[0].Detail)
	}
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartNeverSignalsAPIDThatDoesNotNameTheRunDir(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	// Live, its own group's leader and started inside the window — every
	// condition the pid alone can satisfy — but it carries nothing of this
	// run's directory, so it is another process that took the pid.
	cmd := spawn(t, []string{"sleep", "300"}, true)
	pid := cmd.Process.Pid
	dir := t.TempDir()
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionForeign, pid)
	if !strings.Contains(outcomes[0].Detail, "does not carry --settings "+filepath.Join(dir, "settings.json")) {
		t.Fatalf("detail = %q, want the settings-argument reason for %s", outcomes[0].Detail, dir)
	}
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartNeverSignalsAPIDThatOnlyMentionsAFileUnderTheRunDir(t *testing.T) {
	// The argv lookup decides it, and only darwin can read one.
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	// Every condition the pid alone can satisfy, and its argv even names a path
	// under this run's directory — but not as the runtime's settings argument,
	// which is the identity. A mention is not the run.
	dir := t.TempDir()
	other := filepath.Join(dir, "other.txt")
	cmd := spawn(t, []string{"sh", "-c", "sleep 300 & wait", "sh", other}, true)
	pid := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionForeign, pid)
	assertSignals(t, sent)
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartNeverSignalsAPIDCarryingAnotherRunsSettings(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	// The settings argument is there, for a different run directory: the pid
	// belongs to another agent's run, and killing its group would stop it.
	dir := t.TempDir()
	cmd := spawn(t, ownedArgv(t.TempDir(), "sleep 300 & wait"), true)
	pid := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, dir)

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionForeign, pid)
	assertSignals(t, sent)
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartNeverSignalsARowWithNoRunDir(t *testing.T) {
	s := newStore(t)
	// Nothing to match the process against, so nothing is established: a row
	// carrying a pid and no run_dir is closed rather than signalled.
	cmd := spawn(t, []string{"sleep", "300"}, true)
	pid := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, "")

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionForeign, pid)
	if outcomes[0].Detail != "no run_dir recorded" {
		t.Fatalf("detail = %q, want the missing run_dir reason", outcomes[0].Detail)
	}
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertReaped(t, s, run.ID)
}

func TestRestartRecordsADeadPIDOnARowWithNoRunDirAsDead(t *testing.T) {
	// The group listing decides it, as for any dead pid, and only darwin can
	// answer that.
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	// Liveness is probed before the row's own contents are read: the pid is
	// gone, so the row is dead rather than foreign — there is no process left
	// for the missing run_dir to say anything about.
	pid := deadPID(t)
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, "")

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionDead, pid)
	if outcomes[0].Detail != "" {
		t.Fatalf("detail = %q, want none", outcomes[0].Detail)
	}
	assertSignals(t, sent)
	assertReaped(t, s, run.ID)
}

// A lookup that errors on a live pid establishes nothing — not that the pid is
// the run's, and not that it is not — so the row is held rather than closed as
// foreign: the process may still be the run's own, and a later pass that can
// read the kernel is the one to decide.
func TestRestartHoldsARowWhoseIdentityLookupFails(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	sent := recordSignals(t)
	dir := t.TempDir()
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	pid := cmd.Process.Pid
	run := claim(t, s, "2026-09-02T04:30:00-07:00", time.Now(), pid, dir)
	was := lookupIsRun
	lookupIsRun = func(int, string) (bool, error) { return false, errors.New("sysctl: boom") }
	t.Cleanup(func() { lookupIsRun = was })

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], run, ActionUnresolved, pid)
	if !strings.Contains(outcomes[0].Detail, "identity lookup failed: sysctl: boom") {
		t.Fatalf("detail = %q, want the lookup error and its prefix", outcomes[0].Detail)
	}
	assertSignals(t, sent)
	if !alive(pid) {
		t.Fatalf("pid %d was signalled", pid)
	}
	assertInFlight(t, s, run.ID)
}

func TestRestartLeavesARowInFlightWhenTheKillFails(t *testing.T) {
	requireIdentity(t)
	s := newStore(t)
	// No signal this daemon may send fails on demand, so the failure is
	// injected: what matters is what the pass records and what it leaves.
	was := kill
	kill = func(member) (Action, string) { return ActionKillFailed, "process group outlived SIGKILL" }
	t.Cleanup(func() { kill = was })
	dir := t.TempDir()
	base := time.Now().UTC().Truncate(time.Second)
	cmd := spawn(t, ownedArgv(dir, "sleep 300 & wait"), true)
	pid := cmd.Process.Pid
	stuck := claim(t, s, "2026-09-02T04:30:00-07:00", base, pid, dir)
	next := claim(t, s, "2026-09-03T04:30:00-07:00", base.Add(time.Minute), 0, "")

	outcomes := restart(t, s)
	assertOutcome(t, outcomes[0], stuck, ActionKillFailed, pid)
	if !strings.Contains(outcomes[0].Detail, "outlived SIGKILL") {
		t.Fatalf("detail = %q, want the kill error", outcomes[0].Detail)
	}
	// One unkillable process does not stop the pass: the rows after it are
	// still checked and closed.
	assertOutcome(t, outcomes[1], next, ActionNoPID, 0)
	assertUnknown(t, s, next.ID)
	// The row stays in flight, which is what keeps the gate from starting a
	// second run for the agent beside the processes that are still alive.
	assertInFlight(t, s, stuck.ID)
}

func TestRestartClosesEveryInFlightRowInLedgerOrder(t *testing.T) {
	s := newStore(t)
	base := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	first := claim(t, s, "2026-09-02T04:30:00-07:00", base, 0, "")
	second := claim(t, s, "2026-09-03T04:30:00-07:00", base.Add(time.Minute), 0, "")
	third := claim(t, s, "2026-09-04T04:30:00-07:00", base.Add(2*time.Minute), 0, "")
	// A terminal row is not the pass's business.
	done := claim(t, s, "2026-09-05T04:30:00-07:00", base.Add(3*time.Minute), 0, "")
	if err := s.Complete(done.ID, ledger.Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	outcomes := restart(t, s)
	var got []string
	for _, o := range outcomes {
		got = append(got, o.RunID)
	}
	want := []string{first.ID, second.ID, third.ID}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("outcomes = %v, want %v in claimed_at order", got, want)
	}
	assertReaped(t, s, want...)
	if run, err := s.Get(done.ID); err != nil || run.Status != ledger.StatusCompleted {
		t.Fatalf("completed row = (%v, %v), want it untouched", run, err)
	}
}

// readPID waits for the spawned descendant to record its own pid.
func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if body := strings.TrimSpace(string(raw)); err == nil && body != "" {
			pid, err := strconv.Atoi(body)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was never written", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
