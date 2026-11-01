package reap

import (
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// startSession starts script as the leader of its own session, as a hosted
// terminal's shell is, and answers its pid and kernel start time.
func startSession(t *testing.T, script string) (*exec.Cmd, int, time.Time) {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		members, _ := sessionMembers(pid)
		for _, m := range members {
			_ = syscall.Kill(-m.PGID, syscall.SIGKILL)
		}
	})
	start, err := ProcessStart(pid)
	if err != nil {
		t.Fatal(err)
	}
	return cmd, pid, start
}

// RestartSession signals only a leader established by its kernel start time,
// and then kills its whole session — a job in its own process group that
// ignores SIGHUP included. A live pid with no recorded start, or the same pid
// with another start time, is never signalled.
func TestRestartSessionKillsAnEstablishedSessionOnly(t *testing.T) {
	cmd, pid, start := startSession(t, `set -m; sh -c 'trap "" HUP; exec sleep 60' & exec sleep 60`)
	reaped := make(chan struct{})
	go func() { _ = cmd.Wait(); close(reaped) }()
	var members []sessionMember
	deadline := time.Now().Add(5 * time.Second)
	for len(members) < 2 && time.Now().Before(deadline) {
		members, _ = sessionMembers(pid)
		time.Sleep(10 * time.Millisecond)
	}
	if len(members) != 2 || members[0].PGID == members[1].PGID {
		t.Fatalf("session members = %+v, want the shell and a job in its own group", members)
	}
	// No recorded start: unresolved. Another start time on a pid that leads
	// its own session: that session is the reused pid's, and the recorded
	// shell's is gone.
	for recorded, want := range map[time.Time]Action{{}: ActionUnresolved, start.Add(time.Second): ActionForeign} {
		if action, detail := RestartSession(pid, recorded); action != want {
			t.Fatalf("start %v: %s %s, want %s", recorded, action, detail, want)
		}
	}
	if left, _ := sessionMembers(pid); len(left) != 2 {
		t.Fatalf("an unestablished session was signalled: %+v", left)
	}
	if action, detail := RestartSession(pid, start); action != ActionKilled {
		t.Fatalf("established session: %s %s", action, detail)
	}
	<-reaped
	if left, err := sessionMembers(pid); err != nil || len(left) != 0 {
		t.Fatalf("session after the kill: %+v %v", left, err)
	}
	if action, _ := RestartSession(pid, start); action != ActionDead {
		t.Fatalf("gone session: %s", action)
	}
	if action, _ := RestartSession(0, start); action != ActionNoPID {
		t.Fatalf("no pid: %s", action)
	}
}

// Members left in a session whose leader is gone authenticate nothing: they
// are held, never signalled.
func TestRestartSessionHoldsOrphansOfAGoneLeader(t *testing.T) {
	cmd, pid, start := startSession(t, "sleep 60 & exit 0")
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if action, detail := RestartSession(pid, start); action != ActionUnresolved {
		t.Fatalf("orphaned session: %s %s", action, detail)
	}
	if left, _ := sessionMembers(pid); len(left) != 1 {
		t.Fatalf("orphans were signalled: %+v", left)
	}
}

// A pid alive under another user is a pid handed on, never the daemon's
// shell: the shell's session is gone, whatever session the pid's new owner
// leads. launchd, pid 1, is root's and leads session 1.
func TestRestartSessionTakesAnotherUsersPidAsHandedOn(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root may signal pid 1")
	}
	if members, err := sessionMembers(1); err != nil || len(members) == 0 {
		t.Fatalf("session 1 = %+v %v", members, err)
	}
	action, detail := RestartSession(1, time.Now())
	if action != ActionForeign || !strings.HasPrefix(detail, "kill 0: ") {
		t.Fatalf("pid 1: %s %s", action, detail)
	}
}

// A worker's children lead process groups of their own but stay in its
// session, and outlive it there. With the leader exited and not yet reaped,
// the sweep kills them — one that ignores SIGTERM included — and leaves a
// process in any other session alone.
func TestContainSessionKillsADeadLeadersOrphans(t *testing.T) {
	cmd, pid, _ := startSession(t, `set -m; sh -c 'trap "" TERM; exec sleep 60' & exit 0`)
	otherCmd, other, otherStart := startSession(t, `trap "" TERM; exec sleep 60`)
	var members []sessionMember
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, err := processState(pid)
		members, _ = sessionMembers(pid)
		if errors.Is(err, errProcessGone) && len(members) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader %d: %v, session members %+v; want the leader exited and one orphan", pid, err, members)
		}
		time.Sleep(10 * time.Millisecond)
	}
	orphan := members[0]
	if orphan.PGID == pid {
		t.Fatalf("orphan %+v is in the leader's process group", orphan)
	}
	c, err := ContainSession(pid)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Killed, []int{orphan.PID}) || c.Names[orphan.PID] != "sleep" {
		t.Fatalf("containment %s, want only orphan %d killed", c, orphan.PID)
	}
	waitGone(t, "orphan", orphan.PID)
	if now, stopped, err := processState(other); err != nil || !now.Start.Equal(otherStart) || stopped {
		t.Fatalf("process %d of another session was signalled: %+v stopped=%v %v", other, now, stopped, err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	_ = syscall.Kill(other, syscall.SIGKILL)
	_ = otherCmd.Wait()
}
