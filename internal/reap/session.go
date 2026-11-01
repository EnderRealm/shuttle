package reap

import (
	"errors"
	"fmt"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// sessionMember is one live process of a session and the group it is in,
// with its kernel start time and, for a log line, its command name.
type sessionMember struct {
	PID, PGID int
	Start     time.Time
	Comm      string
}

// listSession is the kernel's answer for who is in a session. A var so a
// test can substitute a listing.
var listSession = sessionMembers

// SessionGroups is every process group with a live member in session sid.
func SessionGroups(sid int) ([]int, error) {
	members, err := listSession(sid)
	if err != nil {
		return nil, err
	}
	var groups []int
	for _, m := range members {
		groups = append(groups, m.PGID)
	}
	return groups, nil
}

// ProcessStart is when the kernel says the process began: what
// RestartSession later tells that process from the same pid reused.
func ProcessStart(pid int) (time.Time, error) { return lookupStart(pid) }

// RestartSession is the pass's process control for a hosted terminal's
// shell, which a previous daemon spawned as the leader of its own session and
// recorded by its pid and the kernel's start time for it (weft-service/v2
// §9.4, §12 step 3). What has to be established gone is the whole session —
// every job the shell started, in whatever process group — not the shell's
// group alone.
//
// A live leader established by its start time authenticates its session:
// every member is killed by group, SIGTERM and then SIGKILL, and the answer is
// ActionKilled once the session holds nothing. ActionDead is a leader gone
// with nothing left in its session, and ActionForeign a pid established as
// handed on to another process with nothing of the session left; both are the
// session established gone. Everything else keeps what the session held:
// ActionUnresolved for a live pid whose identity was not recorded or cannot be
// read, and for members left in a session whose leader is gone, which nothing
// authenticates as the shell's and so are never signalled; ActionKillFailed
// for members that outlived SIGKILL or that the kernel refused to signal.
func RestartSession(pid int, start time.Time) (Action, string) {
	if pid <= 0 {
		return ActionNoPID, ""
	}
	members, err := listSession(pid)
	if err != nil {
		return ActionUnresolved, lookupFailed + err.Error()
	}
	if err := syscall.Kill(pid, 0); err != nil {
		if !errors.Is(err, syscall.ESRCH) {
			// EPERM is a live process of another user's holding the pid,
			// which the daemon's shell never is: the pid was handed on, and
			// what is in session pid is that process's.
			return ActionForeign, "kill 0: " + err.Error()
		}
		if len(members) == 0 {
			return ActionDead, ""
		}
		return ActionUnresolved, fmt.Sprintf(
			"leader gone; session %d holds %d live member(s) the pass cannot authenticate", pid, len(members))
	}
	if start.IsZero() {
		return ActionUnresolved, "no process start was recorded for the live pid"
	}
	started, err := lookupStart(pid)
	if err != nil {
		return ActionUnresolved, lookupFailed + err.Error()
	}
	if !started.Equal(start) {
		reused := fmt.Sprintf("process started %s, not %s",
			started.UTC().Format(time.RFC3339Nano), start.UTC().Format(time.RFC3339Nano))
		// A process leading session pid now is the reused pid's own session;
		// members of a session it does not lead are the shell's orphans.
		if sid, err := unix.Getsid(pid); (err == nil && sid == pid) || len(members) == 0 {
			return ActionForeign, reused
		}
		return ActionUnresolved, fmt.Sprintf("%s, and session %d still holds %d member(s) the pass cannot authenticate",
			reused, pid, len(members))
	}
	if sid, err := unix.Getsid(pid); err != nil || sid != pid {
		return ActionUnresolved, fmt.Sprintf("the recorded shell %d does not lead its own session", pid)
	}
	return killSession(pid)
}

// killSession signals every process group with a member in session sid,
// SIGTERM then SIGKILL, until the session holds nothing. Its leader was
// authenticated by the caller; while it held the session id, every member
// was the shell's, and the id cannot pass to another session while any
// member remains.
func killSession(sid int) (Action, string) {
	var refused error
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		members, err := listSession(sid)
		if err != nil {
			return ActionKillFailed, err.Error()
		}
		if len(members) == 0 {
			return ActionKilled, ""
		}
		signalled := map[int]bool{}
		for _, m := range members {
			if signalled[m.PGID] {
				continue
			}
			signalled[m.PGID] = true
			if err := signalGroup(m.PGID, sig); err != nil && !errors.Is(err, errGroupGone) {
				refused = err
			}
		}
		empty, err := sessionEmpties(sid)
		if err != nil {
			return ActionKillFailed, err.Error()
		}
		if empty {
			return ActionKilled, ""
		}
	}
	if refused != nil {
		return ActionKillFailed, refused.Error()
	}
	return ActionKillFailed, fmt.Sprintf("session %d outlived SIGKILL", sid)
}

// sessionEmpties polls the session for up to killGrace and reports whether
// it held nothing.
func sessionEmpties(sid int) (bool, error) {
	deadline := time.Now().Add(killGrace)
	for {
		members, err := listSession(sid)
		if err != nil {
			return false, err
		}
		if len(members) == 0 {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(killPoll)
	}
}

// ContainSession establishes that session sid holds no live process, killing
// every one it does: what is left of a hosted segment's worker, which the
// host started as a session leader, once the worker's own process group is
// gone. The git and tk the worker runs lead process groups of their own but
// stay in its session, and outlive it there — a landing step's push still
// moving the remote a reconcile would observe.
//
// sid must be the pid of a leader that is not reaped yet. Its zombie holds
// the pid, so no process can start a session with that id while the sweep
// runs, and every process the listing answers is the leader's descendant.
//
// Each member is stopped, re-verified once the kernel has it stopped as the
// process the listing named, by its start time, and killed; a pid handed on
// between the listing and the stop is continued and recorded as released.
// The sweep relists until a listing holds nothing, so a child forked before
// its parent was stopped is found by the next one. A listing that errors
// establishes nothing and fails the sweep, as does a member still live after
// the grace period: the caller keeps the segment unsettled while it lives.
func ContainSession(sid int) (Containment, error) {
	var c Containment
	killed := map[int]time.Time{}
	deadline := time.Now().Add(killGrace)
	for {
		members, err := listSession(sid)
		if err != nil {
			return c, err
		}
		if len(members) == 0 {
			return c, nil
		}
		if c.Names == nil {
			c.Names = map[int]string{}
		}
		left := make([]int, len(members))
		for i, m := range members {
			left[i] = m.PID
			c.Names[m.PID] = m.Comm
		}
		if time.Now().After(deadline) {
			return c, fmt.Errorf("%d process(es) of session %d still live after %s: %s", len(members), sid, killGrace, c.pids(left))
		}
		for _, m := range members {
			if start, ok := killed[m.PID]; ok && start.Equal(m.Start) {
				continue
			}
			ok, err := killMember(m, deadline, &c)
			if err != nil {
				return c, err
			}
			if ok {
				killed[m.PID] = m.Start
			}
		}
		time.Sleep(killPoll)
	}
}

// killMember stops m and, once the kernel has it stopped and still says the
// pid is the process the listing named, kills it. A stopped process cannot
// exit and hand its pid on before the kill lands. It reports whether it sent
// the kill; a member that has not stopped by the deadline is left for the
// sweep's next listing to report.
func killMember(m sessionMember, deadline time.Time, c *Containment) (bool, error) {
	if err := sendSignal(m.PID, syscall.SIGSTOP); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, nil
		}
		return false, fmt.Errorf("stop %d: %w", m.PID, err)
	}
	for {
		now, isStopped, err := readState(m.PID)
		if errors.Is(err, errProcessGone) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !now.Start.Equal(m.Start) {
			return false, release(m.PID, c)
		}
		if isStopped {
			break
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(killPoll)
	}
	c.Stopped = append(c.Stopped, m.PID)
	if err := sendSignal(m.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, fmt.Errorf("kill %d: %w", m.PID, err)
	}
	c.Killed = append(c.Killed, m.PID)
	return true, nil
}
