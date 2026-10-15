package reap

import (
	"errors"
	"fmt"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// sessionMember is one live process of a session and the group it is in.
type sessionMember struct{ PID, PGID int }

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
