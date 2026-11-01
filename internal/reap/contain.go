package reap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"shuttle/internal/sandbox"
)

// Boundary is what a hosted coding segment's sandbox grant is recognised by
// on a process that outlived the segment's process group (a descendant that
// called setsid). Canary is a path only this segment's profile lets a process
// write; Control is a path its profile denies. Seatbelt profiles only narrow
// when nested, so every descendant of the segment is denied Control, while a
// process sandboxed more broadly than the segment — the system daemons granted
// the whole home directory — is allowed it and is no holder. The worktree is
// no part of it: host daemons and app-sandboxed helpers granted the per-user
// temp dir or the home tree can write a worktree there and be denied the run
// directory, and only the canary grant is specific to this segment.
type Boundary struct {
	Canary, Control string
}

// SegmentBoundary is the boundary of the segment whose run directory is
// runDir, from what the ledger records: the canary is the segment's TMPDIR and
// the control the run directory around it.
func SegmentBoundary(runDir string) Boundary {
	return Boundary{Canary: sandbox.SegmentTmpDir(runDir), Control: runDir}
}

// Containment is what one sweep did: the holders it stopped and verified,
// the pids it stopped and let go again because they were no longer a holder
// once stopped, and the holders it killed. Names is the command name the
// listing gave each pid it signalled, for the log line.
type Containment struct {
	Stopped, Released, Killed []int
	Names                     map[int]string
}

// Found reports whether the sweep signalled anything.
func (c Containment) Found() bool { return len(c.Stopped) > 0 || len(c.Released) > 0 }

func (c Containment) String() string {
	return fmt.Sprintf("stopped %d %s, released %d %s, killed %d %s",
		len(c.Stopped), c.pids(c.Stopped), len(c.Released), c.pids(c.Released), len(c.Killed), c.pids(c.Killed))
}

// pids renders list as pid(comm), or the bare pid where no name was listed.
func (c Containment) pids(list []int) string {
	parts := make([]string, len(list))
	for i, pid := range list {
		parts[i] = strconv.Itoa(pid)
		if name := c.Names[pid]; name != "" {
			parts[i] += "(" + name + ")"
		}
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// errProcessGone is a lookup on a pid that no longer has a live process.
var errProcessGone = errors.New("no such process")

// The kernel lookups the sweep runs. Vars so a test can drive a lookup that
// fails or a pid handed on, which no process a test starts produces on demand.
var (
	listUser         = userProcesses
	readState        = processState
	checkSandboxed   = seatbeltSandboxed
	checkWriteDenied = seatbeltWriteDenied
)

// Contain establishes that no process of this user holds the boundary's
// grant, killing the ones that do. A holder is a sandboxed process its
// profile lets write the canary and denies the control.
//
// Holders are stopped first, scan after scan, until a scan finds none it has
// not already stopped: a stopped process cannot fork, so from then on the set
// cannot grow. Each is re-verified once the kernel reports it stopped — the
// same start time and still a holder — and one that is not is continued and
// left alone, since its pid was handed on between the scan and the signal.
// The stopped holders are then killed, and the sweep succeeds only once a
// scan finds no holder at all.
//
// Every lookup that errors establishes nothing, so the sweep fails, never
// reports the boundary empty; a control that does not exist fails it too,
// since a missing path answers denied for every process. Only a stable scan
// can end the sweep (scanHolders): one across which the canary stayed the one
// directory and every process judged a non-holder was the same live process
// the listing named. A canary or a process that keeps changing past the grace
// period fails it. A failed sweep leaves what it stopped stopped, unable to
// write, and the caller keeps the repository reservation.
func Contain(b Boundary) (Containment, error) {
	var c Containment
	for _, path := range []string{b.Canary, b.Control} {
		if !filepath.IsAbs(path) || path != filepath.Clean(path) {
			return c, fmt.Errorf("boundary path %q is not a clean absolute path", path)
		}
	}
	stopped := map[int]member{}
	deadline := time.Now().Add(killGrace)
	for {
		holders, stable, err := scanHolders(b)
		if err != nil {
			return c, err
		}
		fresh := false
		for _, h := range holders {
			if s, ok := stopped[h.PID]; ok && s.Start.Equal(h.Start) {
				continue
			}
			fresh = true
			if c.Names == nil {
				c.Names = map[int]string{}
			}
			c.Names[h.PID] = h.Comm
			held, err := stopHolder(h, b, &c)
			if err != nil {
				return c, err
			}
			if held {
				stopped[h.PID] = h
				c.Stopped = append(c.Stopped, h.PID)
			}
		}
		if !fresh && stable {
			break
		}
		if time.Now().After(deadline) {
			if !stable {
				return c, errScanUnstable(b)
			}
			return c, fmt.Errorf("holders of %s still appearing after %s", b.Canary, killGrace)
		}
		if !fresh {
			time.Sleep(killPoll)
		}
	}
	for _, h := range stopped {
		now, _, err := readState(h.PID)
		if errors.Is(err, errProcessGone) {
			continue
		}
		if err != nil {
			return c, err
		}
		if !now.Start.Equal(h.Start) {
			continue
		}
		if err := sendSignal(h.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return c, fmt.Errorf("kill holder %d: %w", h.PID, err)
		}
		c.Killed = append(c.Killed, h.PID)
	}
	deadline = time.Now().Add(killGrace)
	for {
		holders, stable, err := scanHolders(b)
		if err != nil {
			return c, err
		}
		if len(holders) == 0 && stable {
			return c, nil
		}
		if time.Now().After(deadline) {
			if len(holders) == 0 {
				return c, errScanUnstable(b)
			}
			left := make([]int, len(holders))
			for i, h := range holders {
				left[i] = h.PID
			}
			return c, fmt.Errorf("%d holder(s) of %s outlived SIGKILL: %s", len(holders), b.Canary, c.pids(left))
		}
		time.Sleep(killPoll)
	}
}

// stopHolder stops h and reports whether, once the kernel has it stopped, it
// is still the holder the scan found. kill returns once the signal is posted,
// and a fork in flight can still complete before the stop lands, so the
// re-scan that follows is trusted only after the stop is observed. A pid that
// is no longer the holder is continued and recorded as released.
func stopHolder(h member, b Boundary, c *Containment) (bool, error) {
	if err := sendSignal(h.PID, syscall.SIGSTOP); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, nil
		}
		return false, fmt.Errorf("stop holder %d: %w", h.PID, err)
	}
	deadline := time.Now().Add(killGrace)
	for {
		now, isStopped, err := readState(h.PID)
		if errors.Is(err, errProcessGone) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !now.Start.Equal(h.Start) {
			return false, release(h.PID, c)
		}
		if isStopped {
			break
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf("holder %d did not stop within %s", h.PID, killGrace)
		}
		time.Sleep(killPoll)
	}
	for {
		canary, err := prepareBoundary(b)
		if err != nil {
			return false, err
		}
		held, err := holdsGrant(h.PID, b)
		if err != nil {
			return false, err
		}
		after, err := canaryDir(b)
		if err != nil {
			return false, err
		}
		same := after != nil && os.SameFile(canary, after)
		switch {
		case same && held:
			return true, nil
		case same:
			return false, release(h.PID, c)
		case time.Now().After(deadline):
			return false, errCanaryChanging(b)
		}
		time.Sleep(killPoll)
	}
}

func release(pid int, c *Containment) error {
	c.Released = append(c.Released, pid)
	if err := sendSignal(pid, syscall.SIGCONT); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("continue released pid %d: %w", pid, err)
	}
	return nil
}

// scanHolders is every process of this user that holds the boundary's grant,
// and whether the scan is stable: whether the canary stayed the one directory
// prepareBoundary readied, and every process judged a non-holder was the same
// live process the listing named, across every check in it.
//
// A grant check is path-based, so it answers for whatever is at the canary
// path at that instant: a holder that removed the canary reads as denied, and
// one that put a symlink there makes the answer the symlink target's. The
// canary's identity (device and inode, still a directory) is therefore read
// on either side of each process's check, and the answer stands only when the
// same directory was there throughout. A holder cannot move the canary aside
// and back — renaming it needs a write to the run directory, which no holder
// is granted — so an unchanged identity is the same directory.
//
// A grant check is also pid-based, and a listed process can fork a child and
// exit around the instant it is queried: the departed pid then answers denied
// or process-gone — a non-holder — while the child it forked holds the grant
// and is not in this listing. A negative answer is therefore trusted only
// when the kernel start time says the same process the listing named was live
// on both sides of the check; a pid that is gone, reused or unreadable makes
// the scan unstable rather than evidence of absence, so the sweep rescans
// within the grace instead of releasing the reservation beside a live
// descendant. A process whose canary or identity answer does not stand is not
// counted either way; its positives stand, but its absence establishes
// nothing, and the next scan repairs the canary and relists.
func scanHolders(b Boundary) ([]member, bool, error) {
	readied, err := prepareBoundary(b)
	if err != nil {
		return nil, false, err
	}
	procs, err := listUser()
	if err != nil {
		return nil, false, err
	}
	var holders []member
	stable := true
	for _, p := range procs {
		canaryBefore, err := canaryDir(b)
		if err != nil {
			return nil, false, err
		}
		idBefore := sameProcess(p)
		held, err := holdsGrant(p.PID, b)
		if err != nil {
			return nil, false, err
		}
		idAfter := sameProcess(p)
		canaryAfter, err := canaryDir(b)
		if err != nil {
			return nil, false, err
		}
		if canaryBefore == nil || canaryAfter == nil || !os.SameFile(canaryBefore, canaryAfter) {
			stable = false
			continue
		}
		if !os.SameFile(readied, canaryAfter) {
			stable = false
		}
		if held {
			holders = append(holders, p)
			continue
		}
		if !idBefore || !idAfter {
			stable = false
		}
	}
	return holders, stable, nil
}

// sameProcess reports whether the listing's member is still the one live
// process it named, by the kernel start time that tells it from the same pid
// reused. A pid that has gone, that a new process reused, or whose identity
// the kernel will not read — a process vanishing mid-sysctl answers the read
// with an error, not a clean absence — is not established as it, which the
// caller treats as an unstable scan rather than as the process's absence.
func sameProcess(p member) bool {
	now, _, err := readState(p.PID)
	if err != nil {
		return false
	}
	return now.Start.Equal(p.Start)
}

// canaryDir is the canary's identity while it is a directory, and nil while
// it is missing or anything else.
func canaryDir(b Boundary) (os.FileInfo, error) {
	info, err := os.Lstat(b.Canary)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, nil
	}
	return info, nil
}

// errCanaryChanging is a boundary whose canary a holder kept removing or
// replacing for the whole grace period: no scan could establish who holds the
// grant, so the sweep fails closed.
func errCanaryChanging(b Boundary) error {
	return fmt.Errorf("canary %s kept changing for %s", b.Canary, killGrace)
}

// errScanUnstable is a boundary that would not hold still for one scan within
// the grace period — a canary or a listed process changing under every scan —
// so no scan could establish who holds the grant and the sweep fails closed.
func errScanUnstable(b Boundary) error {
	return fmt.Errorf("boundary %s did not hold still for a scan within %s", b.Canary, killGrace)
}

// prepareBoundary readies the boundary's paths for a scan. A path that does
// not exist answers denied for every process, so a missing control would read
// as an empty boundary and fails the scan instead.
//
// The canary is recreated rather than refused: it is the root of a subpath
// the segment's profile grants, so a holder may rmdir it, or put a file or
// symlink in its place, and refusing would let any segment wedge its
// repository's reservation for good. A holder that does so again while a scan
// runs is caught by the identity check scanHolders makes around each process.
//
// It answers the canary's identity once it is a directory.
func prepareBoundary(b Boundary) (os.FileInfo, error) {
	if _, err := os.Stat(b.Control); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(killGrace)
	for {
		info, err := readyCanary(b)
		if err == nil || !errors.Is(err, errCanaryRaced) {
			return info, err
		}
		if time.Now().After(deadline) {
			return nil, errCanaryChanging(b)
		}
		time.Sleep(killPoll)
	}
}

// errCanaryRaced is a holder changing the canary between the host's look at
// it and its repair, which the host meets by looking again.
var errCanaryRaced = errors.New("canary changed under its repair")

func readyCanary(b Boundary) (os.FileInfo, error) {
	info, err := os.Lstat(b.Canary)
	switch {
	case err == nil && info.IsDir():
		return info, nil
	case err == nil:
		if err := os.Remove(b.Canary); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("replace canary: %w", err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	if err := os.Mkdir(b.Canary, 0o700); errors.Is(err, os.ErrExist) {
		return nil, errCanaryRaced
	} else if err != nil {
		return nil, fmt.Errorf("recreate canary: %w", err)
	}
	info, err = os.Lstat(b.Canary)
	if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
		return nil, errCanaryRaced
	}
	if err != nil {
		return nil, err
	}
	return info, nil
}

// holdsGrant reports whether pid is sandboxed, allowed to write the canary,
// and denied the control. A process gone since it was listed holds nothing.
func holdsGrant(pid int, b Boundary) (bool, error) {
	held, err := grantHeld(pid, b)
	if errors.Is(err, errProcessGone) {
		return false, nil
	}
	return held, err
}

func grantHeld(pid int, b Boundary) (bool, error) {
	sandboxed, err := checkSandboxed(pid)
	if err != nil || !sandboxed {
		return false, err
	}
	denied, err := checkWriteDenied(pid, b.Canary)
	if err != nil || denied {
		return false, err
	}
	return checkWriteDenied(pid, b.Control)
}
