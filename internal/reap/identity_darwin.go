package reap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// processStart is when the kernel says the process began, which is what makes
// a recorded pid distinguishable from the same pid reused.
func processStart(pid int) (time.Time, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return time.Time{}, fmt.Errorf("kern.proc.pid %d: %w", pid, err)
	}
	// Some macOS versions answer for a dead pid with a zeroed record rather
	// than ESRCH, and a zero start time would read as a process from 1970
	// rather than as the missing answer it is.
	if kp.Proc.P_pid != int32(pid) {
		return time.Time{}, fmt.Errorf("kern.proc.pid %d: no such process", pid)
	}
	sec, nsec := kp.Proc.P_starttime.Unix()
	return time.Unix(sec, nsec), nil
}

// szomb is a process that has exited and is not reaped yet (SZOMB in
// sys/proc.h, which x/sys does not export).
const szomb = 5

// groupMembers is the kernel's view of a process group: every process still in
// it, each with the start time that tells it from the same pid reused. An empty
// answer is a group with no members left.
func groupMembers(pgid int) ([]member, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pgid)
	if err != nil {
		return nil, fmt.Errorf("kern.proc.pgrp %d: %w", pgid, err)
	}
	members := make([]member, 0, len(procs))
	for _, kp := range procs {
		// A member that took the signal and is not reaped yet is not a
		// survivor; counting it would report kill_failed against a dead group
		// whenever reaping lags.
		if kp.Proc.P_stat == szomb {
			continue
		}
		sec, nsec := kp.Proc.P_starttime.Unix()
		members = append(members, member{PID: int(kp.Proc.P_pid), Start: time.Unix(sec, nsec)})
	}
	return members, nil
}

// processArgv is the process's argv as kern.procargs2 answers it.
// kern.procargs2 answers for a process of the same uid only. Another user's
// process is already foreign by identify's kill 0, so an EPERM here is a
// lookup that establishes nothing and the row is held rather than closed.
func processArgv(pid int) ([]string, error) {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return nil, fmt.Errorf("kern.procargs2 %d: %w", pid, err)
	}
	argv, err := parseProcArgs(buf)
	if err != nil {
		return nil, fmt.Errorf("kern.procargs2 %d: %w", pid, err)
	}
	return argv, nil
}

// parseProcArgs reads the argv out of a kern.procargs2 answer. The layout is a
// little-endian int32 argc, the NUL-terminated exec path, a run of NUL padding,
// then argc NUL-terminated argv strings; whatever follows them is the
// environment and is not argv, so exactly argc strings are taken.
func parseProcArgs(buf []byte) ([]string, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("%d byte answer", len(buf))
	}
	argc := int(int32(binary.LittleEndian.Uint32(buf[:4])))
	if argc < 0 {
		return nil, fmt.Errorf("argc %d", argc)
	}
	rest := buf[4:]
	end := bytes.IndexByte(rest, 0)
	if end < 0 {
		return nil, errors.New("unterminated exec path")
	}
	rest = rest[end:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}
	var argv []string
	for len(argv) < argc {
		end := bytes.IndexByte(rest, 0)
		if end < 0 {
			return nil, fmt.Errorf("argv holds %d of %d arguments", len(argv), argc)
		}
		argv = append(argv, string(rest[:end]))
		rest = rest[end+1:]
	}
	return argv, nil
}

// sessionMembers is every live process whose session id is sid, with its
// process group and the start time that tells it from the same pid reused:
// what is left of a session whose leader the pass recorded.
// Processes that went between the listing and the getsid are skipped.
func sessionMembers(sid int) ([]sessionMember, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, fmt.Errorf("kern.proc.all: %w", err)
	}
	var members []sessionMember
	for _, kp := range procs {
		if kp.Proc.P_stat == szomb {
			continue
		}
		pid := int(kp.Proc.P_pid)
		s, err := unix.Getsid(pid)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("getsid %d: %w", pid, err)
		}
		if s == sid {
			sec, nsec := kp.Proc.P_starttime.Unix()
			members = append(members, sessionMember{
				PID: pid, PGID: int(kp.Eproc.Pgid), Start: time.Unix(sec, nsec), Comm: unix.ByteSliceToString(kp.Proc.P_comm[:]),
			})
		}
	}
	return members, nil
}

// sstop is a process the kernel has stopped (SSTOP in sys/proc.h).
const sstop = 4

// userProcesses is every live process of this user but the daemon itself,
// each with the start time that tells it from the same pid reused.
func userProcesses() ([]member, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.uid", unix.Geteuid())
	if err != nil {
		return nil, fmt.Errorf("kern.proc.uid %d: %w", unix.Geteuid(), err)
	}
	self := unix.Getpid()
	members := make([]member, 0, len(procs))
	for _, kp := range procs {
		if kp.Proc.P_stat == szomb || int(kp.Proc.P_pid) == self {
			continue
		}
		sec, nsec := kp.Proc.P_starttime.Unix()
		members = append(members, member{
			PID: int(kp.Proc.P_pid), Start: time.Unix(sec, nsec), Comm: unix.ByteSliceToString(kp.Proc.P_comm[:]),
		})
	}
	return members, nil
}

// uidProcesses is every live process whose real uid is uid, with its parent
// and exec path. The path is read after the listing, so a process whose start
// time no longer matches the listing's once it is read keeps no path: it is
// then a survivor rather than an exempt service, and the caller lists again.
func uidProcesses(uid int) ([]UserProcess, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.ruid", uid)
	if err != nil {
		return nil, fmt.Errorf("kern.proc.ruid %d: %w", uid, err)
	}
	out := make([]UserProcess, 0, len(procs))
	for _, kp := range procs {
		if kp.Proc.P_stat == szomb {
			continue
		}
		sec, nsec := kp.Proc.P_starttime.Unix()
		p := UserProcess{
			PID: int(kp.Proc.P_pid), PPID: int(kp.Eproc.Ppid), Start: time.Unix(sec, nsec),
			Comm: unix.ByteSliceToString(kp.Proc.P_comm[:]),
		}
		path, err := execPath(p.PID)
		if errors.Is(err, errProcessGone) {
			continue
		}
		now, _, stateErr := readState(p.PID)
		switch {
		case errors.Is(stateErr, errProcessGone):
			continue
		case stateErr != nil:
			return nil, stateErr
		case err == nil && now.Start.Equal(p.Start):
			p.Path = path
		}
		out = append(out, p)
	}
	return out, nil
}

// processState is the kernel's current answer for pid: the process holding
// it, and whether the kernel has it stopped. errProcessGone is a pid with no
// live process.
func processState(pid int) (member, bool, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) {
		return member{}, false, errProcessGone
	}
	if err != nil {
		return member{}, false, fmt.Errorf("kern.proc.pid %d: %w", pid, err)
	}
	// A dead pid can answer with a zeroed record rather than ESRCH.
	if kp.Proc.P_pid != int32(pid) || kp.Proc.P_stat == szomb {
		return member{}, false, errProcessGone
	}
	sec, nsec := kp.Proc.P_starttime.Unix()
	return member{PID: pid, Start: time.Unix(sec, nsec)}, kp.Proc.P_stat == sstop, nil
}
