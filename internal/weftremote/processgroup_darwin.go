//go:build darwin

package weftremote

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

const zombieProcess = 5

func processGroupLive(pgid int) (bool, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pgid)
	if err != nil {
		return false, fmt.Errorf("list worker process group %d: %w", pgid, err)
	}
	for _, proc := range procs {
		if proc.Proc.P_stat != zombieProcess {
			return true, nil
		}
	}
	return false, nil
}

// ttyGroups is every process group with a live member whose controlling
// terminal is the device dev: what is still running on a hosted terminal's
// PTY, whichever job it was started as.
func ttyGroups(dev int) ([]int, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.tty", dev)
	if err != nil {
		return nil, fmt.Errorf("list processes on tty %d: %w", dev, err)
	}
	var groups []int
	for _, proc := range procs {
		if proc.Proc.P_stat != zombieProcess {
			groups = append(groups, int(proc.Eproc.Pgid))
		}
	}
	return groups, nil
}

// pidIDType is P_PID in sys/wait.h, which x/sys does not export.
const pidIDType = 1

// waitExited blocks until the child pid has exited and leaves it unreaped:
// its zombie holds the pid, so the session the worker led cannot be handed
// on while the host sweeps it. darwin's wait4 ignores WNOWAIT and reaps, and
// x/sys exports no waitid here, so the system call is made directly. Nothing
// of the siginfo_t it fills (104 bytes on darwin) is read.
func waitExited(pid int) error {
	var info [104]byte
	for {
		_, _, errno := unix.Syscall6(unix.SYS_WAITID, pidIDType, uintptr(pid),
			uintptr(unsafe.Pointer(&info)), unix.WEXITED|unix.WNOWAIT, 0, 0)
		switch errno {
		case 0:
			return nil
		case unix.EINTR:
			continue
		}
		return fmt.Errorf("waitid %d: %w", pid, errno)
	}
}
