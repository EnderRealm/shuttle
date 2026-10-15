//go:build darwin

package weftremote

import (
	"fmt"

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
