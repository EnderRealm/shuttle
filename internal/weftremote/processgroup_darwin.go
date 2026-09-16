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
