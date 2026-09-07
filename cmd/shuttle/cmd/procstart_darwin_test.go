//go:build darwin

package cmd

import (
	"time"

	"golang.org/x/sys/unix"
)

// procStart is the kernel's start time for a pid, which is what tells a process
// this test started from whatever took its pid after it died. The daemon test
// needs it to clean up safely and cannot reach internal/reap's copy.
func procStart(pid int) (time.Time, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return time.Time{}, err
	}
	sec, nsec := kp.Proc.P_starttime.Unix()
	return time.Unix(sec, nsec), nil
}
