//go:build darwin

package cmd

import (
	"bytes"
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

// procArgvCarries reports whether the kernel answers for the process's argument
// space and that answer holds an entry ending in want. Only the end is anchored,
// which is enough for a readiness gate: this fixture's argv either carries the
// settings path or has not been published yet. The restart
// pass identifies a run by its argv, and exec.Cmd.Start returns after the fork
// rather than after the exec, so a fixture is only ready once the argv it was
// started with is actually visible. A forked child that has not exec'd yet
// answers with the argument space it inherited from this test binary, which is
// why the check is for the string and not merely for the lookup succeeding.
func procArgvCarries(pid int, want string) bool {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return false
	}
	return bytes.Contains(buf, append([]byte(want), 0))
}
