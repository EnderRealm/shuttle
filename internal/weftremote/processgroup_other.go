//go:build !darwin

package weftremote

import (
	"errors"
	"fmt"
	"syscall"
)

func processGroupLive(pgid int) (bool, error) {
	err := syscall.Kill(-pgid, 0)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return false, fmt.Errorf("probe worker process group %d: %w", pgid, err)
}

// ttyGroups lists nothing where the kernel's process table cannot be read by
// terminal: a hosted terminal's close then rests on the shell's own group and
// the PTY's foreground group.
func ttyGroups(int) ([]int, error) { return nil, nil }
