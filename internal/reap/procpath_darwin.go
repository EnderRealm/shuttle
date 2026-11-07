//go:build cgo

package reap

/*
#include <libproc.h>
#include <sys/proc_info.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

// execPath is the exec path the kernel holds for pid. proc_pidpath answers
// for another uid's process, where kern.procargs2 is EPERM. A pid with no
// live process answers errProcessGone.
func execPath(pid int) (string, error) {
	buf := make([]byte, C.PROC_PIDPATHINFO_MAXSIZE)
	n, errno := C.proc_pidpath(C.int(pid), unsafe.Pointer(&buf[0]), C.uint32_t(len(buf)))
	if n > 0 {
		return string(buf[:n]), nil
	}
	if errors.Is(errno, syscall.ESRCH) {
		return "", errProcessGone
	}
	return "", fmt.Errorf("proc_pidpath %d: %v", pid, errno)
}
