//go:build cgo

package reap

/*
#include <stdlib.h>
#include <sys/types.h>

// sandbox_check is libsystem_sandbox's, which libSystem links by default. It
// is variadic, and Apple arm64 passes variadic arguments on the stack where
// cgo would pass them in registers, so it is only ever called from C.
extern int sandbox_check(pid_t pid, const char *operation, int type, ...);

enum {
	shuttle_filter_none = 0,
	shuttle_filter_path = 1,
	shuttle_check_no_report = 0x40000000,
};

static int shuttle_sandboxed(pid_t pid) {
	return sandbox_check(pid, NULL, shuttle_filter_none | shuttle_check_no_report);
}

static int shuttle_write_denied(pid_t pid, const char *path) {
	return sandbox_check(pid, "file-write-data", shuttle_filter_path | shuttle_check_no_report, path);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

// seatbeltSandboxed reports whether the kernel holds a seatbelt profile on
// pid. The profile is inherited across fork, exec and setsid and is never
// removed, so it follows every descendant a segment's worker started. A pid
// that went since it was listed answers sandboxed, or errProcessGone, and is
// then denied every path.
func seatbeltSandboxed(pid int) (bool, error) {
	r, errno := C.shuttle_sandboxed(C.pid_t(pid))
	return seatbeltAnswer(r, errno, fmt.Sprintf("sandbox_check %d", pid))
}

// seatbeltWriteDenied reports whether pid's profile denies writing path. A
// path that does not exist answers denied for every process. A pid that went
// since it was listed answers denied too, or errProcessGone: neither can show
// a grant.
func seatbeltWriteDenied(pid int, path string) (bool, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	r, errno := C.shuttle_write_denied(C.pid_t(pid), cpath)
	return seatbeltAnswer(r, errno, fmt.Sprintf("sandbox_check %d file-write-data %s", pid, path))
}

// seatbeltAnswer reads sandbox_check's result: 1 is yes, 0 is no, and
// anything else is a lookup that established nothing, bar ESRCH, which is the
// process gone. cgo reports errno whenever it is set, so it is read only on
// that failure.
func seatbeltAnswer(r C.int, errno error, what string) (bool, error) {
	switch r {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	if errors.Is(errno, syscall.ESRCH) {
		return false, errProcessGone
	}
	return false, fmt.Errorf("%s: returned %d: %v", what, int(r), errno)
}
