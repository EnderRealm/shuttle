//go:build darwin

package sandbox

import (
	"bytes"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// filesystemSpelling is the path as the volume itself spells it. On a
// case-insensitive APFS volume /code/Weft and /code/weft open one directory,
// and symlink resolution keeps whichever spelling it was given; F_GETPATH
// answers with the names the directory entries carry, so every spelling of
// one checkout yields one canonical path and one repository_id.
// O_EVTONLY opens for the descriptor alone, so a checkout the daemon may not
// list still resolves.
func filesystemSpelling(path string) (string, error) {
	fd, err := unix.Open(path, unix.O_EVTONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", &os.PathError{Op: "open", Path: path, Err: err}
	}
	defer unix.Close(fd)
	buf := make([]byte, unix.PathMax)
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETPATH, int(uintptr(unsafe.Pointer(&buf[0])))); err != nil {
		return "", fmt.Errorf("F_GETPATH %s: %w", path, err)
	}
	if end := bytes.IndexByte(buf, 0); end >= 0 {
		buf = buf[:end]
	}
	return string(buf), nil
}
