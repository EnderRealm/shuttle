//go:build darwin

package sandbox

import (
	"fmt"
	"os"
	"syscall"
)

// RepositoryIdentity is stable across symlink, /private and case aliases on a
// macOS volume. It is a reservation key only; worker argv still uses the
// canonical path.
func RepositoryIdentity(repo string) (string, error) {
	canonical, err := CanonicalRepository(repo)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("repository %q has no Darwin file identity", canonical)
	}
	return fmt.Sprintf("darwin:%d:%d", stat.Dev, stat.Ino), nil
}
