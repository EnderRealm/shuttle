//go:build darwin

package sandbox

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// UserTempDir is confstr(_CS_DARWIN_USER_TEMP_DIR), symlink-resolved so the
// profile names the /private/var path srt matches against. golang.org/x/sys
// does not expose confstr, so getconf reads it.
func UserTempDir() (string, error) {
	out, err := exec.Command("/usr/bin/getconf", "DARWIN_USER_TEMP_DIR").Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if evidence := head(string(exit.Stderr)); evidence != "" {
				return "", fmt.Errorf("getconf DARWIN_USER_TEMP_DIR: %w: %s", err, evidence)
			}
		}
		return "", fmt.Errorf("getconf DARWIN_USER_TEMP_DIR: %w", err)
	}
	dir := strings.TrimSpace(string(out))
	if !filepath.IsAbs(dir) {
		return "", fmt.Errorf("getconf DARWIN_USER_TEMP_DIR: %q is not absolute", dir)
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("resolve user temp dir %q: %w", dir, err)
	}
	return resolved, nil
}
