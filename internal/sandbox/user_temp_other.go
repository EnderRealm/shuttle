//go:build !darwin

package sandbox

import (
	"fmt"
	"os"
	"path/filepath"
)

// UserTempDir falls back to the resolved os.TempDir off macOS. Shuttle's
// installed service is macOS-only; this keeps fixture builds portable.
func UserTempDir() (string, error) {
	resolved, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return "", fmt.Errorf("resolve user temp dir: %w", err)
	}
	return resolved, nil
}
