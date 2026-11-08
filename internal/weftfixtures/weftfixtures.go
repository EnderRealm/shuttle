// Package weftfixtures locates weft's docs/service-v2, the weft-service/v2
// fixtures shuttle's tests hold its client and sandbox against.
package weftfixtures

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"shuttle/internal/gitenv"
)

// Resolve returns weft's docs/service-v2 for the checkout containing start:
// $WEFT_CONTRACT_DIR, a relative value resolved against that checkout's root,
// else the weft checkout beside the main checkout. The main checkout is the
// parent of the repository's common git directory, so a linked worktree (a
// hosted run's, under /Users/Shared/weft/worktrees) resolves to the same weft
// as the main checkout it was added from. A missing manifest.json is an error.
func Resolve(start string) (string, error) {
	dir := os.Getenv("WEFT_CONTRACT_DIR")
	if dir == "" {
		common, err := git(start, "rev-parse", "--path-format=absolute", "--git-common-dir")
		if err != nil {
			return "", err
		}
		dir = filepath.Join(filepath.Dir(filepath.Dir(common)), "weft", "docs", "service-v2")
	} else if !filepath.IsAbs(dir) {
		root, err := git(start, "rev-parse", "--show-toplevel")
		if err != nil {
			return "", err
		}
		dir = filepath.Join(root, dir)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		return "", err
	}
	return dir, nil
}

// Dir is Resolve as a test helper. Absent is a failure rather than a skip:
// these fixtures are the contract under test.
func Dir(t testing.TB, start string) string {
	t.Helper()
	dir, err := Resolve(start)
	if err != nil {
		t.Fatalf("weft-service/v2 fixtures are required (set WEFT_CONTRACT_DIR): %v", err)
	}
	return dir
}

func git(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = gitenv.Env()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s in %s: %w: %s", strings.Join(args, " "), dir, err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}
