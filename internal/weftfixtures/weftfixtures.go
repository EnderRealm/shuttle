// Package weftfixtures locates weft's docs/service-v2, the weft-service/v2
// fixtures shuttle's tests hold its client and sandbox against.
package weftfixtures

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"shuttle/internal/gitenv"
)

// Resolve returns weft's docs/service-v2 for the checkout containing start.
// By default it is the tree committed at HEAD of the weft checkout beside the
// main checkout, extracted into scratch: an uncommitted edit there is another
// change's work in flight, and must not move shuttle's verdict. The main
// checkout is the parent of the repository's common git directory, so a linked
// worktree (a hosted run's, under /Users/Shared/weft/worktrees) resolves to the
// same weft as the main checkout it was added from. $WEFT_CONTRACT_DIR, a
// relative value resolved against start's checkout root, overrides that with a
// live directory, so a contract change can be tested before it is committed;
// scratch is then unused. A missing manifest.json is an error.
func Resolve(start, scratch string) (string, error) {
	dir := os.Getenv("WEFT_CONTRACT_DIR")
	if dir == "" {
		common, err := git(start, "rev-parse", "--path-format=absolute", "--git-common-dir")
		if err != nil {
			return "", err
		}
		weft := filepath.Join(filepath.Dir(filepath.Dir(common)), "weft")
		if err := extractCommitted(weft, scratch); err != nil {
			return "", err
		}
		dir = scratch
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

type dirKey struct {
	t     testing.TB
	start string
}

var (
	dirsMu sync.Mutex
	dirs   = map[dirKey]string{}
)

// Dir is Resolve as a test helper, extracting into a t.TempDir() once per
// test: callers resolve on every fixture read, and terminal tests race a
// shell's output against reads between opening a session and attaching.
// Absent is a failure rather than a skip: these fixtures are the contract
// under test.
func Dir(t testing.TB, start string) string {
	t.Helper()
	dirsMu.Lock()
	defer dirsMu.Unlock()
	key := dirKey{t, start}
	if dir, ok := dirs[key]; ok {
		return dir
	}
	dir, err := Resolve(start, t.TempDir())
	if err != nil {
		t.Fatalf("weft-service/v2 fixtures are required (set WEFT_CONTRACT_DIR): %v", err)
	}
	dirs[key] = dir
	t.Cleanup(func() {
		dirsMu.Lock()
		delete(dirs, key)
		dirsMu.Unlock()
	})
	return dir
}

// extractCommitted writes HEAD:docs/service-v2 of the weft checkout into dst,
// from the object store alone so neither the working tree nor the index is
// read.
func extractCommitted(weft, dst string) error {
	out, err := gitOutput(weft, "archive", "--format=tar", "HEAD:docs/service-v2")
	if err != nil {
		return err
	}
	tr := tar.NewReader(bytes.NewReader(out))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read weft fixture archive: %w", err)
		}
		if !filepath.IsLocal(hdr.Name) {
			return fmt.Errorf("weft fixture archive entry %q escapes %s", hdr.Name, dst)
		}
		path := filepath.Join(dst, hdr.Name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return err
			}
			body, err := io.ReadAll(tr)
			if err != nil {
				return fmt.Errorf("read weft fixture %s: %w", hdr.Name, err)
			}
			if err := os.WriteFile(path, body, 0o644); err != nil {
				return err
			}
		case tar.TypeXGlobalHeader:
		default:
			return fmt.Errorf("weft fixture archive entry %q has unsupported type %q", hdr.Name, hdr.Typeflag)
		}
	}
}

func git(dir string, args ...string) (string, error) {
	out, err := gitOutput(dir, args...)
	return strings.TrimSpace(string(out)), err
}

func gitOutput(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = gitenv.Env()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s in %s: %w: %s", strings.Join(args, " "), dir, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
