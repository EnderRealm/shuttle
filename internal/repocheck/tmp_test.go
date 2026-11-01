// Package repocheck holds checks over the module's source as a whole. It is
// test-only and its own package so that `go test ./...` runs each check once.
package repocheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tmpRootHelpers are the only functions allowed a literal /tmp fixture root,
// keyed by file relative to the module root; each says why where it does it.
// All but one root a run directory whose srt mux socket must fit the 103-byte
// unix socket path limit, which macOS's os.TempDir() (49–57 bytes) leaves no
// room for. internal/reap's hostTmpRoot roots a run directory that must sit
// outside the per-user temp dir a containment decoy is granted.
var tmpRootHelpers = map[string]string{
	"internal/reap/contain_darwin_test.go":   "hostTmpRoot",
	"cmd/shuttle/cmd/observer_test.go":       "newObserverEnv",
	"internal/runner/preflight_test.go":      "preflightFixture",
	"internal/runner/live_test.go":           "newLiveFixture",
	"internal/weftremote/queue_test.go":      "newQueueHost",
	"internal/weftremote/supervisor_test.go": "newSupervisorFixture",
	"internal/sandbox/sandbox_test.go":       "shortRunDir",
	"internal/sandbox/coding_test.go":        "shortCodingRoot",
}

// A test that roots its fixtures at a literal /tmp ignores TMPDIR, and a
// hosted run's sandbox grants the TMPDIR it sets rather than /tmp, so the
// suite cannot run there. Fixture roots come from t.TempDir() or
// os.TempDir(), outside tmpRootHelpers. The needles are
// concatenated so this file does not match itself.
func TestNoTestHardcodesTmp(t *testing.T) {
	const tmp = "/tmp"
	needles := []string{`MkdirTemp("` + tmp + `"`, `"` + tmp + `/"`, `(t, "` + tmp + `"`}
	root := filepath.Join("..", "..")
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("%s is not the module root: %v", root, err)
	}
	if module, _, _ := strings.Cut(string(mod), "\n"); module != "module shuttle" {
		t.Fatalf("%s is not the module root: its go.mod starts %q", root, module)
	}
	scanned := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// The directories the go command ignores hold no test it runs.
		if name := entry.Name(); entry.IsDir() && path != root &&
			(strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "vendor") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		var file *ast.File
		fset := token.NewFileSet()
		for i, line := range strings.Split(string(src), "\n") {
			for _, needle := range needles {
				if !strings.Contains(line, needle) {
					continue
				}
				if file == nil {
					if file, err = parser.ParseFile(fset, path, src, parser.SkipObjectResolution); err != nil {
						return err
					}
				}
				fn := enclosingFunc(fset, file, i+1)
				if allowed, ok := tmpRootHelpers[rel]; ok && fn == allowed {
					continue
				}
				t.Errorf("%s:%d: %s in %s roots a fixture at a literal /tmp, which ignores TMPDIR; use t.TempDir() or os.TempDir()",
					rel, i+1, needle, fn)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("scanned no _test.go files")
	}
}

// enclosingFunc names the top-level function declared around line, or
// "package scope" when there is none.
func enclosingFunc(fset *token.FileSet, file *ast.File, line int) string {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fset.Position(fn.Pos()).Line <= line && line <= fset.Position(fn.End()).Line {
			return fn.Name.Name
		}
	}
	return "package scope"
}
