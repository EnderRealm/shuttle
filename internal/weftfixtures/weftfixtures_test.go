package weftfixtures

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"shuttle/internal/gitenv"
)

// A hosted run tests shuttle in a linked worktree whose own siblings hold no
// weft; the fixtures resolve beside the main checkout it was added from.
func TestContractDirResolvesFromLinkedWorktree(t *testing.T) {
	t.Setenv("WEFT_CONTRACT_DIR", "")
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "shuttle")
	want := filepath.Join(tmp, "weft", "docs", "service-v2")
	if err := os.MkdirAll(want, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(want, "manifest.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	gitRun(t, repo, "init", "-q")
	gitRun(t, repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init")
	worktree := filepath.Join(tmp, "worktrees", "shuttle-1", "shuttle")
	gitRun(t, repo, "worktree", "add", "-q", "--detach", worktree)
	pkg := filepath.Join(worktree, "internal", "weftremote")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatal(err)
	}

	got := Dir(t, pkg)
	if evalSymlinks(t, got) != evalSymlinks(t, want) {
		t.Fatalf("contract dir = %s, want %s", got, want)
	}

	if err := os.Remove(filepath.Join(want, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(pkg); err == nil {
		t.Fatal("resolved with no manifest.json, want an error")
	}
}

func evalSymlinks(t *testing.T, path string) string {
	t.Helper()
	out, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = gitenv.Env()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}
