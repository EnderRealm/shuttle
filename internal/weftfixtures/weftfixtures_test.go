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
	repo := newRepo(t, filepath.Join(tmp, "shuttle"))
	weft := newRepo(t, filepath.Join(tmp, "weft"))
	writeFile(t, filepath.Join(weft, "docs", "service-v2", "manifest.json"), `{"committed":true}`)
	writeFile(t, filepath.Join(weft, "docs", "service-v2", "projects.json"), "{}")
	gitRun(t, weft, "add", "-A")
	commit(t, weft, "fixtures")
	worktree := filepath.Join(tmp, "worktrees", "shuttle-1", "shuttle")
	gitRun(t, repo, "worktree", "add", "-q", "--detach", worktree)
	pkg := filepath.Join(worktree, "internal", "weftremote")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatal(err)
	}

	if got := readFile(t, filepath.Join(Dir(t, pkg), "manifest.json")); got != `{"committed":true}` {
		t.Fatalf("manifest.json = %s, want weft's committed manifest", got)
	}

	gitRun(t, weft, "rm", "-q", filepath.Join("docs", "service-v2", "manifest.json"))
	commit(t, weft, "drop manifest")
	if _, err := Resolve(pkg, t.TempDir()); err == nil {
		t.Fatal("resolved with no manifest.json, want an error")
	}
}

// Weft's contract work in flight must not move shuttle's verdict: an edited,
// staged or untracked fixture in the weft checkout is not what the tests read.
func TestContractDirReadsCommittedFixtures(t *testing.T) {
	t.Setenv("WEFT_CONTRACT_DIR", "")
	tmp := t.TempDir()
	repo := newRepo(t, filepath.Join(tmp, "shuttle"))
	weft := newRepo(t, filepath.Join(tmp, "weft"))
	fixtures := filepath.Join(weft, "docs", "service-v2")
	writeFile(t, filepath.Join(fixtures, "manifest.json"), "{}")
	writeFile(t, filepath.Join(fixtures, "ticket.json"), `{"id":"committed"}`)
	writeFile(t, filepath.Join(fixtures, "negative", "bad.json"), `{"bad":"committed"}`)
	gitRun(t, weft, "add", "-A")
	commit(t, weft, "fixtures")

	writeFile(t, filepath.Join(fixtures, "ticket.json"), `{"id":"committed","criteria_count":2}`)
	writeFile(t, filepath.Join(fixtures, "negative", "bad.json"), `{"bad":"staged"}`)
	gitRun(t, weft, "add", filepath.Join("docs", "service-v2", "negative", "bad.json"))
	writeFile(t, filepath.Join(fixtures, "untracked.json"), "{}")

	dir := Dir(t, repo)
	if evalSymlinks(t, dir) == evalSymlinks(t, fixtures) {
		t.Fatalf("contract dir = %s, the weft working tree", dir)
	}
	if got := readFile(t, filepath.Join(dir, "ticket.json")); got != `{"id":"committed"}` {
		t.Fatalf("ticket.json = %s, want the committed fixture", got)
	}
	if got := readFile(t, filepath.Join(dir, "negative", "bad.json")); got != `{"bad":"committed"}` {
		t.Fatalf("negative/bad.json = %s, want the committed fixture", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "untracked.json")); !os.IsNotExist(err) {
		t.Fatalf("untracked.json in contract dir: %v", err)
	}
}

// WEFT_CONTRACT_DIR names live fixtures, so a contract change is testable
// before weft commits it.
func TestContractDirOverrideReadsLiveFixtures(t *testing.T) {
	tmp := t.TempDir()
	repo := newRepo(t, filepath.Join(tmp, "shuttle"))
	weft := newRepo(t, filepath.Join(tmp, "weft"))
	writeFile(t, filepath.Join(weft, "docs", "service-v2", "manifest.json"), "{}")
	writeFile(t, filepath.Join(weft, "docs", "service-v2", "ticket.json"), `{"id":"committed"}`)
	gitRun(t, weft, "add", "-A")
	commit(t, weft, "fixtures")

	live := filepath.Join(repo, "contract")
	writeFile(t, filepath.Join(live, "manifest.json"), "{}")
	writeFile(t, filepath.Join(live, "ticket.json"), `{"id":"uncommitted"}`)

	for name, value := range map[string]string{"absolute": live, "relative": "contract"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("WEFT_CONTRACT_DIR", value)
			dir := Dir(t, repo)
			if evalSymlinks(t, dir) != evalSymlinks(t, live) {
				t.Fatalf("contract dir = %s, want %s", dir, live)
			}
			if got := readFile(t, filepath.Join(dir, "ticket.json")); got != `{"id":"uncommitted"}` {
				t.Fatalf("ticket.json = %s, want the live fixture", got)
			}
		})
	}
}

func newRepo(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "init", "-q")
	commit(t, dir, "init")
	return dir
}

func commit(t *testing.T, dir, message string) {
	t.Helper()
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", message)
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
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
