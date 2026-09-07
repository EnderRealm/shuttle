package gitlog

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"shuttle/internal/provider"
)

func git(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

// newRepo builds a checkout with one commit at each of the given instants.
func newRepo(t *testing.T, at ...time.Time) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, nil, "init", "--initial-branch=main")
	for i, when := range at {
		name := filepath.Join(dir, "file"+string(rune('a'+i))+".txt")
		if err := os.WriteFile(name, []byte("one\ntwo\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		git(t, dir, nil, "add", ".")
		stamp := when.Format(time.RFC3339)
		git(t, dir, []string{"GIT_AUTHOR_DATE=" + stamp, "GIT_COMMITTER_DATE=" + stamp},
			"-c", "user.name=Test", "-c", "user.email=test@example.com",
			"commit", "-m", "[shuttle/x] commit "+strconv.Itoa(i))
	}
	return dir
}

// tkHome writes a config.yaml registering each named path as a project.
func tkHome(t *testing.T, paths map[string]string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), ".ticket")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "central_root: " + filepath.Join(dir, "store") + "\nprojects:\n"
	for name, path := range paths {
		body += "    " + name + ":\n        path: " + path + "\n"
	}
	config := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(config, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return config
}

func testWindow(t *testing.T) provider.Window {
	t.Helper()
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	w, err := provider.Resolve(provider.WindowPreviousDay, time.Date(2026, 9, 2, 4, 30, 0, 0, loc), loc)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func run(t *testing.T, env provider.Env) (document, []byte) {
	t.Helper()
	env.Logger = log.New(&bytes.Buffer{}, "", 0)
	data, err := Run(context.Background(), env, nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return doc, data
}

func TestCommitsInTheWindow(t *testing.T) {
	w := testWindow(t)
	repo := newRepo(t, w.From.Add(-24*time.Hour), w.From.Add(9*time.Hour))
	doc, _ := run(t, provider.Env{TKConfig: tkHome(t, map[string]string{"shuttle": repo})})
	if len(doc.Repos) != 1 || len(doc.Repos[0].Commits) != 1 {
		t.Fatalf("repos = %+v, want the one commit in the window", doc.Repos)
	}
	c := doc.Repos[0].Commits[0]
	if c.Subject != "[shuttle/x] commit 1" {
		t.Errorf("subject = %q, want the in-window commit's", c.Subject)
	}
	if c.Hash == "" || c.Date == "" {
		t.Errorf("commit = %+v, want a hash and a date", c)
	}
	if c.FilesChanged != 1 || c.Insertions != 2 || c.Deletions != 0 {
		t.Errorf("shortstat = %+v, want one file and two insertions", c)
	}
	if len(c.Branches) != 1 || c.Branches[0] != "main" {
		t.Errorf("branches = %v, want main with the HEAD arrow stripped", c.Branches)
	}
}

// A commit at the window's To belongs to the next day: git's --until is
// inclusive, the window is not.
func TestEmptyWindowEmitsAnEmptyDocument(t *testing.T) {
	w := testWindow(t)
	repo := newRepo(t, w.To)
	doc, data := run(t, provider.Env{TKConfig: tkHome(t, map[string]string{"shuttle": repo})})
	if len(doc.Repos) != 1 || len(doc.Repos[0].Commits) != 0 {
		t.Fatalf("repos = %+v, want the repo with no commits", doc.Repos)
	}
	if !strings.Contains(string(data), `"commits":[]`) || !strings.Contains(string(data), `"skipped":[]`) {
		t.Fatalf("data = %s, want empty arrays rather than null", data)
	}
}

// config.yaml lists checkouts from other machines, and one broken checkout must
// not take the run down.
func TestUnusablePathsAreSkipped(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	notARepo := t.TempDir()
	w := testWindow(t)
	repo := newRepo(t, w.From.Add(time.Hour))
	doc, _ := run(t, provider.Env{TKConfig: tkHome(t, map[string]string{
		"shuttle": repo, "elsewhere": missing, "loose": notARepo,
	})})
	if len(doc.Repos) != 1 || doc.Repos[0].Name != "shuttle" {
		t.Fatalf("repos = %+v, want only the usable checkout", doc.Repos)
	}
	if len(doc.Skipped) != 2 {
		t.Fatalf("skipped = %+v, want the missing path and the non-repository", doc.Skipped)
	}
	for _, s := range doc.Skipped {
		if s.Reason == "" {
			t.Errorf("skip %+v: no reason", s)
		}
	}
}

// The reports repo (§9.3) joins the estate once it exists.
func TestTheReportsRepoIsIncludedWhenItExists(t *testing.T) {
	w := testWindow(t)
	reports := newRepo(t, w.From.Add(2*time.Hour))
	env := provider.Env{TKConfig: tkHome(t, map[string]string{}), ReportsDir: reports}
	doc, _ := run(t, env)
	if len(doc.Repos) != 1 || doc.Repos[0].Name != reportsName {
		t.Fatalf("repos = %+v, want the reports repo", doc.Repos)
	}

	env.ReportsDir = filepath.Join(t.TempDir(), "reports")
	doc, _ = run(t, env)
	if len(doc.Repos) != 0 || len(doc.Skipped) != 0 {
		t.Fatalf("document = %+v, want a reports repo that does not exist yet left out entirely", doc)
	}
}
