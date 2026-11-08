package sandbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"shuttle/internal/weftfixtures"
)

// The worktree directory is the canonical checkout's repository_id, which
// weft's WorktreeProvider derives the same way; weft-47327fe5 is the id its
// contract gives /Users/steve/code/weft. WorktreePath resolves the repository
// first, so the derivation runs on a checkout that exists here, whose
// basename, like weft's, needs no restriction.
func TestWorktreePathMatchesWeftContract(t *testing.T) {
	if got := RepositoryID("/Users/steve/code/weft"); got != "weft-47327fe5" {
		t.Fatalf("RepositoryID(/Users/steve/code/weft) = %s, want weft-47327fe5", got)
	}
	repo := filepath.Join(tempRoot(t), "weft")
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := WorktreePath("/Users/steve/.shuttle/weft", repo, "weft/example-ticket-0a1b")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join("/Users/steve/.shuttle/weft/worktrees",
		RepositoryID(strings.TrimPrefix(repo, "/private")), "weft-example-ticket-0a1b")
	if got != want {
		t.Fatalf("worktree = %s, want %s", got, want)
	}
}

// contractDir is weft's docs/service-v2, resolved from this package's checkout.
func contractDir(t *testing.T) string {
	t.Helper()
	return weftfixtures.Dir(t, ".")
}

func TestRepositoryIDMatchesServiceFixtures(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join(contractDir(t), "projects.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Projects []struct {
			RepositoryID *string `json:"repository_id"`
			Repository   *string `json:"repository"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(doc, &fixture); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, p := range fixture.Projects {
		if p.Repository == nil {
			continue
		}
		if got := RepositoryID(*p.Repository); got != *p.RepositoryID {
			t.Errorf("RepositoryID(%s) = %s, want %s", *p.Repository, got, *p.RepositoryID)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("projects.json binds no repository")
	}
	long := "/repos/" + strings.Repeat("a", 60) + ".git checkout"
	got := RepositoryID(long)
	if len(got) != 64 || !strings.HasPrefix(got, strings.Repeat("a", 55)+"-") {
		t.Fatalf("RepositoryID(%q) = %q, want the basename cut to 55 bytes", long, got)
	}
	if got := RepositoryID("/tmp/my repo.v2"); !strings.HasPrefix(got, "my-repo-v2-") {
		t.Fatalf("RepositoryID kept bytes outside [A-Za-z0-9-]: %s", got)
	}
}

func TestCanonicalRepositoryCollapsesSymlinkAliases(t *testing.T) {
	root := tempRoot(t)
	repo := filepath.Join(root, "repo")
	alias := filepath.Join(root, "alias")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(repo, alias); err != nil {
		t.Fatal(err)
	}
	direct, err := CanonicalRepository(repo)
	if err != nil {
		t.Fatal(err)
	}
	throughAlias, err := CanonicalRepository(alias)
	if err != nil {
		t.Fatal(err)
	}
	if direct != throughAlias {
		t.Fatalf("canonical identities differ: %s != %s", direct, throughAlias)
	}
}

// Two spellings of one checkout on a case-insensitive volume are one
// repository: one canonical path, one repository_id, one reservation.
func TestCanonicalRepositoryCollapsesCaseAliases(t *testing.T) {
	root := tempRoot(t)
	repo := filepath.Join(root, "Weft")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "weft")
	if _, err := os.Stat(alias); err != nil {
		t.Skip("test volume is case-sensitive")
	}
	direct, err := CanonicalRepository(repo)
	if err != nil {
		t.Fatal(err)
	}
	throughAlias, err := CanonicalRepository(filepath.Join(strings.ToUpper(root), "WEFT"))
	if err != nil {
		t.Fatal(err)
	}
	lower, err := CanonicalRepository(alias)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.TrimPrefix(repo, "/private")
	if direct != want || throughAlias != want || lower != want {
		t.Fatalf("canonical paths %s, %s, %s; want the volume's spelling %s", direct, throughAlias, lower, want)
	}
	if RepositoryID(direct) != RepositoryID(lower) || RepositoryID(direct) != RepositoryID(throughAlias) {
		t.Fatalf("repository_ids differ: %s %s %s", RepositoryID(direct), RepositoryID(lower), RepositoryID(throughAlias))
	}
}

// A tree covers a path it reaches through a symlink, the Data-volume firmlink
// spelling, a case alias or a Unicode normalization alias, whether or not the
// aliased component exists yet; a sibling it does not.
func TestCoversSeesThroughAliases(t *testing.T) {
	root := tempRoot(t)
	weft := filepath.Join(root, "weft")
	link := filepath.Join(root, "link")
	if err := os.Mkdir(weft, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(weft, link); err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(link, "worktrees", "repo-deadbeef", "demo-ticket")
	covers := func(path, tree string) {
		t.Helper()
		if covered, err := Covers(tree, path); err != nil || !covered {
			t.Errorf("Covers(%s, %s) = %v, %v; want covered", tree, path, covered, err)
		}
	}
	misses := func(path, tree string) {
		t.Helper()
		if covered, err := Covers(tree, path); err != nil || covered {
			t.Errorf("Covers(%s, %s) = %v, %v; want not covered", tree, path, covered, err)
		}
	}
	trees := []string{
		worktree, filepath.Dir(worktree), filepath.Join(link, "worktrees"), filepath.Join(weft, "worktrees"),
	}
	if runtime.GOOS == "darwin" {
		trees = append(trees, filepath.Join(root, "WEFT", "worktrees"))
		// The fixture is on the Data volume, reachable through its firmlink
		// spelling; the tree is added only where that spelling is the same
		// directory, so it never degenerates into the literal case.
		data := filepath.Join("/System/Volumes/Data", weft)
		a, err := os.Stat(weft)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := os.Stat(data); err == nil && os.SameFile(a, b) {
			trees = append(trees, filepath.Join(data, "worktrees"))
		}
	}
	for _, tree := range trees {
		covers(worktree, tree)
	}
	misses(worktree, worktree+"-sibling")
	misses(worktree, filepath.Join(weft, "worktrees", "repo-deadbeef", "demo-ticket-sibling"))
	// Below an existing worktrees directory, components not created yet are
	// compared folded: a missing repository directory spelled in another case
	// or Unicode normalization is the same once created.
	worktrees := filepath.Join(root, "missing", "worktrees")
	if err := os.MkdirAll(worktrees, 0o755); err != nil {
		t.Fatal(err)
	}
	covers(filepath.Join(worktrees, "repo-deadbeef", "demo-ticket"), filepath.Join(worktrees, "REPO-deadbeef"))
	covers(filepath.Join(worktrees, "café", "demo-ticket"), filepath.Join(worktrees, "café"))
	misses(filepath.Join(worktrees, "repo-deadbeef", "demo-ticket"), filepath.Join(worktrees, "REPO-deadbeef-sibling"))
	// A tree that is a symlink to a directory not created yet cannot be
	// compared by identity, so it is refused.
	shared := filepath.Join(root, "shared")
	pending := filepath.Join(root, "pending", "worktrees")
	if err := os.Symlink(pending, shared); err != nil {
		t.Fatal(err)
	}
	if _, err := Covers(shared, filepath.Join(pending, "repo-deadbeef", "demo-ticket")); err == nil ||
		!strings.Contains(err.Error(), "dangling symlink") || !strings.Contains(err.Error(), shared) {
		t.Errorf("Covers(%s) error = %v, want a dangling symlink refusal naming it", shared, err)
	}
	if runtime.GOOS != "darwin" {
		return
	}
	// An existing worktrees parent spelled with a composed é in the path and a
	// decomposed é in the tree.
	composed := filepath.Join(root, "café", "worktrees")
	decomposed := filepath.Join(root, "café", "worktrees")
	if err := os.MkdirAll(composed, 0o755); err != nil {
		t.Fatal(err)
	}
	a, err := os.Stat(composed)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.Stat(decomposed); err != nil || !os.SameFile(a, b) {
		t.Skipf("test volume is normalization-sensitive: %v", err)
	}
	covers(filepath.Join(composed, "repo-deadbeef", "demo-ticket"), decomposed)
}

// tempRoot is t.TempDir() with its symlinks resolved: on macOS it sits under
// /var, an alias of /private/var.
func tempRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}
