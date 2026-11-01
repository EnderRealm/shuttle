package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"shuttle/internal/weftfixtures"
)

func TestWorktreePathMatchesWeftContract(t *testing.T) {
	got, err := WorktreePath(
		"/Users/steve/.shuttle/weft",
		"/Users/steve/code/weft",
		"weft/example-ticket-0a1b",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "/Users/steve/.shuttle/weft/worktrees/weft-47327fe5/weft-example-ticket-0a1b"
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
	long := "/tmp/" + strings.Repeat("a", 60) + ".git checkout"
	got := RepositoryID(long)
	if len(got) != 64 || !strings.HasPrefix(got, strings.Repeat("a", 55)+"-") {
		t.Fatalf("RepositoryID(%q) = %q, want the basename cut to 55 bytes", long, got)
	}
	if got := RepositoryID("/tmp/my repo.v2"); !strings.HasPrefix(got, "my-repo-v2-") {
		t.Fatalf("RepositoryID kept bytes outside [A-Za-z0-9-]: %s", got)
	}
}

func TestCanonicalRepositoryCollapsesSymlinkAliases(t *testing.T) {
	root := shortCodingRoot(t)
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

func TestRepositoryIdentityCollapsesCaseAliases(t *testing.T) {
	root := shortCodingRoot(t)
	repo := filepath.Join(root, "MixedCase")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "mixedcase")
	if _, err := os.Stat(alias); err != nil {
		t.Skip("test volume is case-sensitive")
	}
	direct, err := RepositoryIdentity(repo)
	if err != nil {
		t.Fatal(err)
	}
	throughAlias, err := RepositoryIdentity(alias)
	if err != nil {
		t.Fatal(err)
	}
	if direct != throughAlias {
		t.Fatalf("filesystem identities differ: %s != %s", direct, throughAlias)
	}
}

// Two spellings of one checkout on a case-insensitive volume are one
// repository: one canonical path, one repository_id, one reservation.
func TestCanonicalRepositoryCollapsesCaseAliases(t *testing.T) {
	root := shortCodingRoot(t)
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

func TestCodingProfileProtectsGitIndirectionsAndSiblingWorktrees(t *testing.T) {
	root := shortCodingRoot(t)
	repo := filepath.Join(root, "repo")
	worktree := filepath.Join(root, "weft", "worktrees", "repo-deadbeef", "demo-ticket")
	runDir := filepath.Join(root, "run")
	tickets := filepath.Join(root, "tickets")
	admin := filepath.Join(repo, ".git", "worktrees")
	for _, dir := range []string{
		filepath.Join(admin, "demo-ticket"), filepath.Join(admin, "other-ticket"),
		worktree, runDir, tickets,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(worktree, ".git"), []byte("gitdir: "+filepath.Join(admin, "demo-ticket")), 0o644); err != nil {
		t.Fatal(err)
	}
	o := CodingOptions{
		SRTBin: "/usr/local/bin/srt", RunDir: runDir, HomeDir: root, PATH: testPATH,
		Repo: repo, TicketsRoot: tickets, Worktree: worktree,
		UserTempDir:    filepath.Join(root, "user-temp"),
		AllowedDomains: []string{"github.com"},
		AllowWrite:     []string{filepath.Join(root, "swift-cache")},
	}
	doc, err := CodingProfileJSON(o)
	if err != nil {
		t.Fatal(err)
	}
	var got profile
	if err := json.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		filepath.Join(worktree, ".git"),
		filepath.Join(repo, ".git", "config"),
		filepath.Join(repo, ".git", "hooks"),
		filepath.Join(admin, "demo-ticket", "commondir"),
		filepath.Join(admin, "demo-ticket", "gitdir"),
		filepath.Join(admin, "demo-ticket", "config.worktree"),
		filepath.Join(admin, "other-ticket"),
		filepath.Join(tickets, ".git"),
		filepath.Join(root, ".claude", "commands"),
		filepath.Join(root, ".claude", "plugins"),
		filepath.Join(root, ".claude", "rules"),
		filepath.Join(root, ".claude", "projects", "*", "memory"),
		filepath.Join(root, ".claude.json"),
		filepath.Join(root, ".claude", "work-policy"),
	} {
		if !slices.Contains(got.Filesystem.DenyWrite, want) {
			t.Errorf("denyWrite does not contain %s", want)
		}
	}
	for _, want := range []string{worktree, filepath.Join(repo, ".git"), tickets, filepath.Join(root, "user-temp"), filepath.Join(root, "swift-cache")} {
		if !slices.Contains(got.Filesystem.AllowWrite, want) {
			t.Errorf("allowWrite does not contain %s", want)
		}
	}
	if !slices.Contains(got.Filesystem.DenyRead, filepath.Join(root, "Library", "Keychains")) {
		t.Errorf("denyRead does not contain %s", filepath.Join(root, "Library", "Keychains"))
	}
	if !slices.Equal(got.Network.AllowedDomains, []string{APIHost, platformHost, "github.com"}) {
		t.Fatalf("allowed domains = %v", got.Network.AllowedDomains)
	}
}

func TestUserTempDirIsResolvedAndClean(t *testing.T) {
	dir, err := UserTempDir()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(dir) || dir != filepath.Clean(dir) {
		t.Fatalf("user temp dir %q is not a clean absolute path", dir)
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != dir {
		t.Fatalf("user temp dir %q resolves to %q", dir, resolved)
	}
}

func TestCodingProfileRefusesMissingUserTempDir(t *testing.T) {
	root := shortCodingRoot(t)
	_, err := CodingProfileJSON(CodingOptions{
		SRTBin: "/usr/local/bin/srt", RunDir: filepath.Join(root, "run"),
		HomeDir: root, PATH: testPATH, Repo: filepath.Join(root, "repo"),
		TicketsRoot: filepath.Join(root, "tickets"), Worktree: filepath.Join(root, "worktree"),
	})
	if err == nil || !strings.Contains(err.Error(), "user temp dir") {
		t.Fatalf("profile error = %v, want user temp dir refusal", err)
	}
}

// The run directory is the control the host's containment sweep tells the
// segment's processes apart by, so no allow_write entry may grant it.
func TestCodingProfileRefusesAllowWriteCoveringRunDir(t *testing.T) {
	root := shortCodingRoot(t)
	runDir := filepath.Join(root, "runs", "weft", "segment")
	options := func(allow string) CodingOptions {
		return CodingOptions{
			SRTBin: "/usr/local/bin/srt", RunDir: runDir, HomeDir: root, PATH: testPATH,
			Repo: filepath.Join(root, "repo"), TicketsRoot: filepath.Join(root, "tickets"),
			Worktree: filepath.Join(root, "worktree"), UserTempDir: filepath.Join(root, "user-temp"),
			AllowWrite: []string{allow},
		}
	}
	for _, allow := range []string{runDir, filepath.Dir(runDir), root, "/"} {
		if _, err := CodingProfileJSON(options(allow)); err == nil || !strings.Contains(err.Error(), "covers run dir") {
			t.Errorf("allow_write %s: profile error = %v, want a run dir refusal", allow, err)
		}
	}
	if _, err := CodingProfileJSON(options(runDir + "-sibling")); err != nil {
		t.Fatalf("allow_write sibling of the run dir: %v", err)
	}
}

// allow_write is shared by every segment's profile, so an entry covering one
// segment's worktree would let another segment's processes write it and be
// swept as its holders. Seatbelt judges the real path, so an entry covering
// the worktree through a symlink, /tmp alias, case alias or Unicode
// normalization alias is refused too, whether or not the aliased component
// exists yet.
func TestCodingProfileRefusesAllowWriteCoveringWorktree(t *testing.T) {
	root := shortCodingRoot(t)
	weft := filepath.Join(root, "weft")
	link := filepath.Join(root, "link")
	if err := os.Mkdir(weft, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(weft, link); err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(link, "worktrees", "repo-deadbeef", "demo-ticket")
	options := func(worktree, allow string) CodingOptions {
		return CodingOptions{
			SRTBin: "/usr/local/bin/srt", RunDir: filepath.Join(root, "run"), HomeDir: root, PATH: testPATH,
			Repo: filepath.Join(root, "repo"), TicketsRoot: filepath.Join(root, "tickets"),
			Worktree: worktree, UserTempDir: filepath.Join(root, "user-temp"),
			AllowWrite: []string{allow},
		}
	}
	refuses := func(worktree, allow string) {
		t.Helper()
		_, err := CodingProfileJSON(options(worktree, allow))
		if err == nil || !strings.Contains(err.Error(), "covers worktree") || !strings.Contains(err.Error(), strconv.Quote(allow)) {
			t.Errorf("allow_write %s: profile error = %v, want a worktree refusal naming the entry", allow, err)
		}
	}
	refused := []string{
		worktree, filepath.Dir(worktree), filepath.Join(link, "worktrees"), filepath.Join(weft, "worktrees"),
	}
	if runtime.GOOS == "darwin" {
		refused = append(refused,
			filepath.Join(strings.TrimPrefix(root, "/private"), "weft", "worktrees"),
			filepath.Join(root, "WEFT", "worktrees"))
	}
	for _, allow := range refused {
		refuses(worktree, allow)
	}
	for _, allow := range []string{worktree + "-sibling", filepath.Join(weft, "worktrees", "repo-deadbeef", "demo-ticket-sibling")} {
		if _, err := CodingProfileJSON(options(worktree, allow)); err != nil {
			t.Fatalf("allow_write sibling of the worktree %s: %v", allow, err)
		}
	}
	// Below an existing worktrees directory, components the worker has not
	// created yet are compared folded: a missing repository directory spelled
	// in another case or Unicode normalization is the same once created.
	worktrees := filepath.Join(root, "missing", "worktrees")
	if err := os.MkdirAll(worktrees, 0o755); err != nil {
		t.Fatal(err)
	}
	refuses(filepath.Join(worktrees, "repo-deadbeef", "demo-ticket"), filepath.Join(worktrees, "REPO-deadbeef"))
	refuses(filepath.Join(worktrees, "caf\u00e9", "demo-ticket"), filepath.Join(worktrees, "cafe\u0301"))
	if _, err := CodingProfileJSON(options(filepath.Join(worktrees, "repo-deadbeef", "demo-ticket"), filepath.Join(worktrees, "REPO-deadbeef-sibling"))); err != nil {
		t.Fatalf("allow_write sibling of the missing repository directory: %v", err)
	}
	// An entry that is a symlink to a worktrees parent not created yet cannot
	// be compared by identity, so it is refused.
	shared := filepath.Join(root, "shared")
	pending := filepath.Join(root, "pending", "worktrees")
	if err := os.Symlink(pending, shared); err != nil {
		t.Fatal(err)
	}
	_, err := CodingProfileJSON(options(filepath.Join(pending, "repo-deadbeef", "demo-ticket"), shared))
	if err == nil || !strings.Contains(err.Error(), "dangling symlink") || !strings.Contains(err.Error(), strconv.Quote(shared)) {
		t.Errorf("allow_write %s: profile error = %v, want a dangling symlink refusal naming the entry", shared, err)
	}
	if runtime.GOOS != "darwin" {
		return
	}
	// An existing worktrees parent spelled with a composed é in the worktree
	// and a decomposed é in the entry.
	composed := filepath.Join(root, "caf\u00e9", "worktrees")
	decomposed := filepath.Join(root, "cafe\u0301", "worktrees")
	if err := os.MkdirAll(composed, 0o755); err != nil {
		t.Fatal(err)
	}
	a, err := os.Stat(composed)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.Stat(decomposed); err != nil || !os.SameFile(a, b) {
		t.Skipf("temp volume is normalization-sensitive: %v", err)
	}
	refuses(filepath.Join(composed, "repo-deadbeef", "demo-ticket"), decomposed)
}

func TestCheckCodingRefusesMissingSandbox(t *testing.T) {
	root := shortCodingRoot(t)
	o := CodingOptions{
		SRTBin: filepath.Join(root, "missing-srt"), RunDir: filepath.Join(root, "run"),
		HomeDir: root, PATH: testPATH, Repo: filepath.Join(root, "repo"),
		TicketsRoot: filepath.Join(root, "tickets"), Worktree: filepath.Join(root, "worktree"),
		UserTempDir: filepath.Join(root, "user-temp"),
	}
	admin := filepath.Join(o.Repo, ".git", "worktrees", filepath.Base(o.Worktree))
	for _, dir := range []string{o.RunDir, o.Repo, o.TicketsRoot, o.Worktree, admin} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(o.Worktree, ".git"), []byte("gitdir: "+admin), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteCodingProfile(o); err != nil {
		t.Fatal(err)
	}
	if err := CheckCoding(context.Background(), o); err == nil {
		t.Fatal("missing srt established a coding sandbox")
	}
}

// checkableCoding is a coding profile's directories laid out under root, the
// run directory at runDir, ready for WriteCodingProfile and CheckCoding.
func checkableCoding(t *testing.T, root, runDir, srt string, allowWrite ...string) CodingOptions {
	t.Helper()
	o := CodingOptions{
		SRTBin: srt, RunDir: runDir, HomeDir: filepath.Join(root, "home"), PATH: testPATH,
		Repo: filepath.Join(root, "repo"), TicketsRoot: filepath.Join(root, "tickets"),
		Worktree: filepath.Join(root, "worktree"), UserTempDir: filepath.Join(root, "user-temp"),
		AllowWrite: allowWrite,
	}
	admin := filepath.Join(o.Repo, ".git", "worktrees", filepath.Base(o.Worktree))
	for _, dir := range []string{o.RunDir, o.HomeDir, o.TicketsRoot, o.Worktree, o.UserTempDir, admin} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(o.Worktree, ".git"), []byte("gitdir: "+admin), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteCodingProfile(o); err != nil {
		t.Fatal(err)
	}
	return o
}

// The preflight has the sandbox itself try the run directory: a profile that
// lets the segment write it is refused, whatever the configured strings say.
// The stand-in srt runs the probe unconfined, so the write lands.
func TestCheckCodingRefusesASandboxThatGrantsTheRunDir(t *testing.T) {
	root := shortCodingRoot(t)
	srt := filepath.Join(root, "srt")
	if err := os.WriteFile(srt, []byte("#!/bin/sh\nexec /bin/sh -c \"$4\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	o := checkableCoding(t, root, filepath.Join(root, "run"), srt)
	if err := CheckCoding(context.Background(), o); err == nil || !strings.Contains(err.Error(), "run dir") {
		t.Fatalf("check = %v, want the run dir refusal", err)
	}
	if _, err := os.Lstat(filepath.Join(o.RunDir, controlProbe)); !os.IsNotExist(err) {
		t.Fatalf("control probe left behind: %v", err)
	}
}

// An allow_write entry spelled through /private/tmp covers a run directory
// spelled through /tmp: the strings differ, but seatbelt judges the real
// path, so real srt grants the run directory and the preflight refuses it.
func TestLiveCodingSRTRefusesAnAliasedRunDirGrant(t *testing.T) {
	if os.Getenv("SHUTTLE_LIVE_SRT") != "1" {
		t.Skip("set SHUTTLE_LIVE_SRT=1 to run the coding profile against real srt")
	}
	bin := os.Getenv("SHUTTLE_SRT_BIN")
	if bin == "" {
		bin = defaultSRTBin
	}
	root := shortCodingRoot(t)
	if !strings.HasPrefix(root, "/private/tmp/") {
		t.Skipf("temp root %s is not under /private/tmp", root)
	}
	aliased := strings.TrimPrefix(root, "/private")
	denied := checkableCoding(t, root, filepath.Join(aliased, "run-denied"), bin)
	if err := CheckCoding(context.Background(), denied); err != nil {
		t.Fatalf("a profile denying the run dir was refused: %v", err)
	}
	o := checkableCoding(t, root, filepath.Join(aliased, "run"), bin, root)
	if err := CheckCoding(context.Background(), o); err == nil || !strings.Contains(err.Error(), "run dir") {
		t.Fatalf("check = %v, want the run dir refusal", err)
	}
}

func TestCodingProfileRefusesOccupiedExpectedAdminName(t *testing.T) {
	root := shortCodingRoot(t)
	repo := filepath.Join(root, "repo")
	worktree := filepath.Join(root, "worktrees", "demo-ticket")
	expected := filepath.Join(repo, ".git", "worktrees", "demo-ticket")
	for _, dir := range []string{expected, filepath.Join(root, "run"), filepath.Join(root, "tickets")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_, err := CodingProfileJSON(CodingOptions{
		SRTBin: "/usr/local/bin/srt", RunDir: filepath.Join(root, "run"),
		HomeDir: root, PATH: testPATH, Repo: repo,
		TicketsRoot: filepath.Join(root, "tickets"), Worktree: worktree,
		UserTempDir: filepath.Join(root, "user-temp"),
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("profile error = %v, want occupied admin refusal", err)
	}
}

func shortCodingRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "cw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestLiveCodingSRT(t *testing.T) {
	if os.Getenv("SHUTTLE_LIVE_SRT") != "1" {
		t.Skip("set SHUTTLE_LIVE_SRT=1 to run the coding profile against real srt")
	}
	bin := os.Getenv("SHUTTLE_SRT_BIN")
	if bin == "" {
		bin = defaultSRTBin
	}
	root := shortRunDir(t)
	repo := filepath.Join(root, "repo")
	worktree := filepath.Join(root, "worktree")
	otherWorktree := filepath.Join(root, "other-worktree")
	tickets := filepath.Join(root, "tickets")
	runDir := filepath.Join(root, "segment")
	for _, dir := range []string{repo, tickets, runDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	gitRun(t, repo, "init")
	if err := os.WriteFile(filepath.Join(repo, "base.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, repo, "add", "base.txt")
	gitRun(t, repo, "-c", "user.name=Shuttle Test", "-c", "user.email=shuttle@example.test", "commit", "-m", "base")
	gitRun(t, repo, "worktree", "add", "-b", "ticket", worktree)
	gitRun(t, repo, "worktree", "add", "-b", "other", otherWorktree)
	gitRun(t, tickets, "init")
	if err := os.WriteFile(filepath.Join(tickets, "base.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, tickets, "add", "base.txt")
	gitRun(t, tickets, "-c", "user.name=Shuttle Test", "-c", "user.email=shuttle@example.test", "commit", "-m", "base")
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	userTemp, err := UserTempDir()
	if err != nil {
		t.Fatal(err)
	}
	o := CodingOptions{
		SRTBin: bin, RunDir: runDir, HomeDir: home, PATH: testPATH,
		Repo: repo, TicketsRoot: tickets, Worktree: worktree, UserTempDir: userTemp,
	}
	if _, err := WriteCodingProfile(o); err != nil {
		t.Fatal(err)
	}
	if err := CheckCoding(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if code, stderr := codingExec(o, "touch "+filepath.Join(worktree, "allowed")); code != 0 {
		t.Fatalf("declared worktree write failed: %s", stderr)
	}
	userTempFile := filepath.Join(userTemp, "shuttle-live-"+filepath.Base(root))
	t.Cleanup(func() { os.Remove(userTempFile) })
	if code, stderr := codingExec(o, "touch "+userTempFile+" && rm "+userTempFile); code != 0 {
		t.Fatalf("user temp dir write failed: %s", stderr)
	}
	userTempSibling := filepath.Join(filepath.Dir(userTemp), "shuttle-live-"+filepath.Base(root))
	t.Cleanup(func() { os.Remove(userTempSibling) })
	if code, stderr := codingExec(o, "touch "+userTempSibling); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("user temp dir sibling write returned %d: %s", code, stderr)
	}
	commit := "printf committed > " + filepath.Join(worktree, "committed.txt") +
		" && git -C " + worktree + " add committed.txt" +
		" && git -C " + worktree + " -c user.name=Shuttle -c user.email=shuttle@example.test commit -m committed"
	if code, stderr := codingExec(o, commit); code != 0 {
		t.Fatalf("worktree commit failed: %s", stderr)
	}
	if code, stderr := codingExec(o, "touch "+filepath.Join(repo, ".git", "hooks", "pre-commit")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("out-of-policy hook write returned %d: %s", code, stderr)
	}
	ownAdmin := filepath.Join(repo, ".git", "worktrees", filepath.Base(worktree))
	if code, stderr := codingExec(o, "printf alt > "+filepath.Join(ownAdmin, "commondir")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("worktree admin redirect returned %d: %s", code, stderr)
	}
	otherAdmin := filepath.Join(repo, ".git", "worktrees", filepath.Base(otherWorktree))
	if code, stderr := codingExec(o, "printf alt > "+filepath.Join(otherAdmin, "commondir")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("sibling admin redirect returned %d: %s", code, stderr)
	}
	if code, stderr := codingExec(o, "touch "+filepath.Join(tickets, "ticket.md")); code != 0 {
		t.Fatalf("ticket write failed: %s", stderr)
	}
	if code, stderr := codingExec(o, "touch "+filepath.Join(tickets, ".git", "hooks", "pre-push")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("ticket-store hook write returned %d: %s", code, stderr)
	}
	if code, stderr := codingExec(o, "mkdir -p "+filepath.Join(home, ".claude", "commands")+
		" && touch "+filepath.Join(home, ".claude", "commands", "work.md")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("global command write returned %d: %s", code, stderr)
	}
	if code, stderr := codingExec(o, "mkdir -p "+filepath.Join(home, ".claude", "rules")+
		" && touch "+filepath.Join(home, ".claude", "rules", "persistent.md")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("global rule write returned %d: %s", code, stderr)
	}
	if code, stderr := codingExec(o, "printf malicious > "+filepath.Join(home, ".claude.json")); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("global Claude configuration write returned %d: %s", code, stderr)
	}
	memory := filepath.Join(home, ".claude", "projects", "other-project", "memory", "MEMORY.md")
	if code, stderr := codingExec(o, "mkdir -p "+filepath.Dir(memory)+" && touch "+memory); code == 0 || !strings.Contains(stderr, "Operation not permitted") {
		t.Fatalf("auto-memory write returned %d: %s", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(worktree, "committed.txt")); err != nil {
		t.Fatalf("committed worktree file: %v", err)
	}
}

func codingExec(o CodingOptions, script string) (int, string) {
	argv := []string{o.SRTBin, "-s", o.ProfilePath(), "--", "/bin/sh", "-c", script}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = o.RunDir
	cmd.Env = []string{"PATH=" + o.PATH, "HOME=" + o.HomeDir, "TMPDIR=" + o.tmpDir(), "CLAUDE_CODE_TMPDIR=" + o.tmpDir()}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), stderr.String()
	}
	if err != nil {
		return -1, err.Error()
	}
	return 0, stderr.String()
}

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}
