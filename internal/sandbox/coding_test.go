package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
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
	for _, want := range []string{worktree, filepath.Join(repo, ".git"), tickets, filepath.Join(root, "swift-cache")} {
		if !slices.Contains(got.Filesystem.AllowWrite, want) {
			t.Errorf("allowWrite does not contain %s", want)
		}
	}
	if !slices.Equal(got.Network.AllowedDomains, []string{APIHost, platformHost, "github.com"}) {
		t.Fatalf("allowed domains = %v", got.Network.AllowedDomains)
	}
}

func TestCheckCodingRefusesMissingSandbox(t *testing.T) {
	root := shortCodingRoot(t)
	o := CodingOptions{
		SRTBin: filepath.Join(root, "missing-srt"), RunDir: filepath.Join(root, "run"),
		HomeDir: root, PATH: testPATH, Repo: filepath.Join(root, "repo"),
		TicketsRoot: filepath.Join(root, "tickets"), Worktree: filepath.Join(root, "worktree"),
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
	o := CodingOptions{
		SRTBin: bin, RunDir: runDir, HomeDir: home, PATH: testPATH,
		Repo: repo, TicketsRoot: tickets, Worktree: worktree,
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
