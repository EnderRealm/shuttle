package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const platformHost = "platform.claude.com"

// CodingOptions describes the per-segment profile used by hosted weft model
// launches. It is intentionally separate from Options: a coding workflow has
// a writable worktree, ticket store and subscription-backed Claude config,
// while the phase-1 report profile denies all of those.
type CodingOptions struct {
	SRTBin         string
	RunDir         string
	HomeDir        string
	PATH           string
	Repo           string
	TicketsRoot    string
	Worktree       string
	UserTempDir    string
	AllowedDomains []string
	AllowWrite     []string
}

func (o CodingOptions) ProfilePath() string { return filepath.Join(o.RunDir, profileFile) }
func (o CodingOptions) tmpDir() string      { return filepath.Join(o.RunDir, "tmp") }

// WorktreePath mirrors WeftKit's hosted WorktreeProvider derivation.
func WorktreePath(root, repo, ticket string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("weft root and repository must be absolute")
	}
	canonical, err := CanonicalRepository(repo)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(canonical))
	repoDir := filepath.Base(canonical) + "-" + hex.EncodeToString(sum[:4])
	return filepath.Join(filepath.Clean(root), "worktrees", repoDir, strings.ReplaceAll(ticket, "/", "-")), nil
}

// CanonicalRepository collapses symlink aliases to the one path used for the
// repository reservation and hosted worker argv. Swift's standardized path
// drops macOS's /private alias, so the host does the same after resolution.
func CanonicalRepository(repo string) (string, error) {
	if !filepath.IsAbs(repo) {
		return "", fmt.Errorf("repository %q is not absolute", repo)
	}
	canonical, err := filepath.EvalSymlinks(filepath.Clean(repo))
	if err != nil {
		return "", fmt.Errorf("resolve repository %q: %w", repo, err)
	}
	if strings.HasPrefix(canonical, "/private/") {
		canonical = strings.TrimPrefix(canonical, "/private")
	}
	return canonical, nil
}

// CodingProfileJSON renders weft-remote/v1 §5's coding policy and snapshots
// every sibling worktree admin directory while the repository reservation is
// held.
func CodingProfileJSON(o CodingOptions) ([]byte, error) {
	if err := requireCoding(o); err != nil {
		return nil, err
	}
	adminRoot := filepath.Join(o.Repo, ".git", "worktrees")
	adminName := filepath.Base(o.Worktree)
	ownAdmin := filepath.Join(adminRoot, adminName)
	if err := requireExpectedWorktreeAdmin(o.Worktree, ownAdmin); err != nil {
		return nil, err
	}
	denyWrite := []string{
		"/tmp/claude",
		"/private/tmp/claude",
		filepath.Join(o.HomeDir, ".npm", "_logs"),
		filepath.Join(o.Worktree, ".git"),
		filepath.Join(o.Repo, ".git", "config"),
		filepath.Join(o.Repo, ".git", "hooks"),
		filepath.Join(o.Repo, ".git", "config.worktree"),
		filepath.Join(o.Repo, ".git", "commondir"),
		filepath.Join(ownAdmin, "commondir"),
		filepath.Join(ownAdmin, "gitdir"),
		filepath.Join(ownAdmin, "config.worktree"),
	}
	entries, err := os.ReadDir(adminRoot)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("list worktree admin directories: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != adminName {
			denyWrite = append(denyWrite, filepath.Join(adminRoot, entry.Name()))
		}
	}
	denyWrite = append(denyWrite,
		filepath.Join(o.TicketsRoot, ".git"),
		filepath.Join(o.HomeDir, ".claude", "settings.json"),
		filepath.Join(o.HomeDir, ".claude", "CLAUDE.md"),
		filepath.Join(o.HomeDir, ".claude", "skills"),
		filepath.Join(o.HomeDir, ".claude", "agents"),
		filepath.Join(o.HomeDir, ".claude", "commands"),
		filepath.Join(o.HomeDir, ".claude", "plugins"),
		filepath.Join(o.HomeDir, ".claude", "rules"),
		filepath.Join(o.HomeDir, ".claude", "projects", "*", "memory"),
		filepath.Join(o.HomeDir, ".claude.json"),
		filepath.Join(o.HomeDir, ".claude", "work-policy"),
		filepath.Join(o.HomeDir, ".claude", "work-policy.sh"),
		filepath.Join(o.HomeDir, ".claude", "security-lens.sh"),
		filepath.Join(o.HomeDir, ".claude", "codex-lens.sh"),
		filepath.Join(o.HomeDir, ".claude", "verdict-merge.sh"),
		filepath.Join(o.HomeDir, ".claude", "rm-gate.sh"),
		filepath.Join(o.HomeDir, ".claude", "work-gate.sh"),
	)
	allowWrite := []string{
		o.Worktree,
		filepath.Join(o.Repo, ".git"),
		o.TicketsRoot,
		filepath.Join(o.HomeDir, ".ticket", "state"),
		filepath.Join(o.HomeDir, ".claude"),
		o.tmpDir(),
		// SwiftPM's build service stages atomic writes and swift test's
		// build temp under confstr's per-user temp dir, ignoring TMPDIR.
		o.UserTempDir,
	}
	for _, path := range o.AllowWrite {
		if !slices.Contains(allowWrite, path) {
			allowWrite = append(allowWrite, path)
		}
	}
	p := profile{
		Filesystem: filesystem{
			DenyRead: []string{
				filepath.Join(o.HomeDir, ".ssh"),
				filepath.Join(o.HomeDir, ".gnupg"),
				filepath.Join(o.HomeDir, "Library", "Keychains"),
				filepath.Join(o.HomeDir, ".loom"),
				filepath.Join(o.HomeDir, ".shuttle"),
				filepath.Join(o.HomeDir, ".codex"),
				filepath.Join(o.HomeDir, ".cursor"),
				filepath.Join(o.HomeDir, "code"),
			},
			AllowRead:  []string{o.Repo, o.TicketsRoot, o.Worktree, o.RunDir},
			AllowWrite: allowWrite,
			DenyWrite:  denyWrite,
		},
		Network: network{
			AllowedDomains:    codingDomains(o.AllowedDomains),
			DeniedDomains:     []string{},
			AllowUnixSockets:  []string{},
			AllowLocalBinding: false,
		},
	}
	return json.MarshalIndent(p, "", "  ")
}

// requireExpectedWorktreeAdmin refuses the suffix Git adds when the expected
// registration name is already occupied. Without it the per-file deny rules
// would protect a directory the model does not use.
func requireExpectedWorktreeAdmin(worktree, expected string) error {
	_, worktreeErr := os.Stat(worktree)
	switch {
	case worktreeErr == nil:
		data, err := os.ReadFile(filepath.Join(worktree, ".git"))
		if err != nil {
			return fmt.Errorf("read worktree gitfile: %w", err)
		}
		value := strings.TrimSpace(string(data))
		actual, ok := strings.CutPrefix(value, "gitdir:")
		if !ok {
			return fmt.Errorf("worktree gitfile does not start with gitdir:")
		}
		actual = strings.TrimSpace(actual)
		if !filepath.IsAbs(actual) {
			actual = filepath.Join(worktree, actual)
		}
		actual = filepath.Clean(actual)
		if actual != expected {
			return fmt.Errorf("worktree admin directory %q, want %q; refusing an unprotected registration", actual, expected)
		}
		return nil
	case !os.IsNotExist(worktreeErr):
		return fmt.Errorf("stat worktree: %w", worktreeErr)
	}
	if _, err := os.Stat(expected); err == nil {
		return fmt.Errorf("worktree admin directory %q already exists before worktree creation", expected)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat expected worktree admin directory: %w", err)
	}
	return nil
}

func WriteCodingProfile(o CodingOptions) (string, error) {
	doc, err := CodingProfileJSON(o)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(o.ProfilePath(), doc, 0o600); err != nil {
		return "", err
	}
	return o.ProfilePath(), nil
}

// CheckCoding is the fail-closed srt preflight for a coding profile.
func CheckCoding(ctx context.Context, o CodingOptions) error {
	if err := requireCoding(o); err != nil {
		return err
	}
	if err := os.MkdirAll(o.tmpDir(), 0o700); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, o.SRTBin, "-s", o.ProfilePath(), "-c", "true")
	cmd.Dir = o.RunDir
	cmd.Env = []string{
		"PATH=" + o.PATH,
		"HOME=" + o.HomeDir,
		"TMPDIR=" + o.tmpDir(),
		"CLAUDE_CODE_TMPDIR=" + o.tmpDir(),
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if evidence := head(stderr.String()); evidence != "" {
			return fmt.Errorf("srt -s %s: %w: %s", o.ProfilePath(), err, evidence)
		}
		return fmt.Errorf("srt -s %s: %w", o.ProfilePath(), err)
	}
	return nil
}

func requireCoding(o CodingOptions) error {
	for name, path := range map[string]string{
		"srt bin": o.SRTBin, "run dir": o.RunDir, "home dir": o.HomeDir,
		"repository": o.Repo, "tickets root": o.TicketsRoot, "worktree": o.Worktree,
		"user temp dir": o.UserTempDir,
	} {
		if !filepath.IsAbs(path) || path != filepath.Clean(path) {
			return fmt.Errorf("%s %q is not a clean absolute path", name, path)
		}
	}
	if o.PATH == "" {
		return fmt.Errorf("path is empty")
	}
	if sock := filepath.Join(o.tmpDir(), muxSocketProbe); len(sock) > maxUnixSocketPath {
		return fmt.Errorf("run dir %q: srt mux socket path is %d bytes, over %d", o.RunDir, len(sock), maxUnixSocketPath)
	}
	for _, path := range o.AllowWrite {
		if !filepath.IsAbs(path) || path != filepath.Clean(path) {
			return fmt.Errorf("sandbox.allow_write: %q is not a clean absolute path", path)
		}
	}
	for _, domain := range o.AllowedDomains {
		if domain == "" || domain == "*" || strings.ContainsAny(domain, " \t\r\n") {
			return fmt.Errorf("sandbox.allowed_domains: invalid domain %q", domain)
		}
	}
	return nil
}

func codingDomains(configured []string) []string {
	out := []string{APIHost, platformHost}
	for _, domain := range configured {
		if !slices.Contains(out, domain) {
			out = append(out, domain)
		}
	}
	return out
}
