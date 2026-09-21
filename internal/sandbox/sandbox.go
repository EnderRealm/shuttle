package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"
)

// APIHost is the model API the allowedDomains list always carries (§11). The
// manifest's sandbox.allowed_domains is appended to it, never substituted for
// it: a run that cannot reach the API has nothing to run.
const APIHost = "api.anthropic.com"

// profileFile is the generated profile's name in the run directory (§7.2).
const profileFile = "srt.json"

// srt 1.0.0 creates its host-side mux socket as <TMPDIR>/srt-mux-<pid>-<n>.sock
// and fails with `listen EINVAL: invalid argument` before launching anything if
// the path is over the darwin sun_path limit of 104 bytes including the NUL.
// The probe is the longest name that shape can take with a five-digit pid and a
// counter assumed to stay under 100, so a run directory is refused here rather
// than at launch.
const (
	muxSocketProbe    = "srt-mux-99999-99.sock"
	maxUnixSocketPath = 103
)

// stderrLimitBytes bounds what a refusal carries back to the runner for §7.6's
// sandbox class. The head is kept rather than the tail because srt names the
// invalid key on its first lines and repeats the refusal after them.
const stderrLimitBytes = 2048

// Options is the sandbox for one run. Like the runtime package, nothing here
// reads the ambient process environment: a daemon-launched run must not inherit
// a login session (§7.4).
type Options struct {
	SRTBin         string   // absolute path to the srt binary (config paths.srt_bin)
	RunDir         string   // the run directory (§7.2), absolute
	HomeDir        string   // absolute; the profile's home-relative deny paths are derived from it
	PATH           string   // the baked PATH, for Check's own child
	AllowedDomains []string // manifest sandbox.allowed_domains, appended to APIHost
	DeniedRead     []string // additional harness-private paths outside RunDir
}

// ProfilePath is the generated profile, which Launcher's argv names (§7.2).
func (o Options) ProfilePath() string { return filepath.Join(o.RunDir, profileFile) }

// tmpDir is the run's tmp/ (§7.2): both what srt puts its mux socket in and,
// through CLAUDE_CODE_TMPDIR, what it rewrites the child's TMPDIR to.
func (o Options) tmpDir() string { return filepath.Join(o.RunDir, "tmp") }

// profile is the srt 1.0.0 settings document. Every field is present because
// 1.0.0 validates the file with a schema that requires network.allowedDomains,
// network.deniedDomains, filesystem.denyRead, filesystem.allowWrite and
// filesystem.denyWrite, and refuses to run at all when one is missing.
type profile struct {
	Filesystem filesystem `json:"filesystem"`
	Network    network    `json:"network"`
}

type filesystem struct {
	DenyRead   []string `json:"denyRead"`
	AllowRead  []string `json:"allowRead"`
	AllowWrite []string `json:"allowWrite"`
	DenyWrite  []string `json:"denyWrite"`
}

type network struct {
	AllowedDomains    []string `json:"allowedDomains"`
	DeniedDomains     []string `json:"deniedDomains"`
	AllowUnixSockets  []string `json:"allowUnixSockets"`
	AllowLocalBinding bool     `json:"allowLocalBinding"`
}

// ProfileJSON renders the §11 profile. Paths are absolute rather than the
// design's `~`-relative form, so the document does not depend on the HOME srt
// happens to expand it against.
func ProfileJSON(o Options) ([]byte, error) {
	if err := requireLaunchable(o); err != nil {
		return nil, err
	}
	p := profile{
		Filesystem: filesystem{
			// Reads are deny-then-allow in 1.0.0: everything not named here is
			// readable, so the state and credential directories are denied
			// explicitly and the run directory is re-allowed beneath the
			// denied ~/.shuttle. The whole of ~/.shuttle is denied rather than
			// runs.db and config.json alone: every other run's prompt, inputs,
			// transcript and out/ live under it, as do the ledger's -wal and
			// -shm sidecars, and anything the model can read it can send to
			// the allowed domain.
			DenyRead: []string{
				filepath.Join(o.HomeDir, ".ssh"),
				filepath.Join(o.HomeDir, ".gnupg"),
				filepath.Join(o.HomeDir, "Library", "Keychains"),
				filepath.Join(o.HomeDir, ".claude", ".credentials.json"),
				filepath.Join(o.HomeDir, ".claude", "settings.json"),
				filepath.Join(o.HomeDir, ".ticket"),
				filepath.Join(o.HomeDir, ".loom"),
				filepath.Join(o.HomeDir, ".shuttle"),
				filepath.Join(o.HomeDir, ".codex"),
				filepath.Join(o.HomeDir, ".cursor"),
				filepath.Join(o.HomeDir, "code"),
			},
			AllowRead:  []string{o.RunDir},
			AllowWrite: []string{filepath.Join(o.RunDir, "out"), o.tmpDir(), filepath.Join(o.RunDir, "claude-home")},
			// srt allows writes to /tmp/claude, /private/tmp/claude,
			// ~/.claude/debug and ~/.npm/_logs whether or not a profile names
			// them, and denyWrite is what takes them back: without it "the
			// model writes nothing outside its run directory" is approximate.
			DenyWrite: []string{
				"/tmp/claude",
				"/private/tmp/claude",
				filepath.Join(o.HomeDir, ".claude", "debug"),
				filepath.Join(o.HomeDir, ".npm", "_logs"),
			},
		},
		Network: network{
			AllowedDomains: allowedDomains(o),
			DeniedDomains:  []string{},
			// The SSH agent is refused by this, not by denyRead: an empty
			// socket list is what lets the harness, and never the model, own
			// every write that leaves the machine.
			AllowUnixSockets:  []string{},
			AllowLocalBinding: false,
		},
	}
	p.Filesystem.DenyRead = append(p.Filesystem.DenyRead, o.DeniedRead...)
	return json.MarshalIndent(p, "", "  ")
}

// WriteProfile renders the profile to ProfilePath and returns that path. The
// run directory is not created here: §7.1 creates it before anything else in
// the run, so a missing one is a run that never got that far and is better as
// this write's error than as a directory appearing out of order.
func WriteProfile(o Options) (string, error) {
	doc, err := ProfileJSON(o)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(o.ProfilePath(), doc, 0o600); err != nil {
		return "", err
	}
	return o.ProfilePath(), nil
}

// Launcher is the §11 invocation, as the argv prefix claude.Options.Launcher
// takes. `--` rather than `-c`: srt's -c takes one string it runs through bash
// and silently drops every positional argument after it, so the model argv the
// runtime appends would never be exec'd. The profile must already be on disk
// when this argv runs — srt refuses to start without it.
func Launcher(o Options) ([]string, error) {
	if err := requireLaunchable(o); err != nil {
		return nil, err
	}
	return []string{o.SRTBin, "-s", o.ProfilePath(), "--"}, nil
}

// Check is the §7.5 sandbox preflight: srt loads the generated profile and runs
// true under it. A malformed or missing profile makes srt exit non-zero rather
// than fall back to a permissive default, so this failing is the run's sandbox
// class (§7.6) and the returned error carries srt's own words as the evidence.
func Check(ctx context.Context, o Options) error {
	if err := requireLaunchable(o); err != nil {
		return err
	}
	// srt's mux socket lands in this directory, and the preflight runs before
	// the runtime creates the run's subdirectories.
	if err := os.MkdirAll(o.tmpDir(), 0o755); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, o.SRTBin, "-s", o.ProfilePath(), "-c", "true")
	cmd.Dir = o.RunDir
	// Built from scratch like the runtime's (§7.4). CLAUDE_CODE_TMPDIR is what
	// srt rewrites the child's TMPDIR to; without it the child would get
	// /tmp/claude, which the profile denies writes to.
	cmd.Env = []string{
		"PATH=" + o.PATH,
		"HOME=" + o.HomeDir,
		"TMPDIR=" + o.tmpDir(),
		"CLAUDE_CODE_TMPDIR=" + o.tmpDir(),
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// A chdir or exec failure happens before srt says anything, and an
		// error ending in a dangling ": " is worse evidence than one without.
		if evidence := head(stderr.String()); evidence != "" {
			return fmt.Errorf("srt -s %s: %w: %s", o.ProfilePath(), err, evidence)
		}
		return fmt.Errorf("srt -s %s: %w", o.ProfilePath(), err)
	}
	return nil
}

// allowedDomains is the manifest's list appended to the model API host,
// de-duplicated with APIHost first: the profile is a golden artefact, so the
// order may not depend on the manifest's.
func allowedDomains(o Options) []string {
	domains := []string{APIHost}
	for _, d := range o.AllowedDomains {
		if !slices.Contains(domains, d) {
			domains = append(domains, d)
		}
	}
	return domains
}

// head is srt's stderr, bounded, for the ledger's evidence column.
func head(s string) string {
	if len(s) > stderrLimitBytes {
		s = s[:stderrLimitBytes]
		// The cut is a byte offset and can land mid-rune; this head reaches the
		// ledger's evidence column and the summary email, so it may not carry
		// the leading bytes of a rune whose remainder was dropped.
		for len(s) > 0 {
			if r, size := utf8.DecodeLastRuneInString(s); r != utf8.RuneError || size > 1 {
				break
			}
			s = s[:len(s)-1]
		}
	}
	return strings.TrimSpace(s)
}

// requireLaunchable is the contract the profile, the launcher and the preflight
// are built against, checked at all three entry points so they agree on what a
// valid Options is: a profile that cannot be written or that srt would refuse
// is not a boundary, and this package reads nothing ambient to fill a gap.
func requireLaunchable(o Options) error {
	// The launcher is exec'd as argv[0], so a relative one would resolve
	// against the daemon's own PATH rather than the path the caller named.
	if !filepath.IsAbs(o.SRTBin) {
		return fmt.Errorf("srt bin %q is not an absolute path", o.SRTBin)
	}
	// A relative run dir would put the only writable directories, and the
	// allowRead the profile grants, wherever the daemon happens to be running.
	if !filepath.IsAbs(o.RunDir) {
		return fmt.Errorf("run dir %q is not an absolute path", o.RunDir)
	}
	if !filepath.IsAbs(o.HomeDir) {
		return fmt.Errorf("home dir %q is not an absolute path", o.HomeDir)
	}
	// allowRead carries RunDir verbatim while every other entry goes through
	// filepath.Join, which cleans; a trailing slash, a `..` or a doubled
	// separator would let the two disagree about the same directory.
	if o.RunDir != filepath.Clean(o.RunDir) {
		return fmt.Errorf("run dir %q is not a clean path", o.RunDir)
	}
	if o.HomeDir != filepath.Clean(o.HomeDir) {
		return fmt.Errorf("home dir %q is not a clean path", o.HomeDir)
	}
	if o.PATH == "" {
		return fmt.Errorf("path is empty: the preflight child inherits nothing to fall back on")
	}
	if sock := filepath.Join(o.tmpDir(), muxSocketProbe); len(sock) > maxUnixSocketPath {
		return fmt.Errorf("run dir %q: srt's mux socket %q is %d bytes, over the %d-byte unix socket path limit",
			o.RunDir, sock, len(sock), maxUnixSocketPath)
	}
	for _, d := range o.AllowedDomains {
		if d == "" {
			return fmt.Errorf("sandbox.allowed_domains: an empty domain")
		}
		if strings.ContainsAny(d, " \t\n\r") {
			return fmt.Errorf("sandbox.allowed_domains: %q contains whitespace", d)
		}
		// A pattern like *.example.com is a legitimate allowance; a bare star
		// is every host there is, which is the network section not applying.
		if d == "*" {
			return fmt.Errorf("sandbox.allowed_domains: %q would allow every host", d)
		}
	}
	for _, path := range o.DeniedRead {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("sandbox denied read path %q is not absolute and clean", path)
		}
		rel, err := filepath.Rel(o.RunDir, path)
		if err != nil {
			return fmt.Errorf("sandbox denied read path %q relative to run dir: %w", path, err)
		}
		if rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("sandbox denied read path %q is inside run dir %q, whose allowRead would reopen it", path, o.RunDir)
		}
	}
	return nil
}
