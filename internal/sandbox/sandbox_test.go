package sandbox

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// defaultSRTBin is where Homebrew's symlink puts srt; SHUTTLE_SRT_BIN overrides
// it for a host that installed the npm package somewhere else.
const defaultSRTBin = "/opt/homebrew/bin/srt"

const testPATH = "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin"

// goldenOptions is the §11 worked example: the daily agent, which adds no
// domain of its own, against one run directory.
func goldenOptions() Options {
	return Options{
		SRTBin:  defaultSRTBin,
		RunDir:  "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a",
		HomeDir: "/Users/steve",
		PATH:    testPATH,
	}
}

// The profile is the boundary itself, so it is compared byte for byte: a key
// srt does not recognise is accepted silently by 1.0.0 rather than refused, so
// a misspelt one would otherwise reach production as a missing restriction.
func TestProfileJSONGolden(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("testdata", "profile.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	// json.MarshalIndent ends without one; an editor that adds a final newline
	// to the file must not turn a boundary test into a formatting test.
	want = bytes.TrimSuffix(want, []byte("\n"))
	got, err := ProfileJSON(goldenOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("profile =\n%s\nwant\n%s", got, want)
	}
}

func TestLauncherGolden(t *testing.T) {
	want := []string{
		defaultSRTBin, "-s",
		"/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/srt.json",
		"--",
	}
	got, err := Launcher(goldenOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("launcher =\n%q\nwant\n%q", got, want)
	}
}

// The manifest's domains are appended to the model API host, which stays first
// and appears once however the manifest names it.
func TestProfileAllowedDomains(t *testing.T) {
	o := goldenOptions()
	o.AllowedDomains = []string{"*.example.com", APIHost, "*.example.com"}
	doc, err := ProfileJSON(o)
	if err != nil {
		t.Fatal(err)
	}
	want := `"allowedDomains": [
      "api.anthropic.com",
      "*.example.com"
    ]`
	if !strings.Contains(string(doc), want) {
		t.Errorf("profile =\n%s\nwant it to carry\n%s", doc, want)
	}
}

// Every entry point is guarded by the same contract: a profile srt would refuse,
// or one whose paths are not the run's, is not a boundary.
func TestRefusesUnusableOptions(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{name: "srt bin", edit: func(o *Options) { o.SRTBin = "srt" }},
		{name: "run dir", edit: func(o *Options) { o.RunDir = "runs/daily/daily-20260902-043000-8f2a" }},
		{name: "empty run dir", edit: func(o *Options) { o.RunDir = "" }},
		{name: "home dir", edit: func(o *Options) { o.HomeDir = "" }},
		// allowRead carries RunDir verbatim and the deny entries are cleaned by
		// filepath.Join: an unclean path would let the two name one directory
		// differently.
		{name: "run dir with a trailing slash", edit: func(o *Options) { o.RunDir += "/" }},
		{name: "run dir with a dot-dot", edit: func(o *Options) { o.RunDir += "/../8f2a" }},
		{name: "run dir with a doubled separator", edit: func(o *Options) {
			o.RunDir = "/Users/steve/.shuttle//runs/daily/daily-20260902-043000-8f2a"
		}},
		{name: "home dir with a trailing slash", edit: func(o *Options) { o.HomeDir += "/" }},
		{name: "home dir with a dot-dot", edit: func(o *Options) { o.HomeDir = "/Users/steve/../steve" }},
		{name: "path", edit: func(o *Options) { o.PATH = "" }},
		// srt's mux socket lands under <run>/tmp, and a sun_path over the
		// darwin limit makes it fail before it launches anything.
		{name: "run dir too long for the mux socket", edit: func(o *Options) {
			o.RunDir = "/Users/steve/.shuttle/runs/daily/" + strings.Repeat("d", 80)
		}},
		{name: "empty domain", edit: func(o *Options) { o.AllowedDomains = []string{""} }},
		{name: "whitespace in domain", edit: func(o *Options) { o.AllowedDomains = []string{"api.anthropic.com evil.example"} }},
		// A pattern is legitimate; a bare star is the network section not
		// applying at all.
		{name: "bare star domain", edit: func(o *Options) { o.AllowedDomains = []string{"*"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := goldenOptions()
			tt.edit(&o)
			if doc, err := ProfileJSON(o); err == nil {
				t.Errorf("profile =\n%s\nwant a refused sandbox", doc)
			}
			if argv, err := Launcher(o); err == nil {
				t.Errorf("launcher = %q, want a refused sandbox", argv)
			}
			if err := Check(context.Background(), o); err == nil {
				t.Error("Check passed a sandbox that cannot be built")
			}
		})
	}
}

// shortRunDir is a run directory under /tmp rather than t.TempDir's
// /var/folders/… path: srt's mux socket is <run>/tmp/srt-mux-<pid>-<n>.sock and
// the temp root alone would put it over the 103-byte unix socket path limit
// (104-byte sun_path less its NUL), which the sandbox checks enforce even
// against a stand-in srt. That rules out os.TempDir() as much as t.TempDir():
// on macOS it is the same 49–57-byte /var/folders/… root.
// EvalSymlinks resolves /tmp to /private/tmp, which is what the seatbelt profile
// matches the sandboxed process's writes against.
func shortRunDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "shuttle-srt-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"out", "tmp", "claude-home", "inputs"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// liveOptions is a real sandbox around a real run directory: the deny list is
// this host's home, so what it refuses is what production would refuse.
func liveOptions(t *testing.T) Options {
	t.Helper()
	bin := os.Getenv("SHUTTLE_SRT_BIN")
	if bin == "" {
		bin = defaultSRTBin
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("srt is not installed at %s: %v", bin, err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	return Options{SRTBin: bin, RunDir: shortRunDir(t), HomeDir: home, PATH: testPATH}
}

// srt refusing a broken profile is the property the preflight rests on: 1.0.0
// exits non-zero rather than running with its default configuration, so a
// profile that never reached disk intact cannot become a permissive run. This
// spends nothing and needs no key, so it is not behind SHUTTLE_LIVE_SRT.
func TestCheckRefusesAMalformedProfile(t *testing.T) {
	o := liveOptions(t)
	if _, err := WriteProfile(o); err != nil {
		t.Fatal(err)
	}
	if err := Check(context.Background(), o); err != nil {
		t.Fatalf("Check on the generated profile: %v", err)
	}

	tests := []struct {
		name    string
		profile string
	}{
		{name: "truncated json", profile: `{"filesystem": {"denyRead": [`},
		// 1.0.0's schema requires deniedDomains and denyWrite; the design's
		// original §11 profile omitted both.
		{name: "missing required keys", profile: `{
  "filesystem": {"denyRead": [], "allowWrite": []},
  "network": {"allowedDomains": []}
}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.WriteFile(o.ProfilePath(), []byte(tt.profile), 0o600); err != nil {
				t.Fatal(err)
			}
			err := Check(context.Background(), o)
			if err == nil || !strings.Contains(err.Error(), "Refusing to run") {
				t.Fatalf("err = %v, want srt refusing to run", err)
			}
		})
	}
}

// TestLiveSRT proves the three properties the profile claims, on this host,
// against the real binary: the model writes nothing outside its run directory,
// reaches no host but the allowed one, and cannot use the SSH agent. Opt-in
// because it needs srt installed and takes a few seconds; it spends nothing.
func TestLiveSRT(t *testing.T) {
	if os.Getenv("SHUTTLE_LIVE_SRT") != "1" {
		t.Skip("set SHUTTLE_LIVE_SRT=1 to run the sandbox against the real srt")
	}
	o := liveOptions(t)
	if _, err := WriteProfile(o); err != nil {
		t.Fatal(err)
	}

	t.Run("filesystem", func(t *testing.T) {
		// inputs/ is inside the run directory and outside the three writable
		// subdirectories: the model reads it and may not write it.
		if code, _, stderr := srtExec(t, o, "touch "+filepath.Join(o.RunDir, "inputs", "probe")); code == 0 {
			t.Error("a write to inputs/ succeeded inside the sandbox")
		} else {
			t.Logf("inputs/ write refused: exit %d: %s", code, strings.TrimSpace(stderr))
		}
		// The positive control: a wholly broken srt would fail the assertion
		// above for the wrong reason.
		if code, _, stderr := srtExec(t, o, "touch "+filepath.Join(o.RunDir, "out", "probe")); code != 0 {
			t.Errorf("a write to out/ failed: exit %d: %s", code, strings.TrimSpace(stderr))
		}
		// The whole of ~/.shuttle is denied, so no other run's prompt,
		// transcript or out/ is readable and neither is the ledger. The run
		// directory this test built is under /private/tmp rather than beneath
		// it, so the denial has to be probed against the real home.
		t.Run("home state", func(t *testing.T) {
			state := filepath.Join(o.HomeDir, ".shuttle")
			if _, err := os.Stat(state); err != nil {
				t.Skipf("%s does not exist on this host, so an ls of it would fail for the wrong reason: %v", state, err)
			}
			code, _, stderr := srtExec(t, o, "ls "+state)
			if code == 0 || !strings.Contains(stderr, "Operation not permitted") {
				t.Errorf("ls %s returned exit %d: %s", state, code, strings.TrimSpace(stderr))
			} else {
				t.Logf("%s read refused: exit %d: %s", state, code, strings.TrimSpace(stderr))
			}
		})
		// The read positive controls: the run's own directory stays readable,
		// which is what allowRead re-allows beneath the denied ~/.shuttle in
		// production.
		if code, _, stderr := srtExec(t, o, "ls "+o.RunDir); code != 0 {
			t.Errorf("an ls of the run directory failed: exit %d: %s", code, strings.TrimSpace(stderr))
		}
		if code, stdout, stderr := srtExec(t, o, "cat "+o.ProfilePath()); code != 0 || !strings.Contains(stdout, APIHost) {
			t.Errorf("a read of %s returned exit %d, %q: %s", o.ProfilePath(), code, stdout, strings.TrimSpace(stderr))
		}
	})

	t.Run("network", func(t *testing.T) {
		if code, _, stderr := srtExec(t, o, "curl -sS -m 15 -o /dev/null https://example.com"); code == 0 {
			t.Error("a fetch to a host outside allowedDomains succeeded")
		} else {
			t.Logf("example.com refused: exit %d: %s", code, strings.TrimSpace(stderr))
		}
		code, stdout, stderr := srtExec(t, o, "curl -sS -m 15 -o /dev/null -w '%{http_code}' https://"+APIHost)
		if code != 0 || len(stdout) != 3 || strings.Trim(stdout, "0123456789") != "" {
			t.Errorf("a fetch to %s returned exit %d, %q: %s", APIHost, code, stdout, strings.TrimSpace(stderr))
		}
	})

	t.Run("ssh agent", func(t *testing.T) {
		// With no agent socket in this test's own environment there is nothing
		// for the sandbox to refuse, so the check would pass for the wrong
		// reason.
		if os.Getenv("SSH_AUTH_SOCK") == "" {
			t.Skip("SSH_AUTH_SOCK is unset: nothing here proves the agent is refused")
		}
		code, _, stderr := srtExec(t, o, "ssh-add -l")
		if code == 0 || !strings.Contains(stderr, "Error connecting to agent") {
			t.Errorf("ssh-add -l returned exit %d: %s", code, strings.TrimSpace(stderr))
		}
	})
}

// srtExec runs one shell command under the sandbox and returns its exit code,
// stdout and stderr. The environment is built from scratch as the runtime's is
// (§7.4), except that SSH_AUTH_SOCK is passed through when the test has one:
// the agent must be refused by allowUnixSockets, not by the child never being
// told where the socket is.
func srtExec(t *testing.T, o Options, script string) (int, string, string) {
	t.Helper()
	launcher, err := Launcher(o)
	if err != nil {
		t.Fatal(err)
	}
	argv := append(launcher, "/bin/sh", "-c", script)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = o.RunDir
	cmd.Env = []string{
		"PATH=" + o.PATH,
		"HOME=" + o.HomeDir,
		"TMPDIR=" + o.tmpDir(),
		"CLAUDE_CODE_TMPDIR=" + o.tmpDir(),
	}
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		cmd.Env = append(cmd.Env, "SSH_AUTH_SOCK="+sock)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), stdout.String(), stderr.String()
	case err != nil:
		t.Fatalf("srt %q: %v", script, err)
	}
	return 0, stdout.String(), stderr.String()
}
