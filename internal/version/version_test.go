package version

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const repoRoot = "../.."

// A release's binary reports the tag it was built from, with the ldflags the
// release config actually passes, so a change that breaks either side breaks
// this. The daemon's host_build, which the updater confirms, is the same
// string.
func TestVersionReportsRelease(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot, ".goreleaser.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Builds []struct {
			Main    string   `yaml:"main"`
			Env     []string `yaml:"env"`
			GOOS    []string `yaml:"goos"`
			GOARCH  []string `yaml:"goarch"`
			Tags    []string `yaml:"tags"`
			LDFlags []string `yaml:"ldflags"`
		} `yaml:"builds"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Builds) != 1 {
		t.Fatalf("builds = %+v", cfg.Builds)
	}
	build := cfg.Builds[0]
	if !slices.Contains(build.Env, "CGO_ENABLED=1") || !slices.Contains(build.Tags, "release") {
		t.Fatalf("a release build needs cgo and the release tag: %+v", build)
	}
	if !slices.Equal(build.GOOS, []string{"darwin"}) || !slices.Equal(build.GOARCH, []string{"arm64"}) {
		t.Fatalf("targets = %v %v", build.GOOS, build.GOARCH)
	}

	const release = "v1.2.3"
	ldflags := regexp.MustCompile(`\{\{\s*\.Tag\s*\}\}`).ReplaceAllString(strings.Join(build.LDFlags, " "), release)
	bin := filepath.Join(t.TempDir(), "shuttle")
	cmd := exec.Command("go", "build", "-tags", strings.Join(build.Tags, ","), "-ldflags", ldflags, "-o", bin, "./"+filepath.Clean(build.Main))
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), build.Env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, args := range [][]string{{"version"}, {"--version"}} {
		out, err := exec.Command(bin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
		if got := strings.TrimSpace(string(out)); !strings.HasSuffix(got, release) || strings.Contains(got, "dev") {
			t.Fatalf("%v printed %q, want %s", args, got, release)
		}
	}

	workflow, err := os.ReadFile(filepath.Join(repoRoot, ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		On struct {
			Push struct {
				Tags []string `yaml:"tags"`
			} `yaml:"push"`
		} `yaml:"on"`
		Jobs map[string]struct {
			RunsOn string `yaml:"runs-on"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(workflow, &wf); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(wf.On.Push.Tags, "v*") {
		t.Fatalf("workflow triggers = %+v", wf.On)
	}
	if runner := wf.Jobs["release"].RunsOn; !strings.HasPrefix(runner, "macos-") {
		t.Fatalf("the release runs on %q: cgo needs a macOS runner", runner)
	}
}

func TestStringIsTheRelease(t *testing.T) {
	original := Current
	t.Cleanup(func() { Current = original })
	Current = "v1.2.3"
	if got := String(); got != "v1.2.3" {
		t.Fatalf("String() = %q", got)
	}
}
