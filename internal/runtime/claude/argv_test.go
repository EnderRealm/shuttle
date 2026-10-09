package claude

import (
	"slices"
	"strings"
	"testing"
)

// goldenOptions is the §7.4 worked example: the daily agent's manifest against
// one run directory.
func goldenOptions() Options {
	return Options{
		Bin:            "/opt/homebrew/bin/claude",
		RunDir:         "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a",
		Auth:           AuthAPIKey,
		Model:          "sonnet",
		MaxTurns:       12,
		MaxBudgetUSD:   1.50,
		Tools:          []string{"Read", "Glob", "Grep", "Write"},
		TimeoutSeconds: 900,
		APIKey:         "sk-ant-test",
		Prompt:         "summarise the day",
		PATH:           "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin",
		HomeDir:        "/Users/steve",
		Timezone:       "America/Los_Angeles",
	}
}

func TestArgvGolden(t *testing.T) {
	want := []string{
		"/opt/homebrew/bin/claude",
		"-p",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		"--model", "sonnet",
		"--max-turns", "12",
		"--max-budget-usd", "1.50",
		"--strict-mcp-config",
		"--setting-sources", "",
		"--permission-mode", "dontAsk",
		"--tools", "Read,Glob,Grep,Write",
		"--settings", "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/settings.json",
		"--no-session-persistence",
	}
	got, err := Argv(goldenOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("argv =\n%q\nwant\n%q", got, want)
	}

	// A launcher is where the §11 sandbox is interposed: the same command
	// line, behind the prefix the caller passes.
	o := goldenOptions()
	o.Launcher = []string{"/opt/homebrew/bin/srt", "-s", "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/srt.json", "--"}
	wantLaunched := append(append([]string{}, o.Launcher...), want...)
	got, err = Argv(o)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, wantLaunched) {
		t.Errorf("launched argv =\n%q\nwant\n%q", got, wantLaunched)
	}
}

// The budget is rendered as a fixed two decimals, so a manifest's 0.05 does not
// reach the CLI as 0.05000000000000000277.
func TestArgvBudgetFormat(t *testing.T) {
	o := goldenOptions()
	o.MaxBudgetUSD = 0.05
	argv, err := Argv(o)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(argv, "--max-budget-usd")
	if got := argv[i+1]; got != "0.05" {
		t.Errorf("--max-budget-usd = %q, want %q", got, "0.05")
	}
}

// A manifest may legally carry subscription auth (§4.1); emitting the api_key
// argv for it would run the wrong flag set silently.
func TestArgvRefusesSubscriptionAuth(t *testing.T) {
	o := goldenOptions()
	o.Auth = "subscription"
	if _, err := Argv(o); err == nil || !strings.Contains(err.Error(), "subscription") {
		t.Fatalf("err = %v, want one naming subscription auth", err)
	}
	if _, err := Environ(o); err == nil {
		t.Fatal("Environ built an api_key environment for subscription auth")
	}
}

// A relative path would resolve against the daemon's own working directory
// rather than the run's.
func TestArgvRefusesRelativePaths(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{name: "bin", edit: func(o *Options) { o.Bin = "claude" }},
		{name: "run dir", edit: func(o *Options) { o.RunDir = "runs/daily/daily-20260902-043000-8f2a" }},
		// An empty run dir renders the allow rules as Read(//**), which would
		// make the permission document mean the whole filesystem.
		{name: "empty run dir", edit: func(o *Options) { o.RunDir = "" }},
		{name: "launcher", edit: func(o *Options) { o.Launcher = []string{"srt", "-s", "srt.json"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := goldenOptions()
			tt.edit(&o)
			if _, err := Argv(o); err == nil {
				t.Error("Argv built a command line from a path that is not absolute")
			}
			if _, err := Environ(o); err == nil {
				t.Error("Environ built an environment from a path that is not absolute")
			}
			if _, err := SettingsJSON(o); err == nil {
				t.Error("SettingsJSON rendered permission rules from a path that is not absolute")
			}
		})
	}
}

// Every value the argv interpolates is passed in, so this package refuses a
// launch it cannot express rather than emitting an empty or zero flag.
func TestArgvRefusesUnusableValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{name: "model", edit: func(o *Options) { o.Model = "" }},
		{name: "tools", edit: func(o *Options) { o.Tools = nil }},
		// A tool the permission document cannot express is not launchable
		// either: --tools would name it with nothing allowing it.
		{name: "unknown tool", edit: func(o *Options) { o.Tools = []string{"Read", "Bash"} }},
		{name: "max turns", edit: func(o *Options) { o.MaxTurns = 0 }},
		// Below a cent the two-decimal rendering would cap the run at $0.00.
		{name: "budget", edit: func(o *Options) { o.MaxBudgetUSD = 0.004 }},
		// The environment is built, not inherited: an empty key would launch
		// and fail as the same §7.6 auth class a revoked one produces.
		{name: "api key", edit: func(o *Options) { o.APIKey = "" }},
		{name: "path", edit: func(o *Options) { o.PATH = "" }},
		{name: "home dir", edit: func(o *Options) { o.HomeDir = "" }},
		{name: "timezone", edit: func(o *Options) { o.Timezone = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := goldenOptions()
			tt.edit(&o)
			if argv, err := Argv(o); err == nil {
				t.Errorf("argv = %q, want a refused launch", argv)
			}
			if env, err := Environ(o); err == nil {
				t.Errorf("environment = %q, want a refused launch", env)
			}
		})
	}
}

func TestSettingsJSONGolden(t *testing.T) {
	want := `{
  "permissions": {
    "allow": [
      "Read(//Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/**)",
      "Edit(//Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/out/**)"
    ],
    "deny": [
      "Bash",
      "WebFetch",
      "WebSearch",
      "Agent",
      "Task"
    ]
  },
  "autoMemoryEnabled": false,
  "disableAllHooks": true
}`
	got, err := SettingsJSON(goldenOptions())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("settings =\n%s\nwant\n%s", got, want)
	}
}

// The allow list follows the manifest: a tool the agent did not ask for gets
// no rule.
func TestSettingsJSONDerivesAllowFromTools(t *testing.T) {
	o := goldenOptions()
	o.Tools = []string{"Read"}
	got, err := SettingsJSON(o)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), "Edit(") {
		t.Errorf("settings allow rules for tools the manifest omits:\n%s", got)
	}
}

func TestSettingsJSONRefusesUnknownTool(t *testing.T) {
	o := goldenOptions()
	o.Tools = []string{"Read", "Bash"}
	if _, err := SettingsJSON(o); err == nil || !strings.Contains(err.Error(), "Bash") {
		t.Fatalf("err = %v, want one naming Bash", err)
	}
}

// The child's environment is built, not inherited: exactly the §7.4 nine, and
// nothing the daemon's own session carries.
func TestEnvironIsBuiltFromScratch(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "/private/tmp/agent.sock")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "inherited-oauth-token")

	env, err := Environ(goldenOptions())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"PATH":   "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin",
		"HOME":   "/Users/steve",
		"TMPDIR": "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/tmp",
		// srt rewrites the child's TMPDIR to this; unset, the model's temp
		// writes would land in /tmp/claude, outside the run directory (§11).
		"CLAUDE_CODE_TMPDIR":                       "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/tmp",
		"CLAUDE_CONFIG_DIR":                        "/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/claude-home",
		"ANTHROPIC_API_KEY":                        "sk-ant-test",
		"DISABLE_TELEMETRY":                        "1",
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
		"TZ": "America/Los_Angeles",
	}
	got := map[string]string{}
	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("environment entry %q is not name=value", entry)
		}
		if _, dup := got[name]; dup {
			t.Errorf("environment names %s twice", name)
		}
		got[name] = value
	}
	if len(got) != len(want) {
		t.Errorf("environment has %d variables, want %d: %q", len(got), len(want), env)
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %q, want %q", name, got[name], value)
		}
	}
	for _, name := range []string{"SSH_AUTH_SOCK", "ANTHROPIC_AUTH_TOKEN"} {
		if _, ok := got[name]; ok {
			t.Errorf("%s reached the child from the daemon's own environment", name)
		}
	}
}
