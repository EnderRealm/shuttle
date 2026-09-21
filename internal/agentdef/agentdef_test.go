package agentdef

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validManifest = `{
  "name": "daily",
  "description": "Previous-day summary",
  "runtime": {
    "kind": "claude",
    "auth": "api_key",
    "model": "sonnet",
    "max_turns": 12,
    "max_budget_usd": 1.50,
    "timeout_seconds": 900,
    "tools": ["Read", "Glob", "Grep", "Write"]
  },
  "triggers": [
    { "kind": "cron", "expression": "30 4 * * *", "timezone": "America/Los_Angeles" }
  ],
  "inputs": [
    { "provider": "loom-sessions", "args": { "window": "previous-day" } }
  ],
  "outputs": {
    "email": { "max": 1 },
    "file": { "dir": "daily", "max": 1 }
  },
  "sandbox": { "kind": "srt", "allowed_domains": [] }
}`

// writeAgent lays down one agent directory; an empty prompt writes no AGENT.md.
func writeAgent(t *testing.T, root, name, manifest, prompt string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if prompt != "" {
		if err := os.WriteFile(filepath.Join(dir, promptFile), []byte(prompt), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFile), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func replace(t *testing.T, old, new string) string {
	t.Helper()
	if !strings.Contains(validManifest, old) {
		t.Fatalf("valid manifest does not contain %q", old)
	}
	return strings.Replace(validManifest, old, new, 1)
}

func TestLoadRepoDailyAgent(t *testing.T) {
	def, err := Load(filepath.Join("..", "..", "agents", "daily"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if def.Name != "daily" || def.Manifest.Name != "daily" {
		t.Errorf("name = %q / %q, want daily", def.Name, def.Manifest.Name)
	}
	if strings.TrimSpace(def.Prompt) == "" {
		t.Error("prompt is empty")
	}
	if def.Manifest.Runtime.MaxBudgetUSD != 1.50 {
		t.Errorf("max_budget_usd = %v, want 1.5", def.Manifest.Runtime.MaxBudgetUSD)
	}
	if len(def.Manifest.Inputs) != 6 {
		t.Errorf("inputs = %d, want 6", len(def.Manifest.Inputs))
	}
	if def.Manifest.Sandbox.Kind != sandboxKindSRT {
		t.Errorf("sandbox.kind = %q, want srt", def.Manifest.Sandbox.Kind)
	}
}

func TestLoadValidation(t *testing.T) {
	cases := []struct {
		name     string
		dir      string
		manifest string
		want     []string
	}{
		{
			name:     "unknown top-level key",
			manifest: replace(t, `"name": "daily",`, `"name": "daily", "recipients": ["a@b.c"],`),
			want:     []string{"recipients"},
		},
		{
			name:     "unknown nested key",
			manifest: replace(t, `"model": "sonnet",`, `"model": "sonnet", "api_key": "sk-live",`),
			want:     []string{"api_key"},
		},
		{
			name:     "missing max_budget_usd",
			manifest: replace(t, `"max_budget_usd": 1.50,`, ``),
			want:     []string{"runtime.max_budget_usd"},
		},
		{
			name:     "non-positive max_budget_usd",
			manifest: replace(t, `"max_budget_usd": 1.50,`, `"max_budget_usd": 0,`),
			want:     []string{"runtime.max_budget_usd"},
		},
		{
			name:     "missing timeout_seconds",
			manifest: replace(t, `"timeout_seconds": 900,`, ``),
			want:     []string{"runtime.timeout_seconds"},
		},
		{
			name:     "timeout_seconds over the cap",
			manifest: replace(t, `"timeout_seconds": 900,`, `"timeout_seconds": 3601,`),
			want:     []string{"runtime.timeout_seconds", "3600"},
		},
		{
			name:     "Bash in tools",
			manifest: replace(t, `["Read", "Glob", "Grep", "Write"]`, `["Read", "Bash"]`),
			want:     []string{"runtime.tools", "Bash"},
		},
		{
			name:     "empty tools",
			manifest: replace(t, `["Read", "Glob", "Grep", "Write"]`, `[]`),
			want:     []string{"runtime.tools"},
		},
		{
			name:     "sandbox kind other than srt",
			manifest: replace(t, `"kind": "srt"`, `"kind": "none"`),
			want:     []string{"sandbox.kind", "none"},
		},
		{
			name:     "cron expression that is not five fields",
			manifest: replace(t, `"expression": "30 4 * * *"`, `"expression": "30 4 * *"`),
			want:     []string{"triggers[0].expression"},
		},
		{
			name:     "cron descriptor is not a five-field expression",
			manifest: replace(t, `"expression": "30 4 * * *"`, `"expression": "@daily"`),
			want:     []string{"triggers[0].expression"},
		},
		{
			name:     "cron expression carrying a TZ= prefix",
			manifest: replace(t, `"expression": "30 4 * * *"`, `"expression": "TZ=UTC 30 4 * * *"`),
			want:     []string{"triggers[0].expression", "TZ=UTC"},
		},
		{
			name:     "bare TZ= prefix with no expression",
			manifest: replace(t, `"expression": "30 4 * * *"`, `"expression": "TZ=Foo"`),
			want:     []string{"triggers[0].expression", "TZ=Foo"},
		},
		{
			name:     "trailing JSON after the manifest",
			manifest: validManifest + "\n{\"name\": \"daily\"}\n",
			want:     []string{manifestFile, "trailing content"},
		},
		{
			name:     "timezone that is not an IANA name",
			manifest: replace(t, `"timezone": "America/Los_Angeles"`, `"timezone": "Pacific Time"`),
			want:     []string{"triggers[0].timezone", "IANA"},
		},
		{
			name:     "name that does not match the directory",
			manifest: replace(t, `"name": "daily",`, `"name": "weekly",`),
			want:     []string{"name", "weekly", "daily"},
		},
		{
			name:     "name that does not match the schema",
			dir:      "Daily",
			manifest: replace(t, `"name": "daily",`, `"name": "Daily",`),
			want:     []string{"name", "Daily"},
		},
		{
			name:     "no triggers",
			manifest: replace(t, `{ "kind": "cron", "expression": "30 4 * * *", "timezone": "America/Los_Angeles" }`, ``),
			want:     []string{"triggers"},
		},
		{
			name:     "unregistered input provider",
			manifest: replace(t, `"provider": "loom-sessions"`, `"provider": "web-search"`),
			want:     []string{"inputs[0].provider", "web-search"},
		},
		{
			name:     "auth other than api_key or subscription",
			manifest: replace(t, `"auth": "api_key",`, `"auth": "oauth",`),
			want:     []string{"runtime.auth", "oauth"},
		},
		{
			name:     "outputs.email.max above one",
			manifest: replace(t, `"email": { "max": 1 }`, `"email": { "max": 2 }`),
			want:     []string{"outputs.email.max"},
		},
		{
			name:     "outputs.file.dir with a path separator",
			manifest: replace(t, `"dir": "daily"`, `"dir": "../daily"`),
			want:     []string{"outputs.file.dir"},
		},
		{
			name:     "reserved runtime kind codex",
			manifest: replace(t, `"kind": "claude",`, `"kind": "codex",`),
			want:     []string{"runtime.kind", "codex", "reserved for a future phase"},
		},
		{
			name:     "reserved runtime kind ollama",
			manifest: replace(t, `"kind": "claude",`, `"kind": "ollama",`),
			want:     []string{"runtime.kind", "ollama", "reserved for a future phase"},
		},
		{
			name:     "reserved runtime kind script",
			manifest: replace(t, `"kind": "claude",`, `"kind": "script",`),
			want:     []string{"runtime.kind", "script", "reserved for a future phase"},
		},
		{
			name:     "reserved trigger kind event",
			manifest: replace(t, `{ "kind": "cron",`, `{ "kind": "event",`),
			want:     []string{"triggers[0].kind", "event", "reserved for a future phase"},
		},
		{
			name:     "reserved trigger kind fire",
			manifest: replace(t, `{ "kind": "cron",`, `{ "kind": "fire",`),
			want:     []string{"triggers[0].kind", "fire", "reserved for a future phase"},
		},
		{
			name:     "reserved output ticket_create",
			manifest: replace(t, `"email": { "max": 1 },`, `"email": { "max": 1 }, "ticket_create": { "max": 1 },`),
			want:     []string{"outputs.ticket_create", "reserved for a future phase"},
		},
		{
			name:     "reserved output ticket_note",
			manifest: replace(t, `"email": { "max": 1 },`, `"email": { "max": 1 }, "ticket_note": { "max": 1 },`),
			want:     []string{"outputs.ticket_note", "reserved for a future phase"},
		},
		{
			name:     "reserved output memory",
			manifest: replace(t, `"email": { "max": 1 },`, `"email": { "max": 1 }, "memory": { "max": 1 },`),
			want:     []string{"outputs.memory", "reserved for a future phase"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dirName := tc.dir
			if dirName == "" {
				dirName = "daily"
			}
			dir := writeAgent(t, t.TempDir(), dirName, tc.manifest, "body\n")
			_, err := Load(dir)
			if err == nil {
				t.Fatal("Load succeeded, want an error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
			if !strings.Contains(err.Error(), dir) {
				t.Errorf("error %q does not name the agent dir %q", err, dir)
			}
		})
	}
}

func TestLoadPromptBody(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		dir := writeAgent(t, t.TempDir(), "daily", validManifest, "")
		if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), promptFile) {
			t.Fatalf("err = %v, want one naming %s", err, promptFile)
		}
	})
	t.Run("blank", func(t *testing.T) {
		dir := writeAgent(t, t.TempDir(), "daily", validManifest, "  \n\t\n")
		if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("err = %v, want an empty-prompt error", err)
		}
	})
}

func TestLoadObserverSelectsOperatorConfiguration(t *testing.T) {
	manifest := replace(t,
		`"inputs": [
    { "provider": "loom-sessions", "args": { "window": "previous-day" } }
  ],`,
		`"inputs": [],
  "observer": {"config":"harness-fixture"},`)
	dir := writeAgent(t, t.TempDir(), "daily", manifest, "Observe the configured domain.\n")
	def, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if def.Manifest.Observer == nil || def.Manifest.Observer.Config != "harness-fixture" {
		t.Fatalf("observer = %+v", def.Manifest.Observer)
	}
}

func TestLoadObserverRefusesProviderInputsAndInvalidRegistrationNames(t *testing.T) {
	withObserver := replace(t, `"sandbox": {`, `"observer":{"config":"harness-fixture"},"sandbox": {`)
	dir := writeAgent(t, t.TempDir(), "daily", withObserver, "body\n")
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "collect through their configured collector") {
		t.Fatalf("provider input error = %v", err)
	}
	invalid := strings.Replace(withObserver, `"harness-fixture"`, `"../fixture"`, 1)
	invalid = strings.Replace(invalid,
		`"inputs": [
    { "provider": "loom-sessions", "args": { "window": "previous-day" } }
  ],`,
		`"inputs": [],`, 1)
	dir = writeAgent(t, t.TempDir(), "daily", invalid, "body\n")
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "observer.config") {
		t.Fatalf("registration name error = %v", err)
	}
}

func TestLoadAll(t *testing.T) {
	root := t.TempDir()
	writeAgent(t, root, "daily", validManifest, "body\n")
	writeAgent(t, root, "broken", replace(t, `"name": "daily",`, `"name": "broken", "recipients": [],`), "body\n")
	writeAgent(t, root, ".hidden", validManifest, "body\n")

	defs, err := LoadAll(root)
	if err == nil {
		t.Fatal("LoadAll returned no error, want one for the broken agent")
	}
	if !strings.Contains(err.Error(), "broken") || !strings.Contains(err.Error(), "recipients") {
		t.Errorf("error %q does not name the agent and the offending field", err)
	}
	if len(defs) != 1 || defs[0].Name != "daily" {
		t.Fatalf("defs = %v, want the one valid agent", defs)
	}
}

func TestNextSlot(t *testing.T) {
	def, err := Load(filepath.Join("..", "..", "agents", "daily"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		after time.Time
		want  time.Time
	}{
		{
			name:  "before the slot",
			after: time.Date(2026, 9, 2, 0, 0, 0, 0, loc),
			want:  time.Date(2026, 9, 2, 4, 30, 0, 0, loc),
		},
		{
			name:  "at the slot",
			after: time.Date(2026, 9, 2, 4, 30, 0, 0, loc),
			want:  time.Date(2026, 9, 3, 4, 30, 0, 0, loc),
		},
		{
			// 12:00 UTC is 05:00 in the trigger's zone: the slot is wall clock
			// there, not in the caller's zone.
			name:  "after in another zone",
			after: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
			want:  time.Date(2026, 9, 3, 4, 30, 0, 0, loc),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := def.NextSlot(tc.after)
			if err != nil {
				t.Fatalf("NextSlot: %v", err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("NextSlot = %s, want %s", got, tc.want)
			}
			if got.Location().String() != loc.String() {
				t.Errorf("slot zone = %s, want %s", got.Location(), loc)
			}
		})
	}
}
