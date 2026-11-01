package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/ledger"
	"shuttle/internal/runner"
)

// observerManifest selects the fixture registration by name. It carries no
// inputs and no executable path: those live in config.json.
const observerManifest = `{
  "name": "watch",
  "runtime": {
    "kind": "claude",
    "auth": "api_key",
    "model": "haiku",
    "max_turns": 2,
    "max_budget_usd": 0.05,
    "timeout_seconds": 30,
    "tools": ["Read", "Write"]
  },
  "triggers": [
    { "kind": "cron", "expression": "30 4 * * *", "timezone": "UTC" }
  ],
  "observer": { "config": "fixture" },
  "sandbox": { "kind": "srt", "allowed_domains": [] }
}`

// observerEnv is a complete operator installation for the fixture observer: a
// state root, the agent, config.json naming the domain commands by absolute
// path, a stand-in claude that answers the auth probe and then writes a silent
// observer output, and a stand-in srt that execs its argv. The domain commands
// record the directory and environment they ran with under domainCalls so the
// test can show neither came from this process.
type observerEnv struct {
	domainRoot  string
	domainCalls string
}

func newObserverEnv(t *testing.T) *observerEnv {
	t.Helper()
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "watch", observerManifest)
	// /tmp rather than t.TempDir(): the run directory has to fit srt's unix
	// socket path limit, which the runner checks even against a stand-in srt.
	// os.TempDir() is no better: on macOS it is 49–57 bytes before the run
	// directory starts, which leaves the 103-byte limit no room.
	root, err := os.MkdirTemp("/tmp", "shuttle-observer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	t.Setenv("SHUTTLE_HOME", root)

	domainRoot := filepath.Join(root, "domain")
	if err := os.MkdirAll(domainRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	domainCalls := filepath.Join(root, "domain-calls")
	writeScript(t, filepath.Join(root, "collector"), fmt.Sprintf(`#!/bin/sh
cat >/dev/null
printf 'collector %%s [%%s]\n' "$(pwd -P)" "$SHUTTLE_PROJECT_SECRET" >> %q
printf '%%s\n' '{"status":"empty","evidence":[],"exploration":"Nothing moved in the window."}'
`, domainCalls))
	writeScript(t, filepath.Join(root, "notebook"), fmt.Sprintf(`#!/bin/sh
cat >/dev/null
printf 'notebook %%s [%%s]\n' "$(pwd -P)" "$SHUTTLE_PROJECT_SECRET" >> %q
printf '%%s\n' '{"revision":0}'
`, domainCalls))
	definition := filepath.Join(root, "definition.json")
	if err := os.WriteFile(definition, []byte(`{
  "contract_version": "observer-run/v1",
  "revision": "fixture-r1",
  "identity": {"domain": "harness", "observer": "cli"},
  "questions": [],
  "instructions": "Report only what the retained evidence supports."
}`), 0o600); err != nil {
		t.Fatal(err)
	}

	calls := filepath.Join(root, "model-calls")
	writeScript(t, filepath.Join(root, "claude"), fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then exit 0; fi
cat >/dev/null
printf 'call\n' >> %q
if [ "$(wc -l < %q)" -eq 1 ]; then
  printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.001}'
  exit 0
fi
mkdir -p out
printf '%%s\n' '{"silent":true,"assessments":[],"actions":[]}' > out/observer.json
printf '%%s\n' '{"type":"system","subtype":"init","session_id":"observer-cli"}'
printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"session_id":"observer-cli","num_turns":1,"total_cost_usd":0.002}'
`, calls, calls))
	writeScript(t, filepath.Join(root, "srt"), `#!/bin/sh
if [ "$3" = "-c" ]; then exit 0; fi
shift 3
exec "$@"
`)
	writeConfig(t, fmt.Sprintf(`{
  "anthropic": {"keychain_item": "model"},
  "paths": {"claude_bin": %q, "srt_bin": %q},
  "observers": {
    "fixture": {
      "definition": %q,
      "collector": {"argv": [%q], "working_dir": %q},
      "notebook": {"argv": [%q], "working_dir": %q},
      "data_policy": "cloud",
      "deny_read": [%q],
      "max_model_input_bytes": 8192,
      "timeout_seconds": 10
    }
  }
}`, filepath.Join(root, "claude"), filepath.Join(root, "srt"), definition,
		filepath.Join(root, "collector"), domainRoot,
		filepath.Join(root, "notebook"), domainRoot,
		filepath.Join(domainRoot, "store")))

	// The real pipeline constructor, with only the Keychain lookup replaced:
	// the credential is host state this test cannot provision.
	original := buildPipeline
	buildPipeline = func(store *ledger.Store, logger *log.Logger) (claimedPipeline, error) {
		built, err := newPipeline(store, logger)
		if err != nil {
			return nil, err
		}
		built.(*runner.Pipeline).FindKey = func(string) (string, error) { return "fixture-secret", nil }
		return built, nil
	}
	t.Cleanup(func() { buildPipeline = original })

	// Neither entry point may lean on the shell's cwd or its environment: the
	// process runs from an unrelated directory carrying a variable that must
	// not reach a domain command.
	t.Chdir(t.TempDir())
	t.Setenv("SHUTTLE_PROJECT_SECRET", "must-not-cross")
	return &observerEnv{domainRoot: domainRoot, domainCalls: domainCalls}
}

func writeScript(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
}

// assertObserverRun checks the retained record of one completed observer run:
// the ledger row, the artifacts the run keeps, and that both domain commands
// ran from their configured directory with a built environment.
func (e *observerEnv) assertObserverRun(t *testing.T, run *ledger.Run) {
	t.Helper()
	if run.Status != ledger.StatusCompleted || !run.Silent || !run.RunDir.Valid {
		t.Fatalf("row = %+v, want a completed silent run with a directory", run)
	}
	for _, name := range []string{
		"run.json", "prompt.md", "settings.json", "observer-result.json", "applied.json",
		filepath.Join("observer", "definition.json"),
		filepath.Join("observer", "notebook.json"),
		filepath.Join("observer", "evidence.json"),
		filepath.Join("observer", "validated.json"),
		filepath.Join("inputs", "observer.json"),
	} {
		if _, err := os.Stat(filepath.Join(run.RunDir.String, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	result, err := os.ReadFile(filepath.Join(run.RunDir.String, "observer-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var outcome struct {
		Collection string `json:"collection"`
		Notebook   string `json:"notebook_status"`
		Silent     bool   `json:"silent"`
	}
	if err := json.Unmarshal(result, &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.Collection != "empty" || outcome.Notebook != "applied" || !outcome.Silent {
		t.Fatalf("observer-result.json = %s", result)
	}

	calls, err := os.ReadFile(e.domainCalls)
	if err != nil {
		t.Fatal(err)
	}
	wantDir, err := filepath.EvalSymlinks(e.domainRoot)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
	seen := map[string]bool{}
	for _, line := range lines {
		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 || fields[1] != wantDir || fields[2] != "[]" {
			t.Fatalf("domain command ran with %q, want cwd %s and no inherited environment", line, wantDir)
		}
		seen[fields[0]] = true
	}
	if !seen["collector"] || !seen["notebook"] {
		t.Fatalf("domain calls = %q, want both the collector and the notebook", calls)
	}
}

func TestRunExecutesAConfiguredObserverThroughTheManualEntryPoint(t *testing.T) {
	env := newObserverEnv(t)
	rootCmd.SetArgs([]string{"run", "watch", "--slot", yesterday()})
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		runSlot = ""
	})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("run: %v", err)
	}
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runs, err := store.Runs("watch", 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = (%d rows, %v), want the manual claim", len(runs), err)
	}
	if runs[0].TriggerKind != ledger.TriggerManual {
		t.Fatalf("trigger = %s, want manual", runs[0].TriggerKind)
	}
	env.assertObserverRun(t, runs[0])
}

func TestDaemonExecutesAConfiguredObserverThroughTheScheduledEntryPoint(t *testing.T) {
	env := newObserverEnv(t)
	// A completed slot two days back gives the first tick a catch-up basis
	// (§5): the newest slot since then is claimed and launched.
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	seed, ok, err := store.Claim("watch", time.Now().Add(-48*time.Hour), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("seed claim = (%t, %v)", ok, err)
	}
	if err := store.Complete(seed.ID, ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	store.Close()

	var logs bytes.Buffer
	daemonCmd.SetErr(&logs)
	ctx, cancel := context.WithCancel(context.Background())
	daemonCmd.SetContext(ctx)
	t.Cleanup(func() {
		cancel()
		daemonCmd.SetErr(nil)
		daemonCmd.SetContext(context.Background())
	})
	// The loop leaves only on ctx.Done, after waiting for its workers; cancel
	// once the tick's claim has reached a terminal state.
	go func() {
		defer cancel()
		watcher, err := openLedger()
		if err != nil {
			return
		}
		defer watcher.Close()
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			runs, err := watcher.Runs("watch", 5)
			if err == nil {
				for _, run := range runs {
					if run.ID != seed.ID && run.TriggerKind == ledger.TriggerCron &&
						(run.Status == ledger.StatusCompleted || run.Status == ledger.StatusFailed) {
						return
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	if err := daemonCmd.RunE(daemonCmd, nil); err != nil {
		t.Fatalf("daemon: %v\n%s", err, logs.String())
	}
	if !strings.Contains(logs.String(), "agent=watch slot=") || !strings.Contains(logs.String(), "action=claim run=") {
		t.Fatalf("the first tick did not claim the observer's slot, log:\n%s", logs.String())
	}

	store, err = openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runs, err := store.Runs("watch", 5)
	if err != nil {
		t.Fatal(err)
	}
	// Catch-up-one (§5): the older slot since the seed is recorded as missed
	// and only the newest is claimed and launched.
	var claimed *ledger.Run
	for _, run := range runs {
		if run.ID != seed.ID && run.TriggerKind == ledger.TriggerCron && run.Status != ledger.StatusSkipped {
			if claimed != nil {
				t.Fatalf("two launched cron runs %s and %s, want one", claimed.ID, run.ID)
			}
			claimed = run
		}
	}
	if claimed == nil {
		t.Fatalf("runs = %v, want the tick's cron claim, log:\n%s", runs, logs.String())
	}
	env.assertObserverRun(t, claimed)
}
