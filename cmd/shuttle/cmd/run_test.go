package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
	"shuttle/internal/runner"
)

type pipelineFunc func(context.Context, *agentdef.Definition, *ledger.Run, runner.Options) (*runner.RunResult, error)

func (f pipelineFunc) Run(ctx context.Context, def *agentdef.Definition, run *ledger.Run, opts runner.Options) (*runner.RunResult, error) {
	return f(ctx, def, run, opts)
}

func stubCompletedPipeline(t *testing.T) {
	stubPipeline(t, nil)
}

func stubPipeline(t *testing.T, observe func(*ledger.Run, runner.Options)) {
	t.Helper()
	original := buildPipeline
	buildPipeline = func(store *ledger.Store, _ *log.Logger) (claimedPipeline, error) {
		return pipelineFunc(func(_ context.Context, _ *agentdef.Definition, run *ledger.Run, opts runner.Options) (*runner.RunResult, error) {
			if observe != nil {
				observe(run, opts)
			}
			if err := store.Complete(run.ID, ledger.Result{}); err != nil {
				return nil, err
			}
			return &runner.RunResult{RunID: run.ID, RunDir: "/retained/" + run.ID}, nil
		}), nil
	}
	t.Cleanup(func() { buildPipeline = original })
}

// execRun drives the command through cobra rather than calling RunE directly,
// so the flag names and their bindings are exercised too. The flag variables
// are package globals cobra does not reset between executions, so they are
// restored here along with the argv and the error writer.
func execRun(t *testing.T, args ...string) (*bytes.Buffer, error) {
	t.Helper()
	var warnings bytes.Buffer
	rootCmd.SetErr(&warnings)
	rootCmd.SetArgs(append([]string{"run", "daily", "--slot", yesterday()}, args...))
	t.Cleanup(func() {
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
		runUnsafeNoSandbox = false
		runNoDeliver = false
		runDryRun = false
		runSlot = ""
	})
	return &warnings, rootCmd.Execute()
}

// yesterday is the day every run test claims a slot for: a slot in the future
// is refused before the claim (§5).
func yesterday() string { return time.Now().AddDate(0, 0, -1).Format(dateLayout) }

const dateLayout = "2006-01-02"

// The flag is the only path out of the §11 sandbox, so a run that took it says
// so and names the run, which is what ties the warning to the ledger row.
func TestRunWarnsWhenTheSandboxIsBypassed(t *testing.T) {
	stubCompletedPipeline(t)
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	warnings, err := execRun(t, "--unsafe-no-sandbox")
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	store, err := openLedger()
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer store.Close()
	runs, err := store.Runs("daily", 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = (%d rows, %v), want the claimed one", len(runs), err)
	}
	want := "warning: --unsafe-no-sandbox: run " + runs[0].ID + " is exempt from the srt sandbox\n"
	if warnings.String() != want {
		t.Fatalf("stderr = %q, want %q", warnings.String(), want)
	}
}

// The operator is told why and the ledger keeps nothing: a skipped row would
// spend the slot on a refusal that lasts minutes.
func TestRunDuringAnUpdateRestartIsRefusedAndRecordsNothing(t *testing.T) {
	stubCompletedPipeline(t)
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	store, err := openLedger()
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer store.Close()
	if claimed, _, err := store.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("pause = %t/%v", claimed, err)
	}
	if err := store.ArmRestartBarrier(time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := execRun(t); !errors.Is(err, ledger.ErrRestarting) || !strings.Contains(err.Error(), "update") {
		t.Fatalf("run = %v", err)
	}
	if runs, err := store.Runs("daily", 1); err != nil || len(runs) != 0 {
		t.Fatalf("runs = %+v/%v", runs, err)
	}
}

// Without the flag there is nothing to warn about: the sandbox is the default
// and no other input can turn it off.
func TestRunIsSilentWithoutTheFlag(t *testing.T) {
	stubCompletedPipeline(t)
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	warnings, err := execRun(t)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.Contains(warnings.String(), "unsafe-no-sandbox") {
		t.Fatalf("stderr = %q, want no warning", warnings.String())
	}
}

func TestRunPassesTheManualExecutionOptionsToTheSharedPipeline(t *testing.T) {
	var got runner.Options
	stubPipeline(t, func(run *ledger.Run, opts runner.Options) {
		if run.TriggerKind != ledger.TriggerManual {
			t.Errorf("trigger = %s, want manual", run.TriggerKind)
		}
		got = opts
	})
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	if _, err := execRun(t, "--unsafe-no-sandbox", "--no-deliver"); err != nil {
		t.Fatal(err)
	}
	if !got.UnsafeNoSandbox || !got.NoDeliver {
		t.Fatalf("options = %+v, want both explicit flags", got)
	}
}

func TestManualRunCancellationSettlesTheClaim(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)

	originalContext := manualRunContext
	manualRunContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { manualRunContext = originalContext })

	originalPipeline := buildPipeline
	var cancelled bool
	buildPipeline = func(store *ledger.Store, _ *log.Logger) (claimedPipeline, error) {
		return pipelineFunc(func(ctx context.Context, _ *agentdef.Definition, run *ledger.Run, _ runner.Options) (*runner.RunResult, error) {
			cancelled = errors.Is(ctx.Err(), context.Canceled)
			err := errors.New("manual run interrupted")
			if failErr := store.Fail(run.ID, ledger.ClassStuck, err.Error(), ledger.Result{}); failErr != nil {
				return nil, failErr
			}
			return nil, err
		}), nil
	}
	t.Cleanup(func() { buildPipeline = originalPipeline })

	if _, err := execRun(t); err == nil || !strings.Contains(err.Error(), "manual run interrupted") {
		t.Fatalf("run error = %v", err)
	}
	if !cancelled {
		t.Fatal("the shared pipeline did not receive the cancelled manual-run context")
	}
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runs, err := store.Runs("daily", 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %v, %v", runs, err)
	}
	if runs[0].Status != ledger.StatusFailed || runs[0].FailureClass.String != ledger.ClassStuck {
		t.Fatalf("row = %+v", runs[0])
	}
}

// dryRunManifest is the daily agent's manifest: the six §8 inputs are what a
// dry run has to gather.
const dryRunManifest = `{
  "name": "daily",
  "runtime": {
    "kind": "claude",
    "auth": "api_key",
    "model": "sonnet",
    "max_turns": 1,
    "max_budget_usd": 0.5,
    "timeout_seconds": 60,
    "tools": ["Read"]
  },
  "triggers": [
    { "kind": "cron", "expression": "30 4 * * *", "timezone": "America/Los_Angeles" }
  ],
  "inputs": [
    { "provider": "loom-sessions",   "args": { "window": "previous-day" } },
    { "provider": "loom-workreport", "args": { "window": "previous-day" } },
    { "provider": "tk-journal",      "args": { "window": "previous-day" } },
    { "provider": "tk-tickets",      "args": { "window": "previous-day" } },
    { "provider": "git-log",         "args": { "window": "previous-day" } },
    { "provider": "prior-reports",   "args": { "agent": "daily", "count": 1 } }
  ],
  "sandbox": { "kind": "srt", "allowed_domains": [] }
}`

// dryRunEnv points config.json's §4.2 paths at temp trees: an empty loom home
// (so loom-sessions reports unavailable), a stand-in loom binary, and a tk
// config registering one checkout. loomBin is what the caller varies.
func dryRunEnv(t *testing.T, loomBin string) {
	t.Helper()
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dryRunManifest)

	tkDir := filepath.Join(t.TempDir(), ".ticket")
	if err := os.MkdirAll(tkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	if out, err := exec.Command("git", "-C", repo, "init").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	tkConfig := filepath.Join(tkDir, "config.yaml")
	body := "central_root: " + filepath.Join(tkDir, "store") + "\nprojects:\n    shuttle:\n        path: " + repo + "\n"
	if err := os.WriteFile(tkConfig, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	writeConfig(t, fmt.Sprintf(`{"paths": {
		"loom_home": %q,
		"tk_config": %q,
		"claude_bin": "/nonexistent/claude",
		"srt_bin": "/nonexistent/srt",
		"loom_bin": %q
	}}`, t.TempDir(), tkConfig, loomBin))
}

// fakeLoom stands in for the loom binary: `loom work-report` on one JSON object.
func fakeLoom(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "loom")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho '{\"runs\":[],\"totals\":{}}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// The §15 acceptance for the providers: six documents and a prompt under 40 KB,
// with no model call and no ledger row.
func TestRunDryRunWritesSixDocumentsAndAPrompt(t *testing.T) {
	dryRunEnv(t, fakeLoom(t))
	if _, err := execRun(t, "--dry-run"); err != nil {
		t.Fatalf("run --dry-run: %v", err)
	}

	dirs, err := filepath.Glob(filepath.Join(os.Getenv("SHUTTLE_HOME"), "runs", "daily", "*"))
	if err != nil || len(dirs) != 1 {
		t.Fatalf("run dirs = %v (%v), want one", dirs, err)
	}
	inputs, err := filepath.Glob(filepath.Join(dirs[0], "inputs", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 6 {
		t.Fatalf("inputs = %v, want the six §8 documents", inputs)
	}
	for _, path := range inputs {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Provider    string          `json:"provider"`
			Window      map[string]any  `json:"window"`
			GeneratedAt string          `json:"generated_at"`
			Data        json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if doc.Provider+".json" != filepath.Base(path) || doc.GeneratedAt == "" || len(doc.Data) == 0 {
			t.Errorf("%s: envelope = %+v, want the §8 shape", path, doc)
		}
		// Every document reports the same day, resolved from the slot rather
		// than from the clock (§8.1).
		if doc.Window["label"] != dayBefore(t, yesterday()) {
			t.Errorf("%s: label = %v, want the day before the slot", path, doc.Window["label"])
		}
	}

	prompt, err := os.ReadFile(filepath.Join(dirs[0], "prompt.md"))
	if err != nil {
		t.Fatal(err)
	}
	if len(prompt) >= 40*1024 {
		t.Errorf("prompt.md = %d bytes, want under 40 KB", len(prompt))
	}
	if !strings.Contains(string(prompt), "## Run contract") || !strings.Contains(string(prompt), filepath.Base(dirs[0])) {
		t.Errorf("prompt.md does not carry the §7.3 contract:\n%s", prompt)
	}

	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runs, err := store.Runs("daily", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %d rows, want none: a dry run claims nothing", len(runs))
	}
}

func dayBefore(t *testing.T, date string) string {
	t.Helper()
	day, err := time.Parse(dateLayout, date)
	if err != nil {
		t.Fatal(err)
	}
	return day.AddDate(0, 0, -1).Format(dateLayout)
}

// A provider failure carries its class and its name, which is what the ledger
// row of a live run would say (§7.6).
func TestRunDryRunNamesTheFailingProvider(t *testing.T) {
	dryRunEnv(t, filepath.Join(t.TempDir(), "loom"))
	_, err := execRun(t, "--dry-run")
	if err == nil {
		t.Fatal("run --dry-run over a missing loom: want an error")
	}
	if !strings.Contains(err.Error(), "class provider") || !strings.Contains(err.Error(), "loom-workreport") {
		t.Fatalf("err = %v, want the class and the provider named", err)
	}
}

// The providers read the paths in config.json, so a dry run without it says so
// rather than gathering from defaults.
func TestRunDryRunRequiresConfig(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dryRunManifest)
	_, err := execRun(t, "--dry-run")
	if err == nil || !strings.Contains(err.Error(), "config.json") {
		t.Fatalf("err = %v, want one naming config.json", err)
	}
}

// The execution path, not openLedger, carries the notification policy: the
// store newPipeline is handed honours config.json's cooldown and enablement on
// committed failures and recoveries, and a notifier error leaves the terminal
// row as written.
func TestNewPipelineAttachesTheConfiguredNotifications(t *testing.T) {
	shuttleEnv(t)
	writeConfig(t, `{"notify": {"enabled": true, "cooldown_minutes": 60}}`)
	statePath := filepath.Join(os.Getenv("SHUTTLE_HOME"), "notify.state")
	// The failure below is inside its cooldown, so no notification is sent;
	// without the configured cooldown it would be.
	signature := ledger.Signature(ledger.ClassDied, "exit 1")
	state, err := json.Marshal(map[string]time.Time{"daily\x00incident\x00" + signature: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, state, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var logs bytes.Buffer
	if _, err := newPipeline(store, log.New(&logs, "", 0)); err != nil {
		t.Fatal(err)
	}
	claim := func(at time.Time) string {
		t.Helper()
		r, ok, err := store.Claim("daily", at, ledger.TriggerCron)
		if err != nil || !ok {
			t.Fatalf("claim: %v", err)
		}
		return r.ID
	}
	failed := claim(time.Now().Add(-2 * time.Hour))
	if err := store.Fail(failed, ledger.ClassDied, "exit 1", ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	if want := "run=" + failed + " agent=daily notification=false"; !strings.Contains(logs.String(), want) ||
		!strings.Contains(logs.String(), "error=<nil>") {
		t.Fatalf("failure inside the cooldown: log = %q", &logs)
	}
	// A disabled notifier never reads its state, so an unreadable one
	// surfacing as the recovery's error shows enablement came from config.
	if err := os.WriteFile(statePath, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered := claim(time.Now().Add(-time.Hour))
	if err := store.Complete(recovered, ledger.Result{}); err != nil {
		t.Fatalf("a notification failure changed the terminal result: %v", err)
	}
	if want := "run=" + recovered + " agent=daily notification=false"; !strings.Contains(logs.String(), want) ||
		!strings.Contains(logs.String(), "recovery=true error=notify state") {
		t.Fatalf("recovery with a broken notifier: log = %q", &logs)
	}
	row, err := store.Get(recovered)
	if err != nil || row.Status != ledger.StatusCompleted {
		t.Fatalf("recovered row = %+v, %v", row, err)
	}
}
