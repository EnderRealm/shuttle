package cmd

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// dailyManifest is the smallest §4.1-valid agent.json. These tests only need it
// to load; agentdef's own tests cover the schema.
const dailyManifest = `{
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
  "sandbox": { "kind": "srt", "allowed_domains": [] }
}`

// shuttleEnv points this process at a temp state root and a temp agents dir,
// the two overrides §13's launchd plist carries, and returns the agents dir.
func shuttleEnv(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	agents := t.TempDir()
	t.Setenv("SHUTTLE_HOME", home)
	t.Setenv("SHUTTLE_AGENTS_DIR", agents)
	return agents
}

func writeAgentDir(t *testing.T, agents, name, manifest string) {
	t.Helper()
	dir := filepath.Join(agents, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "agent.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "AGENT.md"), []byte("body\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeConfig(t *testing.T, body string) {
	t.Helper()
	home := os.Getenv("SHUTTLE_HOME")
	if err := os.WriteFile(filepath.Join(home, "config.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestNewSchedulerWithoutConfig(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	// A checkout before `shuttle install` has written config.json: agents_dir
	// has its own default and the limits are simply unset.
	store, err := openLedger()
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer store.Close()
	var logs bytes.Buffer
	sched, err := newScheduler(store, log.New(&logs, "", 0))
	if err != nil {
		t.Fatalf("newScheduler: %v", err)
	}
	if len(sched.Defs) != 1 || sched.Defs[0].Name != "daily" {
		t.Fatalf("defs = %v, want the one agent", sched.Defs)
	}
	if sched.Loc != time.Local {
		t.Fatalf("zone = %v, want the host zone", sched.Loc)
	}
	if sched.MaxRunsPerDay != 0 || sched.MaxSpendUSD != 0 {
		t.Fatalf("limits = (%d, %v), want them unset", sched.MaxRunsPerDay, sched.MaxSpendUSD)
	}
	if logs.Len() != 0 {
		t.Fatalf("newScheduler logged over a clean load:\n%s", logs.String())
	}
}

func TestNewSchedulerRejectsABadConfigTimezone(t *testing.T) {
	shuttleEnv(t)
	// The zone is the day boundary the daily limits are measured from, so an
	// unresolvable one is fatal rather than silently the host's.
	writeConfig(t, `{"timezone": "Mars/Olympus"}`)
	store, err := openLedger()
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer store.Close()
	_, err = newScheduler(store, log.New(&bytes.Buffer{}, "", 0))
	if err == nil || !strings.Contains(err.Error(), "config timezone") {
		t.Fatalf("err = %v, want one naming the config timezone", err)
	}
}

func TestNewSchedulerCarriesConfigAndReportsABrokenDefinition(t *testing.T) {
	agents := shuttleEnv(t)
	writeConfig(t, `{"timezone": "America/Los_Angeles",
		"limits": {"daily_runs_per_agent": 3, "daily_spend_usd": 5}}`)
	writeAgentDir(t, agents, "daily", dailyManifest)
	writeAgentDir(t, agents, "broken", `{"name": "broken"`)
	store, err := openLedger()
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer store.Close()
	var logs bytes.Buffer
	sched, err := newScheduler(store, log.New(&logs, "", 0))
	// One broken manifest must not stop the estate: it is reported and the
	// agents that did load still run.
	if err != nil {
		t.Fatalf("newScheduler: %v", err)
	}
	if len(sched.Defs) != 1 || sched.Defs[0].Name != "daily" {
		t.Fatalf("defs = %v, want only the agent that loaded", sched.Defs)
	}
	if !strings.Contains(logs.String(), "broken") {
		t.Fatalf("newScheduler logged %q, want the broken definition named", logs.String())
	}
	if sched.MaxRunsPerDay != 3 || sched.MaxSpendUSD != 5 {
		t.Fatalf("limits = (%d, %v), want (3, 5) from config.json", sched.MaxRunsPerDay, sched.MaxSpendUSD)
	}
	if sched.Loc.String() != "America/Los_Angeles" {
		t.Fatalf("zone = %v, want the configured one", sched.Loc)
	}
}
