package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"shuttle/internal/ledger"
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

func TestDaemonReapsInFlightRunsBeforeTheFirstTick(t *testing.T) {
	// The pass only signals a pid whose identity it can read from the kernel
	// (kern.proc.pid, kern.procargs2, kern.proc.pgrp), which is darwin only;
	// everywhere else the lookups establish nothing, so it kills nothing and
	// leaves the row in flight. This one runs at the production grace period,
	// since the daemon owns it.
	if runtime.GOOS != "darwin" {
		t.Skip("process identity is readable on darwin only")
	}
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	// A run the previous daemon left behind: the row is running and its
	// process is alive, in its own group as the runtime launches one and
	// carrying --settings <run_dir>/settings.json as the runtime puts it there
	// (§7.4), which is what the pass identifies it by. The script is more than one
	// command so the shell does not exec over itself and drop that argv.
	runDir := t.TempDir()
	claimedAt := time.Now()
	child := exec.Command("sh", "-c", "sleep 300 & wait",
		"sh", "--settings", filepath.Join(runDir, "settings.json"))
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := child.Process.Pid
	// Captured before the reaping goroutine can free the pid: once the pass
	// kills this process the pid is available again, so cleanup has to recheck
	// the identity the leader started with rather than that something is alive
	// on that pid.
	start, startErr := procStart(pid)
	// Reaped in the background: the real target is an orphan reaped by init,
	// while this one is this process's own child and would stay a zombie.
	go child.Wait()
	// The whole group, or a descendant that ignored the pass's SIGTERM, outlives
	// the test — but only while the leader this test started still holds the pid.
	defer func() {
		if startErr != nil {
			t.Logf("start time of pid %d is unreadable (%v); killing nothing", pid, startErr)
			return
		}
		if now, err := procStart(pid); err == nil && now.Equal(start) {
			syscall.Kill(-pid, syscall.SIGKILL)
		}
	}()

	store, err := openLedger()
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	// Stamped from before the launch, as production's is: the pass only
	// establishes a process that started after the run was claimed.
	store.SetClock(func() time.Time { return claimedAt })
	// A slot in the past, so the tick after the pass has one to fire.
	run, ok, err := store.Claim("daily", time.Now().Add(-48*time.Hour), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim = (%t, %v), want the row", ok, err)
	}
	if err := store.MarkRunning(run.ID, pid, runDir); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	store.Close()

	var logs bytes.Buffer
	daemonCmd.SetErr(&logs)
	t.Cleanup(func() {
		daemonCmd.SetErr(nil)
		daemonCmd.SetContext(context.Background())
	})
	// Cancelled up front: the loop runs one tick and returns on ctx.Done.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	daemonCmd.SetContext(ctx)
	if err := daemonCmd.RunE(daemonCmd, nil); err != nil {
		t.Fatalf("daemon: %v", err)
	}

	reaped := strings.Index(logs.String(), fmt.Sprintf("restart: run=%s agent=daily pid=%d action=killed", run.ID, pid))
	ticked := strings.Index(logs.String(), "agent=daily slot=")
	if reaped < 0 || ticked < 0 || reaped > ticked {
		t.Fatalf("want the run reaped before the first tick, log:\n%s", logs.String())
	}
	// The tick claimed rather than skipping on overlap, which is the point of
	// the pass running first: the stale row is no longer in flight.
	if !strings.Contains(logs.String(), "action=claim run=") {
		t.Fatalf("the first tick did not claim, log:\n%s", logs.String())
	}
	// Polled: the killed process can still be an unreaped zombie for a moment
	// after the signal.
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("pid %d survived the restart pass", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}

	store, err = openLedger()
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	defer store.Close()
	got, err := store.Get(run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != ledger.StatusUnknown {
		t.Fatalf("status = %s, want %s", got.Status, ledger.StatusUnknown)
	}
	// The daemon never resumed it, and the run it claimed on the tick has no
	// runner behind it yet (§7), so the only row left in flight is that one.
	rows, err := store.InFlight()
	if err != nil {
		t.Fatalf("in flight: %v", err)
	}
	for _, row := range rows {
		if row.ID == run.ID {
			t.Fatalf("run %s is in flight again after the pass", run.ID)
		}
	}
}
