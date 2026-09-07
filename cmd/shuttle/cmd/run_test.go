package cmd

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// execRun drives the command through cobra rather than calling RunE directly,
// so the flag names and their bindings are exercised too. The flag variables
// are package globals cobra does not reset between executions, so they are
// restored here along with the argv and the error writer.
func execRun(t *testing.T, args ...string) (*bytes.Buffer, error) {
	t.Helper()
	var warnings bytes.Buffer
	rootCmd.SetErr(&warnings)
	// Yesterday: a slot in the future is refused before the claim (§5).
	slot := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	rootCmd.SetArgs(append([]string{"run", "daily", "--slot", slot}, args...))
	t.Cleanup(func() {
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
		runUnsafeNoSandbox = false
		runSlot = ""
	})
	return &warnings, rootCmd.Execute()
}

// The flag is the only path out of the §11 sandbox, so a run that took it says
// so and names the run, which is what ties the warning to the ledger row.
func TestRunWarnsWhenTheSandboxIsBypassed(t *testing.T) {
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

// Without the flag there is nothing to warn about: the sandbox is the default
// and no other input can turn it off.
func TestRunIsSilentWithoutTheFlag(t *testing.T) {
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
