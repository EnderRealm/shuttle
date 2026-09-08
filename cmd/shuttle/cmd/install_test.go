package cmd

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"shuttle/internal/config"
)

// install resolves its paths through config.Home(), so SHUTTLE_HOME keeps the
// test off the real state root.
func TestInstallCreatesTheReportsRepo(t *testing.T) {
	shuttleEnv(t)
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"install"})
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetArgs(nil)
	})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("install: %v", err)
	}

	home, err := config.Home()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(home); err != nil {
		t.Errorf("state root: %v", err)
	}
	reports, err := config.ReportsDir()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), reports) {
		t.Errorf("install printed %q, want it to name %s", out.String(), reports)
	}
	if got := git(t, reports, "rev-parse", "--is-inside-work-tree"); got != "true" {
		t.Errorf("rev-parse: got %q, want a work tree", got)
	}
	if got := git(t, reports, "remote"); got != "" {
		t.Errorf("remote: got %q, want none", got)
	}
}

// install is what creates the state root now, before any ledger exists, so the
// mode it leaves behind is the mode the ledger and the report archive live
// under (§9.3).
func TestInstallCreatesAPrivateStateRoot(t *testing.T) {
	// A path install has to create itself: t.TempDir() already exists at 0700
	// and would pass whatever mode install chose.
	home := filepath.Join(t.TempDir(), "state")
	t.Setenv("SHUTTLE_HOME", home)
	rootCmd.SetArgs([]string{"install"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("install: %v", err)
	}
	fi, err := os.Stat(home)
	if err != nil {
		t.Fatalf("state root: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Errorf("state root mode = %04o, want 0700", perm)
	}
}

// The help advertises `install daemon`, so the argument has to reach an
// explanation rather than cobra's unknown-command error (§13).
func TestInstallDaemonIsNotBuiltYet(t *testing.T) {
	shuttleEnv(t)
	rootCmd.SetArgs([]string{"install", "daemon"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	err := rootCmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "not built yet") {
		t.Fatalf("install daemon: got %v, want the launchd job named as not built yet", err)
	}
}

func TestInstallRejectsAnUnknownArgument(t *testing.T) {
	shuttleEnv(t)
	rootCmd.SetArgs([]string{"install", "everything"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	err := rootCmd.Execute()
	if err == nil || !strings.Contains(err.Error(), `"everything"`) {
		t.Fatalf("install everything: got %v, want the argument named", err)
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}
