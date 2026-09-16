package cmd

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
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

func TestInstallDaemonPersistsAndInstallsTheLaunchAgent(t *testing.T) {
	shuttleEnv(t)
	binary := filepath.Join(t.TempDir(), "shuttle")
	if err := os.WriteFile(binary, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	originalExecutable := currentExecutable
	originalInstall, originalAwait := installLaunchAgent, awaitLaunchAgent
	currentExecutable = func() (string, error) { return binary, nil }
	var installed []launchd.Spec
	installLaunchAgent = func(spec launchd.Spec) error {
		installed = append(installed, spec)
		return nil
	}
	var awaited [][2]string
	awaitLaunchAgent = func(label, program string) error {
		awaited = append(awaited, [2]string{label, program})
		return nil
	}
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		installLaunchAgent, awaitLaunchAgent = originalInstall, originalAwait
	})

	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"install", "daemon"})
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetArgs(nil)
	})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("install daemon: %v", err)
	}
	resolvedBinary, err := filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	if len(installed) != 2 ||
		installed[0].Label != launchd.DaemonLabel ||
		installed[1].Label != launchd.WatchdogLabel ||
		installed[0].Program != resolvedBinary ||
		installed[1].Program != resolvedBinary {
		t.Fatalf("installed specs = %+v", installed)
	}
	wantAwaited := [][2]string{{launchd.DaemonLabel, resolvedBinary}, {launchd.WatchdogLabel, resolvedBinary}}
	if !reflect.DeepEqual(awaited, wantAwaited) {
		t.Fatalf("awaited = %v", awaited)
	}
	for _, label := range []string{launchd.DaemonLabel, launchd.WatchdogLabel} {
		if !strings.Contains(out.String(), label) {
			t.Fatalf("output = %q, want %s", out.String(), label)
		}
	}
	home, err := config.Home()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(home, "daemon-env"))
	if err != nil {
		t.Fatalf("read daemon-env: %v", err)
	}
	for _, want := range []string{config.EnvHome + "=" + home, config.EnvAgentsDir + "=" + os.Getenv(config.EnvAgentsDir), "PATH="} {
		if !strings.Contains(string(data), want) {
			t.Errorf("daemon-env = %q, want %q", data, want)
		}
	}
}

func TestInstallDaemonFailsWhenTheProcessDoesNotStart(t *testing.T) {
	shuttleEnv(t)
	originalExecutable := currentExecutable
	originalInstall, originalAwait := installLaunchAgent, awaitLaunchAgent
	currentExecutable = func() (string, error) { return "/bin/shuttle", nil }
	installLaunchAgent = func(launchd.Spec) error { return nil }
	awaitLaunchAgent = func(label, program string) error { return errors.New("no process") }
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		installLaunchAgent, awaitLaunchAgent = originalInstall, originalAwait
		rootCmd.SetArgs(nil)
	})
	rootCmd.SetArgs([]string{"install", "daemon"})
	err := rootCmd.Execute()
	if err == nil || !strings.Contains(err.Error(), launchd.DaemonLabel) || !strings.Contains(err.Error(), "no process") {
		t.Fatalf("install daemon error = %v", err)
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
