package cmd

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
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
	stubUninstall(t, func(string) error { return nil })
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
	if len(installed) != 3 ||
		installed[0].Label != launchd.DaemonLabel ||
		installed[1].Label != launchd.WatchdogLabel ||
		installed[2].Label != launchd.UpdaterLabel ||
		installed[0].Program != resolvedBinary ||
		installed[1].Program != resolvedBinary ||
		installed[2].Program != resolvedBinary {
		t.Fatalf("installed specs = %+v", installed)
	}
	wantAwaited := [][2]string{{launchd.DaemonLabel, resolvedBinary}, {launchd.WatchdogLabel, resolvedBinary}, {launchd.UpdaterLabel, resolvedBinary}}
	if !reflect.DeepEqual(awaited, wantAwaited) {
		t.Fatalf("awaited = %v", awaited)
	}
	for _, label := range []string{launchd.DaemonLabel, launchd.WatchdogLabel, launchd.UpdaterLabel} {
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

// recordInstalls points the launchd seams at a recorder and returns the labels
// installed so far, in order.
func recordInstalls(t *testing.T) func() []string {
	t.Helper()
	// Off the host's LaunchAgents, whose plists pin the installed shuttle and
	// not the binary this stands in.
	t.Setenv("HOME", t.TempDir())
	binary := filepath.Join(t.TempDir(), "shuttle")
	if err := os.WriteFile(binary, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	originalExecutable := currentExecutable
	originalInstall, originalAwait := installLaunchAgent, awaitLaunchAgent
	currentExecutable = func() (string, error) { return binary, nil }
	var labels []string
	installLaunchAgent = func(spec launchd.Spec) error {
		labels = append(labels, spec.Label)
		return nil
	}
	awaitLaunchAgent = func(string, string) error { return nil }
	stubUninstall(t, func(label string) error {
		labels = append(labels, "-"+label)
		return nil
	})
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		installLaunchAgent, awaitLaunchAgent = originalInstall, originalAwait
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
	})
	return func() []string { return labels }
}

// stubUninstall keeps install daemon's removal of a stale export job off the
// host's launchd.
func stubUninstall(t *testing.T, fn func(string) error) {
	t.Helper()
	original := uninstallLaunchAgent
	uninstallLaunchAgent = fn
	t.Cleanup(func() { uninstallLaunchAgent = original })
}

// The export job is installed with the daemon's only where config.json names
// a destination, ahead of them so the updater is still replaced last, and is
// not awaited: between its daily starts it has no process. Where none is
// named, an export job already installed is removed.
func TestInstallDaemonInstallsTheExportWhenADestinationIsSet(t *testing.T) {
	shuttleEnv(t)
	labels := recordInstalls(t)
	var awaited []string
	awaitLaunchAgent = func(label, _ string) error {
		awaited = append(awaited, label)
		return nil
	}
	writeConfig(t, `{"export": {"destination": "/Volumes/shuttle/studio"}}`)
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"install", "daemon"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("install daemon: %v", err)
	}
	want := []string{launchd.ExportLabel, launchd.DaemonLabel, launchd.WatchdogLabel, launchd.UpdaterLabel}
	if got := labels(); !reflect.DeepEqual(got, want) {
		t.Fatalf("installed = %v, want %v", got, want)
	}
	if slices.Contains(awaited, launchd.ExportLabel) {
		t.Fatalf("awaited = %v, want the export job left to its schedule", awaited)
	}
	if !strings.Contains(out.String(), "/Volumes/shuttle/studio") {
		t.Fatalf("output = %q, want the destination named", out.String())
	}

	// Unset again, install converges on the config: the job installed above
	// is removed rather than left to fail every night.
	writeConfig(t, `{}`)
	before := len(labels())
	rootCmd.SetArgs([]string{"install", "daemon"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("install daemon: %v", err)
	}
	want = []string{"-" + launchd.ExportLabel, launchd.DaemonLabel, launchd.WatchdogLabel, launchd.UpdaterLabel}
	if got := labels()[before:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("installed = %v with no destination set, want %v", got, want)
	}
}

func TestInstallUpdaterInstallsOnlyTheUpdater(t *testing.T) {
	shuttleEnv(t)
	labels := recordInstalls(t)
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"install", "updater"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("install updater: %v", err)
	}
	if got := labels(); !reflect.DeepEqual(got, []string{launchd.UpdaterLabel}) {
		t.Fatalf("installed = %v", got)
	}
	if !strings.Contains(out.String(), launchd.UpdaterLabel) {
		t.Fatalf("output = %q", out.String())
	}
}

// The updater waits on this command, so it must leave the updater's own job
// alone: replacing it would kill the cycle waiting for the answer.
// The export job is spawned afresh daily, so it is re-registered under the
// new binary too, following config.json as install daemon does.
func TestUpdaterRebootstrapLeavesTheUpdatersJob(t *testing.T) {
	shuttleEnv(t)
	labels := recordInstalls(t)
	rootCmd.SetArgs([]string{"updater", "rebootstrap"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("updater rebootstrap: %v", err)
	}
	if got := labels(); !reflect.DeepEqual(got, []string{"-" + launchd.ExportLabel, launchd.DaemonLabel, launchd.WatchdogLabel}) {
		t.Fatalf("installed = %v", got)
	}

	writeConfig(t, `{"export": {"destination": "/Volumes/shuttle/studio"}}`)
	before := len(labels())
	rootCmd.SetArgs([]string{"updater", "rebootstrap"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("updater rebootstrap: %v", err)
	}
	if got := labels()[before:]; !reflect.DeepEqual(got, []string{launchd.ExportLabel, launchd.DaemonLabel, launchd.WatchdogLabel}) {
		t.Fatalf("installed = %v with a destination set", got)
	}
}

// Run from a checkout or a `go run` build, the update would install over that
// binary and the restarts after it would re-pin every job to it. Each command
// that acts on the installed jobs refuses before any lock, pause or install,
// and the binary the jobs pin goes through.
func TestUpdaterTargetsThePinnedBinary(t *testing.T) {
	shuttleEnv(t)
	t.Setenv("HOME", t.TempDir())
	writeBinary := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	installedBinary := filepath.Join(t.TempDir(), "bin", "shuttle")
	checkoutBinary := filepath.Join(t.TempDir(), "checkout", "shuttle")
	writeBinary(installedBinary, "installed")
	writeBinary(checkoutBinary, "checkout")
	specs, err := launchd.DaemonSpecs(installedBinary)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range specs {
		path, err := launchd.PlistPath(spec.Label)
		if err != nil {
			t.Fatal(err)
		}
		writeBinary(path, spec.PlistXML())
	}
	daemonPlist, err := launchd.PlistPath(launchd.DaemonLabel)
	if err != nil {
		t.Fatal(err)
	}
	resolvedCheckout, err := filepath.EvalSymlinks(checkoutBinary)
	if err != nil {
		t.Fatal(err)
	}

	originalExecutable := currentExecutable
	originalInstall, originalAwait := installLaunchAgent, awaitLaunchAgent
	running := checkoutBinary
	currentExecutable = func() (string, error) { return running, nil }
	var installed []launchd.Spec
	installLaunchAgent = func(spec launchd.Spec) error {
		installed = append(installed, spec)
		return nil
	}
	awaitLaunchAgent = func(string, string) error { return nil }
	stubUninstall(t, func(string) error { return nil })
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		installLaunchAgent, awaitLaunchAgent = originalInstall, originalAwait
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
	})
	execute := func(args ...string) error {
		var out bytes.Buffer
		rootCmd.SetOut(&out)
		rootCmd.SetErr(&out)
		rootCmd.SetArgs(args)
		return rootCmd.Execute()
	}

	for _, args := range [][]string{
		{"updater", "once"},
		{"updater", "rebootstrap"},
		{"weft", "bind", "git@github.com:EnderRealm/ticket.git"},
		{"weft", "unbind", "ticket"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			installed = nil
			err := execute(args...)
			if err == nil {
				t.Fatal("ran from the checkout, want a refusal")
			}
			for _, want := range []string{resolvedCheckout, installedBinary, daemonPlist} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not name %s", err, want)
				}
			}
			if len(installed) != 0 {
				t.Errorf("re-pinned %d jobs: %+v", len(installed), installed)
			}
			for path, want := range map[string]string{checkoutBinary: "checkout", installedBinary: "installed"} {
				if data, err := os.ReadFile(path); err != nil || string(data) != want {
					t.Errorf("%s = %q (%v), want it untouched", path, data, err)
				}
			}
			store, err := openLedger()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.RequireUpdatePause(); err == nil {
				t.Error("the refusal left the update pause standing")
			}
		})
	}

	t.Run("the pinned binary", func(t *testing.T) {
		installed = nil
		running = installedBinary
		if err := execute("updater", "rebootstrap"); err != nil {
			t.Fatalf("updater rebootstrap: %v", err)
		}
		resolvedInstalled, err := filepath.EvalSymlinks(installedBinary)
		if err != nil {
			t.Fatal(err)
		}
		if len(installed) != 2 || installed[0].Program != resolvedInstalled || installed[1].Program != resolvedInstalled {
			t.Fatalf("installed = %+v, want the daemon and watchdog under %s", installed, resolvedInstalled)
		}
	})
}

func TestInstallDaemonFailsWhenTheProcessDoesNotStart(t *testing.T) {
	shuttleEnv(t)
	originalExecutable := currentExecutable
	originalInstall, originalAwait := installLaunchAgent, awaitLaunchAgent
	currentExecutable = func() (string, error) { return "/bin/shuttle", nil }
	installLaunchAgent = func(launchd.Spec) error { return nil }
	awaitLaunchAgent = func(label, program string) error { return errors.New("no process") }
	stubUninstall(t, func(string) error { return nil })
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
