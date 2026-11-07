package launchd

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"shuttle/internal/config"
)

func TestDaemonSpecPersistsOnlyItsEnvironment(t *testing.T) {
	home := t.TempDir()
	agents := filepath.Join(t.TempDir(), "agents")
	t.Setenv(config.EnvHome, home)
	t.Setenv(config.EnvAgentsDir, agents)

	spec, err := DaemonSpec("/Users/me/.local/bin/shuttle")
	if err != nil {
		t.Fatalf("DaemonSpec: %v", err)
	}
	wantEnv := map[string]string{
		config.EnvHome:      home,
		config.EnvAgentsDir: agents,
		"PATH":              CanonicalPath(),
	}
	if !reflect.DeepEqual(spec.Env, wantEnv) {
		t.Fatalf("env = %#v, want %#v", spec.Env, wantEnv)
	}
	if spec.Label != DaemonLabel || spec.Program != "/Users/me/.local/bin/shuttle" {
		t.Fatalf("spec identity = (%q, %q)", spec.Label, spec.Program)
	}
	if !reflect.DeepEqual(spec.Args, []string{"daemon"}) {
		t.Fatalf("args = %v, want daemon", spec.Args)
	}
	if !spec.RunAtLoad || !spec.KeepAlive || spec.ThrottleIntervalSeconds != 30 {
		t.Fatalf("lifecycle = run-at-load:%t keep-alive:%t throttle:%d",
			spec.RunAtLoad, spec.KeepAlive, spec.ThrottleIntervalSeconds)
	}
	if spec.LogPath != filepath.Join(home, "daemon.log") {
		t.Fatalf("log path = %q", spec.LogPath)
	}

	path := filepath.Join(home, daemonEnvFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read daemon env: %v", err)
	}
	wantFile := config.EnvHome + "=" + home + "\n" +
		config.EnvAgentsDir + "=" + agents + "\n" +
		"PATH=" + CanonicalPath() + "\n"
	if string(data) != wantFile {
		t.Fatalf("daemon-env = %q, want %q", data, wantFile)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("daemon-env mode = %04o, want 0600", info.Mode().Perm())
	}

	xml := spec.PlistXML()
	for _, secret := range []string{"TOKEN", "SECRET", "API_KEY", "ANTHROPIC"} {
		if strings.Contains(strings.ToUpper(xml), secret) {
			t.Fatalf("plist contains secret-shaped key %q:\n%s", secret, xml)
		}
	}
}

func TestDaemonSpecPreservesPersistedAgentsDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv(config.EnvHome, home)
	t.Setenv(config.EnvAgentsDir, "/first/agents")
	first, err := DaemonSpec("/bin/shuttle")
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(config.EnvAgentsDir, "")
	second, err := DaemonSpec("/bin/shuttle")
	if err != nil {
		t.Fatal(err)
	}
	if second.Env[config.EnvAgentsDir] != first.Env[config.EnvAgentsDir] {
		t.Fatalf("agents dir changed from %q to %q without an override",
			first.Env[config.EnvAgentsDir], second.Env[config.EnvAgentsDir])
	}
}

func TestDaemonSpecsShareTheSecretFreeEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv(config.EnvHome, home)
	t.Setenv(config.EnvAgentsDir, filepath.Join(home, "agents"))
	specs, err := DaemonSpecs("/bin/shuttle")
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 3 {
		t.Fatalf("spec count = %d, want 3", len(specs))
	}
	if specs[0].Label != DaemonLabel || specs[1].Label != WatchdogLabel || specs[2].Label != UpdaterLabel {
		t.Fatalf("labels = (%q, %q, %q)", specs[0].Label, specs[1].Label, specs[2].Label)
	}
	if !reflect.DeepEqual(specs[0].Args, []string{"daemon"}) || !reflect.DeepEqual(specs[1].Args, []string{"watchdog"}) ||
		!reflect.DeepEqual(specs[2].Args, []string{"updater", "daemon"}) {
		t.Fatalf("args = (%v, %v, %v)", specs[0].Args, specs[1].Args, specs[2].Args)
	}
	if !reflect.DeepEqual(specs[0].Env, specs[1].Env) {
		t.Fatalf("environments differ: %#v != %#v", specs[0].Env, specs[1].Env)
	}
	updater := specs[2]
	if updater.LogPath != filepath.Join(home, "updater.log") || !updater.KeepAlive || !updater.RunAtLoad ||
		updater.ThrottleIntervalSeconds != 30 {
		t.Fatalf("updater spec = %+v", updater)
	}
	if updater.Env[EnvUpdaterInterval] != "5" {
		t.Fatalf("updater interval = %q, want the default", updater.Env[EnvUpdaterInterval])
	}
	delete(updater.Env, EnvUpdaterInterval)
	if !reflect.DeepEqual(specs[0].Env, updater.Env) {
		t.Fatalf("updater environment = %#v, want the daemon's and the interval", updater.Env)
	}
}

// The updater re-installs its own job from its own environment, so a tuned
// interval has to survive that.
func TestUpdaterSpecKeepsATunedInterval(t *testing.T) {
	home := t.TempDir()
	t.Setenv(config.EnvHome, home)
	t.Setenv(config.EnvAgentsDir, filepath.Join(home, "agents"))
	t.Setenv(EnvUpdaterInterval, "15")
	specs, err := DaemonSpecs("/bin/shuttle")
	if err != nil {
		t.Fatal(err)
	}
	if specs[2].Env[EnvUpdaterInterval] != "15" {
		t.Fatalf("interval = %q", specs[2].Env[EnvUpdaterInterval])
	}
	if _, leaked := specs[0].Env[EnvUpdaterInterval]; leaked {
		t.Fatal("the interval reached the daemon's environment")
	}
}

func TestDaemonPlistLints(t *testing.T) {
	if _, err := exec.LookPath("plutil"); err != nil {
		t.Skip("plutil not available")
	}
	home := t.TempDir()
	t.Setenv(config.EnvHome, home)
	t.Setenv(config.EnvAgentsDir, filepath.Join(home, "agents"))
	specs, err := DaemonSpecs("/Users/Test User/A&B/shuttle")
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range specs {
		path := filepath.Join(t.TempDir(), spec.Label+".plist")
		if err := os.WriteFile(path, []byte(spec.PlistXML()), 0o644); err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command("plutil", "-lint", path).CombinedOutput(); err != nil {
			t.Fatalf("plutil -lint %s: %v: %s\n%s", spec.Label, err, out, spec.PlistXML())
		}
	}
}

func TestInstallRetriesTransientBootstrapAndWritesPlist(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	spec := Spec{
		Label:                   DaemonLabel,
		Program:                 "/bin/shuttle",
		Args:                    []string{"daemon"},
		KeepAlive:               true,
		RunAtLoad:               true,
		ThrottleIntervalSeconds: 30,
	}
	var calls []string
	bootstrapCalls := 0
	originalRun, originalSleep := runCommand, sleep
	runCommand = func(name string, args ...string) ([]byte, error) {
		call := name + " " + strings.Join(args, " ")
		calls = append(calls, call)
		switch {
		case name == "plutil":
			return []byte("OK"), nil
		case len(args) > 0 && args[0] == "bootout":
			return []byte("Could not find service"), errors.New("not loaded")
		case len(args) > 0 && args[0] == "print":
			return []byte("Could not find service"), errors.New("not loaded")
		case len(args) > 0 && args[0] == "bootstrap":
			bootstrapCalls++
			if bootstrapCalls < 3 {
				return []byte("Bootstrap failed: 5: Input/output error"), errors.New("exit 5")
			}
			return nil, nil
		default:
			return nil, nil
		}
	}
	sleep = func(time.Duration) {}
	t.Cleanup(func() { runCommand, sleep = originalRun, originalSleep })

	if err := Install(spec); err != nil {
		t.Fatalf("Install: %v\ncalls: %v", err, calls)
	}
	if bootstrapCalls != 3 {
		t.Fatalf("bootstrap calls = %d, want 3", bootstrapCalls)
	}
	path, err := PlistPath(DaemonLabel)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	if string(data) != spec.PlistXML() {
		t.Fatal("installed plist differs from rendered spec")
	}
}

func TestInstallRemovesAPlistThatFailsLint(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	originalRun := runCommand
	runCommand = func(name string, args ...string) ([]byte, error) {
		return []byte("bad plist"), errors.New("exit 1")
	}
	t.Cleanup(func() { runCommand = originalRun })

	err := Install(Spec{Label: DaemonLabel, Program: "/bin/shuttle"})
	if err == nil || !strings.Contains(err.Error(), "plutil -lint") {
		t.Fatalf("Install error = %v", err)
	}
	path, pathErr := PlistPath(DaemonLabel)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("plist remains after lint failure: %v", statErr)
	}
}

func TestUninstallIsIdempotent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	path, err := PlistPath(DaemonLabel)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("plist"), 0o644); err != nil {
		t.Fatal(err)
	}
	originalRun := runCommand
	runCommand = func(name string, args ...string) ([]byte, error) {
		return []byte("Could not find service"), errors.New("not loaded")
	}
	t.Cleanup(func() { runCommand = originalRun })

	if err := Uninstall(DaemonLabel); err != nil {
		t.Fatalf("first Uninstall: %v", err)
	}
	if err := Uninstall(DaemonLabel); err != nil {
		t.Fatalf("second Uninstall: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("plist still exists: %v", err)
	}
}

func TestAwaitRunningKickstartsAProcesslessJob(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "shuttle")
	if err := os.WriteFile(binary, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(binary)
	if err != nil {
		t.Fatal(err)
	}

	originalProcess, originalKickstart := processFor, kickstart
	originalTimeout, originalInterval, originalKickAfter := awaitTimeout, awaitInterval, kickstartAfter
	originalSleep := sleep
	calls := 0
	processFor = func(label string) (Process, bool, error) {
		calls++
		if calls == 1 {
			return Process{}, false, nil
		}
		return Process{PID: 42, Started: info.ModTime().Add(time.Second)}, true, nil
	}
	kicks := 0
	kickstart = func(label string) error {
		kicks++
		return nil
	}
	awaitTimeout = time.Second
	awaitInterval = 0
	kickstartAfter = 0
	sleep = func(time.Duration) {}
	t.Cleanup(func() {
		processFor, kickstart = originalProcess, originalKickstart
		awaitTimeout, awaitInterval, kickstartAfter = originalTimeout, originalInterval, originalKickAfter
		sleep = originalSleep
	})

	if err := AwaitRunning(DaemonLabel, binary); err != nil {
		t.Fatalf("AwaitRunning: %v", err)
	}
	if kicks != 1 {
		t.Fatalf("kickstarts = %d, want 1", kicks)
	}
}

func TestProcessParsing(t *testing.T) {
	const output = `gui/501/com.shuttle.daemon = {
	state = running
	program = /Users/me/.local/bin/shuttle
	pid = 1529
	coalitions = {
		resource = {
			state = active
		}
	}
}`
	original := psETime
	psETime = func(pid int) (string, error) {
		if pid != 1529 {
			t.Fatalf("pid = %d", pid)
		}
		return "01:00:00\n", nil
	}
	t.Cleanup(func() { psETime = original })

	process, ok, err := ProcessFromPrint(output)
	if err != nil || !ok {
		t.Fatalf("ProcessFromPrint = (%+v, %t, %v)", process, ok, err)
	}
	if process.PID != 1529 || process.Program != "/Users/me/.local/bin/shuttle" {
		t.Fatalf("process = %+v", process)
	}
	if age := time.Since(process.Started); age < 59*time.Minute || age > 61*time.Minute {
		t.Fatalf("process age = %s, want about one hour", age)
	}
}

func TestParseETime(t *testing.T) {
	cases := map[string]time.Duration{
		"05:03":       5*time.Minute + 3*time.Second,
		"1:02:03":     time.Hour + 2*time.Minute + 3*time.Second,
		"12-21:29:42": 12*24*time.Hour + 21*time.Hour + 29*time.Minute + 42*time.Second,
	}
	for value, want := range cases {
		got, err := parseETime(value)
		if err != nil || got != want {
			t.Errorf("parseETime(%q) = (%s, %v), want %s", value, got, err, want)
		}
	}
	for _, value := range []string{"", "42", "abc", "1:2:3:4", "12-", "12-01:02"} {
		if _, err := parseETime(value); err == nil {
			t.Errorf("parseETime(%q) succeeded", value)
		}
	}
}
