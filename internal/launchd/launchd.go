package launchd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"shuttle/internal/config"
)

const (
	// DaemonLabel is the one launchd job phase 1 installs.
	DaemonLabel = "com.shuttle.daemon"
	// WatchdogLabel supplies demand when macOS defers a KeepAlive respawn.
	WatchdogLabel = "com.shuttle.daemon-watchdog"

	daemonEnvFile = "daemon-env"
	daemonLogFile = "daemon.log"
)

// Spec describes one LaunchAgent.
type Spec struct {
	Label                   string
	Program                 string
	Args                    []string
	LogPath                 string
	Env                     map[string]string
	KeepAlive               bool
	RunAtLoad               bool
	ThrottleIntervalSeconds int
}

// PlistXML renders a deterministic LaunchAgent plist.
func (s Spec) PlistXML() string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	b.WriteString(`<plist version="1.0">` + "\n")
	b.WriteString("<dict>\n")

	writeStringKey(&b, "Label", s.Label)
	b.WriteString("    <key>ProgramArguments</key>\n    <array>\n")
	b.WriteString("        <string>" + xmlEscape(s.Program) + "</string>\n")
	for _, arg := range s.Args {
		b.WriteString("        <string>" + xmlEscape(arg) + "</string>\n")
	}
	b.WriteString("    </array>\n")

	if len(s.Env) > 0 {
		keys := make([]string, 0, len(s.Env))
		for key := range s.Env {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		b.WriteString("    <key>EnvironmentVariables</key>\n    <dict>\n")
		for _, key := range keys {
			b.WriteString("        <key>" + xmlEscape(key) + "</key>\n")
			b.WriteString("        <string>" + xmlEscape(s.Env[key]) + "</string>\n")
		}
		b.WriteString("    </dict>\n")
	}

	if s.RunAtLoad {
		b.WriteString("    <key>RunAtLoad</key>\n    <true/>\n")
	}
	if s.KeepAlive {
		b.WriteString("    <key>KeepAlive</key>\n    <true/>\n")
	}
	if s.ThrottleIntervalSeconds > 0 {
		fmt.Fprintf(&b, "    <key>ThrottleInterval</key>\n    <integer>%d</integer>\n", s.ThrottleIntervalSeconds)
	}
	if s.LogPath != "" {
		writeStringKey(&b, "StandardOutPath", s.LogPath)
		writeStringKey(&b, "StandardErrorPath", s.LogPath)
	}

	b.WriteString("</dict>\n</plist>\n")
	return b.String()
}

func writeStringKey(b *strings.Builder, key, value string) {
	b.WriteString("    <key>" + xmlEscape(key) + "</key>\n")
	b.WriteString("    <string>" + xmlEscape(value) + "</string>\n")
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// DaemonSpec persists the daemon's environment before returning the plist
// shape. A later updater can therefore reproduce it without inheriting the
// shell that performed the original install.
func DaemonSpec(program string) (Spec, error) {
	home, err := config.Home()
	if err != nil {
		return Spec{}, err
	}
	agents, err := daemonAgentsDir(home)
	if err != nil {
		return Spec{}, err
	}
	env := map[string]string{
		config.EnvHome:      home,
		config.EnvAgentsDir: agents,
		"PATH":              CanonicalPath(),
	}
	if err := persistEnvironment(home, env); err != nil {
		return Spec{}, err
	}
	return Spec{
		Label:                   DaemonLabel,
		Program:                 program,
		Args:                    []string{"daemon"},
		LogPath:                 filepath.Join(home, daemonLogFile),
		Env:                     env,
		KeepAlive:               true,
		RunAtLoad:               true,
		ThrottleIntervalSeconds: 30,
	}, nil
}

// DaemonSpecs returns the scheduler first and its companion watchdog second.
// The watchdog is a separate long-running job because macOS 26 may leave the
// scheduler's KeepAlive respawn pended indefinitely after a process exit.
func DaemonSpecs(program string) ([]Spec, error) {
	daemon, err := DaemonSpec(program)
	if err != nil {
		return nil, err
	}
	watchdog := daemon
	watchdog.Label = WatchdogLabel
	watchdog.Args = []string{"watchdog"}
	return []Spec{daemon, watchdog}, nil
}

// daemonAgentsDir preserves a previously installed explicit override when an
// updater has no SHUTTLE_AGENTS_DIR in its own environment.
func daemonAgentsDir(home string) (string, error) {
	if value := strings.TrimSpace(os.Getenv(config.EnvAgentsDir)); value != "" {
		return value, nil
	}
	if value := loadEnvironment(filepath.Join(home, daemonEnvFile))[config.EnvAgentsDir]; value != "" {
		return value, nil
	}
	return config.AgentsDir()
}

func persistEnvironment(home string, env map[string]string) error {
	for key, value := range env {
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("launchd: %s contains a newline", key)
		}
	}
	var b strings.Builder
	for _, key := range []string{config.EnvHome, config.EnvAgentsDir, "PATH"} {
		fmt.Fprintf(&b, "%s=%s\n", key, env[key])
	}
	path := filepath.Join(home, daemonEnvFile)
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func loadEnvironment(path string) map[string]string {
	values := make(map[string]string)
	data, err := os.ReadFile(path)
	if err != nil {
		return values
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok {
			values[key] = value
		}
	}
	return values
}

// CanonicalPath is the complete PATH baked into the launchd job. It matches
// loom's list so Homebrew, local installs, and Go-installed tools resolve.
func CanonicalPath() string {
	parts := []string{
		"/opt/homebrew/bin",
		"/opt/homebrew/sbin",
		"/usr/local/bin",
		"/usr/local/sbin",
		"/usr/bin",
		"/bin",
		"/usr/sbin",
		"/sbin",
	}
	if home, err := os.UserHomeDir(); err == nil {
		parts = append([]string{filepath.Join(home, ".local", "bin"), filepath.Join(home, "go", "bin")}, parts...)
	}
	return strings.Join(parts, ":")
}

// PlistPath returns ~/Library/LaunchAgents/<label>.plist.
func PlistPath(label string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist"), nil
}

func domain() string {
	return fmt.Sprintf("gui/%d", os.Getuid())
}

func target(label string) string {
	return domain() + "/" + label
}

var runCommand = func(name string, args ...string) ([]byte, error) {
	return exec.Command(name, args...).CombinedOutput()
}

var sleep = time.Sleep

// Install writes and validates the plist, replaces any prior job, and retries
// launchctl's transient bootstrap EIO.
func Install(spec Spec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	plistPath, err := PlistPath(spec.Label)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(plistPath, []byte(spec.PlistXML()), 0o644); err != nil {
		return err
	}
	if out, err := runCommand("plutil", "-lint", plistPath); err != nil {
		_ = os.Remove(plistPath)
		return fmt.Errorf("plutil -lint: %v: %s", err, strings.TrimSpace(string(out)))
	}
	_, _ = runCommand("launchctl", "bootout", target(spec.Label))
	waitForBootout(spec.Label, 5*time.Second)
	return bootstrapWithRetry(domain(), plistPath, spec.Label)
}

func waitForBootout(label string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := runCommand("launchctl", "print", target(label))
		if err != nil && strings.Contains(string(out), "Could not find service") {
			return
		}
		sleep(100 * time.Millisecond)
	}
}

func bootstrapWithRetry(dom, plistPath, label string) error {
	var lastOut string
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		out, err := runCommand("launchctl", "bootstrap", dom, plistPath)
		if err == nil {
			return nil
		}
		lastOut = strings.TrimSpace(string(out))
		lastErr = err
		if !strings.Contains(lastOut, "Input/output error") {
			break
		}
		sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
	}
	return fmt.Errorf("launchctl bootstrap %s: %v: %s", label, lastErr, lastOut)
}

// Kickstart asks launchd to spawn a loaded job immediately.
func Kickstart(label string) error {
	out, err := runCommand("launchctl", "kickstart", target(label))
	if err != nil {
		return fmt.Errorf("launchctl kickstart %s: %v: %s", label, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Uninstall boots the job out and removes its plist. An absent job and plist
// are both successful, making repeated uninstall calls idempotent.
func Uninstall(label string) error {
	out, err := runCommand("launchctl", "bootout", target(label))
	if err != nil {
		message := strings.TrimSpace(string(out))
		if message != "" &&
			!strings.Contains(message, "No such process") &&
			!strings.Contains(message, "Could not find service") &&
			!strings.Contains(message, "not find specified service") {
			return fmt.Errorf("launchctl bootout %s: %s", label, message)
		}
	}
	waitForBootout(label, 5*time.Second)
	path, err := PlistPath(label)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Status returns launchctl's description of a job.
func Status(label string) (output string, loaded bool, err error) {
	out, runErr := runCommand("launchctl", "print", target(label))
	if runErr != nil {
		return "", false, nil
	}
	return string(out), true, nil
}

// StartTolerance accounts for ps elapsed-time and filesystem timestamp
// granularity when comparing a process start with its binary's mtime.
const StartTolerance = 5 * time.Second

// Process is the live process launchd runs for a label.
type Process struct {
	PID     int
	Program string
	Started time.Time
}

// ProcessFor returns the process launchd currently runs for label.
func ProcessFor(label string) (Process, bool, error) {
	out, loaded, err := Status(label)
	if err != nil || !loaded {
		return Process{}, false, err
	}
	return ProcessFromPrint(out)
}

// ProcessFromPrint parses launchctl output and resolves the process start via
// ps because launchctl does not expose it.
func ProcessFromPrint(out string) (Process, bool, error) {
	pidString, found := printField(out, "pid")
	if !found {
		return Process{}, false, nil
	}
	pid, err := strconv.Atoi(pidString)
	if err != nil {
		return Process{}, false, fmt.Errorf("launchd: bad pid %q", pidString)
	}
	started, err := processStart(pid)
	if err != nil {
		return Process{}, false, err
	}
	program, _ := printField(out, "program")
	return Process{PID: pid, Program: program, Started: started}, true, nil
}

func printField(out, key string) (string, bool) {
	prefix := key + " = "
	for line := range strings.SplitSeq(out, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}

func processStart(pid int) (time.Time, error) {
	out, err := psETime(pid)
	if err != nil {
		return time.Time{}, fmt.Errorf("ps -p %d: %w", pid, err)
	}
	elapsed, err := parseETime(out)
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(-elapsed), nil
}

var psETime = func(pid int) (string, error) {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "etime=").Output()
	return string(out), err
}

func parseETime(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	rest := value
	days := 0
	if i := strings.Index(rest, "-"); i >= 0 {
		var err error
		days, err = strconv.Atoi(rest[:i])
		if err != nil {
			return 0, fmt.Errorf("launchd: bad etime %q", value)
		}
		rest = rest[i+1:]
	}
	parts := strings.Split(rest, ":")
	if len(parts) < 2 || len(parts) > 3 || (days > 0 && len(parts) != 3) {
		return 0, fmt.Errorf("launchd: bad etime %q", value)
	}
	numbers := make([]int, len(parts))
	for i, part := range parts {
		number, err := strconv.Atoi(part)
		if err != nil || number < 0 {
			return 0, fmt.Errorf("launchd: bad etime %q", value)
		}
		numbers[i] = number
	}
	hours := 0
	if len(numbers) == 3 {
		hours, numbers = numbers[0], numbers[1:]
	}
	return time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(numbers[0])*time.Minute +
		time.Duration(numbers[1])*time.Second, nil
}

var (
	processFor     = ProcessFor
	kickstart      = Kickstart
	awaitTimeout   = 6 * time.Second
	awaitInterval  = 250 * time.Millisecond
	kickstartAfter = 3 * time.Second
)

// AwaitRunning waits for the installed job to run a process no older than the
// installed binary. A processless job gets one kickstart during the wait.
func AwaitRunning(label, binary string) error {
	info, statErr := os.Stat(binary)
	deadline := time.Now().Add(awaitTimeout)
	kickAt := time.Now().Add(kickstartAfter)
	kicked := false
	for {
		var last error
		process, ok, err := processFor(label)
		switch {
		case err != nil:
			last = fmt.Errorf("cannot read process state: %v", err)
		case !ok:
			last = fmt.Errorf("no process")
		case statErr == nil && process.Started.Before(info.ModTime().Add(-StartTolerance)):
			last = fmt.Errorf("pid %d started %s, before the installed binary (%s) — still the old image",
				process.PID, process.Started.Format(time.RFC3339), info.ModTime().Format(time.RFC3339))
		default:
			return nil
		}
		if !kicked && !ok && time.Now().After(kickAt) {
			kicked = true
			_ = kickstart(label)
		}
		if time.Now().After(deadline) {
			return last
		}
		sleep(awaitInterval)
	}
}

func (s Spec) validate() error {
	if s.Label == "" {
		return fmt.Errorf("launchd: empty Label")
	}
	if strings.ContainsAny(s.Label, "/ ") {
		return fmt.Errorf("launchd: Label %q must not contain slashes or spaces", s.Label)
	}
	if s.Program == "" {
		return fmt.Errorf("launchd: empty Program for %s", s.Label)
	}
	if !filepath.IsAbs(s.Program) {
		return fmt.Errorf("launchd: Program for %s must be absolute, got %q", s.Label, s.Program)
	}
	return nil
}
