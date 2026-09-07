package claude

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
)

const stubInitLine = `{"type":"system","subtype":"init","session_id":"stub-session"}` + "\n"

// stubScript writes stdout verbatim, then runs whatever else the case needs.
// The heredoc keeps the emitted bytes exactly the bytes the transcript must
// receive.
func stubScript(stdout, rest string) string {
	return "#!/bin/sh\ncat <<'STREAM'\n" + stdout + "STREAM\n" + rest
}

// stubGroupScript is stubScript with the leader recording its own pid, which
// Setpgid makes the pgid of the group a kill must empty.
func stubGroupScript(stdout, rest string) string {
	return strings.Replace(stubScript(stdout, rest), "#!/bin/sh\n", "#!/bin/sh\necho $$ > pgid\n", 1)
}

// stubOptions installs script as the claude binary for one run. The run
// directory deliberately does not exist yet: Run owns creating it.
func stubOptions(t *testing.T, script string) Options {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return Options{
		Bin:          bin,
		RunDir:       filepath.Join(dir, "run"),
		Auth:         AuthAPIKey,
		Model:        "haiku",
		MaxTurns:     1,
		MaxBudgetUSD: 0.05,
		Tools:        []string{"Read", "Write"},
		// A stub that wedges must fail on this rather than on the package
		// timeout; the cases that test the timeout raise or lower it.
		TimeoutSeconds: 30,
		APIKey:         "stub-key",
		Prompt:         "summarise the day",
		PATH:           "/usr/bin:/bin",
		HomeDir:        dir,
		Timezone:       "America/Los_Angeles",
	}
}

// Each failure class is produced by the real Run path against a stub binary
// that reproduces the condition, never by calling the classifier directly.
func TestRunFailureClasses(t *testing.T) {
	tests := []struct {
		name       string
		stdout     string
		rest       string
		class      string
		exitCode   int
		detailHas  []string
		hasResult  bool
		wantCost   float64
		wantCosted bool
	}{
		{
			name:      "died",
			stdout:    stubInitLine + `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working"}]}}` + "\n",
			rest:      "echo 'claude: fatal: connection reset' >&2\nexit 3\n",
			class:     ledger.ClassDied,
			exitCode:  3,
			detailHas: []string{"exit 3", "connection reset"},
		},
		{
			name:      "no_result",
			stdout:    stubInitLine + `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working"}]}}` + "\n",
			rest:      "exit 0\n",
			class:     ledger.ClassNoResult,
			detailHas: []string{"exit 0"},
		},
		{
			name:       "budget",
			stdout:     stubInitLine + `{"type":"result","subtype":"error_max_budget_usd","is_error":true,"result":"Budget limit of $0.05 reached","num_turns":4,"total_cost_usd":0.0512,"session_id":"stub-session","usage":{"input_tokens":1200,"output_tokens":340}}` + "\n",
			rest:       "exit 0\n",
			class:      ledger.ClassBudget,
			detailHas:  []string{subtypeMaxBudget},
			hasResult:  true,
			wantCost:   0.0512,
			wantCosted: true,
		},
		{
			name: "auth",
			stdout: stubInitLine +
				`{"type":"system","subtype":"api_retry","error":"authentication_failed","attempt":1,"session_id":"stub-session"}` + "\n" +
				`{"type":"result","subtype":"error_during_execution","is_error":true,"result":"API Error: 401","num_turns":0,"total_cost_usd":0,"session_id":"stub-session"}` + "\n",
			rest:       "exit 1\n",
			class:      ledger.ClassAuth,
			exitCode:   1,
			detailHas:  []string{errorAuthenticationFailed},
			hasResult:  true,
			wantCosted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var transcript, stderr bytes.Buffer
			out, err := Run(context.Background(), stubOptions(t, stubScript(tt.stdout, tt.rest)), &transcript, &stderr)
			if err != nil {
				t.Fatal(err)
			}
			if out.Class != tt.class {
				t.Errorf("class = %q, want %q (detail %q)", out.Class, tt.class, out.Detail)
			}
			if out.ExitCode != tt.exitCode {
				t.Errorf("exit code = %d, want %d", out.ExitCode, tt.exitCode)
			}
			for _, want := range tt.detailHas {
				if !strings.Contains(out.Detail, want) {
					t.Errorf("detail %q does not name %q", out.Detail, want)
				}
			}
			if out.SessionID != "stub-session" {
				t.Errorf("session id = %q, want stub-session", out.SessionID)
			}
			if (out.Result != nil) != tt.hasResult {
				t.Fatalf("result present = %v, want %v", out.Result != nil, tt.hasResult)
			}
			if tt.hasResult {
				if out.Result.CostReported != tt.wantCosted || out.Result.TotalCostUSD != tt.wantCost {
					t.Errorf("cost = %v (reported %v), want %v (reported %v)",
						out.Result.TotalCostUSD, out.Result.CostReported, tt.wantCost, tt.wantCosted)
				}
			}
			if transcript.String() != tt.stdout {
				t.Errorf("transcript =\n%q\nwant\n%q", transcript.String(), tt.stdout)
			}
		})
	}
}

// The timeout kills the process GROUP, so a grandchild the CLI spawned does
// not outlive the run (§7.4).
func TestRunStuckKillsProcessGroup(t *testing.T) {
	o := stubOptions(t, stubGroupScript(stubInitLine, "sleep 300 &\nsleep 300\n"))
	o.TimeoutSeconds = 1

	var transcript, stderr bytes.Buffer
	start := time.Now()
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if out.Class != ledger.ClassStuck {
		t.Errorf("class = %q, want %q (detail %q)", out.Class, ledger.ClassStuck, out.Detail)
	}
	if !strings.Contains(out.Detail, "timeout_seconds 1") {
		t.Errorf("detail %q does not name the timeout", out.Detail)
	}
	if out.Result != nil {
		t.Errorf("result = %+v, want none", out.Result)
	}
	if transcript.String() != stubInitLine {
		t.Errorf("transcript = %q, want %q", transcript.String(), stubInitLine)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("run took %s: the group did not die on the SIGTERM", elapsed)
	}

	requireEmptyGroup(t, readPgid(t, o.RunDir))
}

// readPgid is the pid the stub's group leader recorded for itself.
func readPgid(t *testing.T, runDir string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(runDir, "pgid"))
	if err != nil {
		t.Fatal(err)
	}
	pgid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return pgid
}

// requireEmptyGroup polls: a reparented descendant can still be an unreaped
// zombie for a moment after the leader is waited on.
func requireEmptyGroup(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group %d still has members after the run: %v", pgid, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A descendant that ignores SIGTERM and keeps none of the inherited pipes stays
// in the group while the drains hit EOF, so the wait completes with the group
// still populated. The SIGKILL is what empties it, and it may not be
// conditional on that wait.
func TestRunStuckKillsAGroupMemberThatOutlivesTheWait(t *testing.T) {
	// Redirected off the pipes, so the leader dying is EOF for both drains;
	// the trap keeps it in the group through the group SIGTERM, and the loop
	// outlives the sleep that same signal kills.
	descendant := `sh -c 'trap "" TERM; while :; do sleep 0.2; done' >/dev/null 2>&1 &
sleep 300
`
	o := stubOptions(t, stubGroupScript(stubInitLine, descendant))
	o.TimeoutSeconds = 1

	var transcript, stderr bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	pgid := readPgid(t, o.RunDir)
	// Whatever the assertions find, nothing the stub started outlives the test.
	t.Cleanup(func() { syscall.Kill(-pgid, syscall.SIGKILL) })

	if out.Class != ledger.ClassStuck {
		t.Errorf("class = %q, want %q (detail %q)", out.Class, ledger.ClassStuck, out.Detail)
	}
	requireEmptyGroup(t, pgid)
}

// A child that emits a long run of bytes with no newline may not be an
// unbounded allocation in the daemon: the transcript takes every byte and the
// parser skips the line rather than buffering it.
func TestRunBoundsTheLineItParses(t *testing.T) {
	const noiseBytes = 32 << 20
	resultLine := `{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1,"total_cost_usd":0.0031,"session_id":"stub-session"}`
	rest := "dd if=/dev/zero bs=1048576 count=32 2>/dev/null | tr '\\0' x\n" +
		"printf '\\n'\n" +
		"echo '" + resultLine + "'\n"
	o := stubOptions(t, stubScript(stubInitLine, rest))

	var transcript countingWriter
	var stderr bytes.Buffer
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out, err := Run(context.Background(), o, &transcript, &stderr)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	// The line after the oversized one still parses: the drain skipped that
	// line rather than losing its place in the stream.
	if out.Class != "" || out.Result == nil {
		t.Fatalf("class = %q, result = %+v, want a classified success", out.Class, out.Result)
	}
	if want := int64(len(stubInitLine) + noiseBytes + 1 + len(resultLine) + 1); transcript.n != want {
		t.Errorf("transcript received %d bytes, want %d: §7.2 records the stream verbatim", transcript.n, want)
	}
	// What a run allocates is bounded by maxParsedLineBytes and does not grow
	// with the stream; buffering the line to parse it allocates the whole of
	// it, and through append's doubling rather more than once.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > noiseBytes/2 {
		t.Errorf("allocated %d bytes over a %d byte line, want it left unbuffered", allocated, noiseBytes)
	}
}

// countingWriter counts what it is given without holding it, so the memory the
// case above measures is the drain's and not the test's.
type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

// A descendant that leaves the process group outlives SIGKILL and keeps the
// inherited pipes open, so the drains never reach EOF. Run abandons them rather
// than wedging the daemon, and hands off cleanly: the outcome is a snapshot and
// the caller's writers are cut loose before it returns.
func TestRunStuckAbandonsEscapedDrains(t *testing.T) {
	// `set -m` puts the background job in a process group of its own, which is
	// what setsid(1) would do on a system that shipped it. The escaped process
	// only starts writing well after the kill, so anything it produces lands
	// after Run has returned.
	escape := "set -m\n" +
		"sh -c 'echo $$ > escaped.pid; sleep 3; while :; do echo escaped-stdout; echo escaped-stderr >&2; sleep 0.2; done' &\n" +
		"set +m\nsleep 300\n"
	o := stubOptions(t, stubScript(stubInitLine, escape))
	o.TimeoutSeconds = 1
	// Registered before the launch, so a failure below still reaps it, and
	// before the killGrace restore, which LIFO then runs first: this package's
	// grace period must be back at 5s whatever happens here.
	t.Cleanup(func() { killEscaped(t, o.RunDir) })

	killGrace = 100 * time.Millisecond
	t.Cleanup(func() { killGrace = 5 * time.Second })

	var transcript, stderr bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}

	if out.Class != ledger.ClassStuck {
		t.Errorf("class = %q, want %q (detail %q)", out.Class, ledger.ClassStuck, out.Detail)
	}
	for _, want := range []string{"timeout_seconds 1", abandonedDetail} {
		if !strings.Contains(out.Detail, want) {
			t.Errorf("detail %q does not name %q", out.Detail, want)
		}
	}
	if out.SessionID != "stub-session" {
		t.Errorf("session id = %q, want the one the drain had read", out.SessionID)
	}

	// Past the point the escaped process starts writing: whatever it produces
	// must not reach the buffers the caller owns, which it is free to close.
	time.Sleep(3 * time.Second)
	if transcript.String() != stubInitLine {
		t.Errorf("transcript = %q, want %q: the abandoned drain still holds it", transcript.String(), stubInitLine)
	}
	if stderr.String() != "" {
		t.Errorf("stderr = %q, want nothing: the abandoned drain still holds it", stderr.String())
	}
}

// killEscaped stops the process the abandon-path stub left outside the group,
// which also releases the pipes the drain goroutines are still on. It runs as a
// cleanup, so it reports rather than fatals: a Fatal here unwinds through
// runtime.Goexit and skips the cleanups queued behind it, leaving killGrace at
// 100ms for every later test in the package.
func killEscaped(t *testing.T, runDir string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(runDir, "escaped.pid"))
	if err != nil {
		t.Errorf("read the escaped pid: %v", err)
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Errorf("escaped pid %q: %v", raw, err)
		return
	}
	// It leads its own group, so the negative pid takes its children too. ESRCH
	// is the group already being gone, which is the state this wants.
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Errorf("kill escaped group %d: %v", pid, err)
	}
}

// §7.2 makes the transcript the run's only record, and the parser sees every
// line whether or not the write landed, so a failed write has to be reported.
func TestRunReportsATranscriptWriteFailure(t *testing.T) {
	stdout := stubInitLine +
		`{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1,"total_cost_usd":0.0031,"session_id":"stub-session"}` + "\n"
	o := stubOptions(t, stubScript(stdout, "exit 0\n"))

	var stderr bytes.Buffer
	out, err := Run(context.Background(), o, failingWriter{errors.New("no space left on device")}, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if out.TranscriptErr == nil || !strings.Contains(out.TranscriptErr.Error(), "no space left on device") {
		t.Errorf("transcript err = %v, want the write failure", out.TranscriptErr)
	}
	// The parse path carries on regardless: the run itself succeeded.
	if out.Class != "" || out.Result == nil {
		t.Errorf("class = %q, result = %+v, want a classified success", out.Class, out.Result)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write(p []byte) (int, error) { return 0, w.err }

// A lost stderr.log is the same case: §7.2 makes it the evidence behind the
// died and no_result classes, so it is reported rather than dropped.
func TestRunReportsAStderrWriteFailure(t *testing.T) {
	o := stubOptions(t, stubScript(stubInitLine, "echo 'claude: fatal: connection reset' >&2\nexit 3\n"))

	var transcript bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, failingWriter{errors.New("no space left on device")})
	if err != nil {
		t.Fatal(err)
	}
	if out.StderrErr == nil || !strings.Contains(out.StderrErr.Error(), "no space left on device") {
		t.Errorf("stderr err = %v, want the write failure", out.StderrErr)
	}
	// The run classifies regardless: the evidence is what was lost, not the
	// verdict.
	if out.Class != ledger.ClassDied {
		t.Errorf("class = %q, want %q (detail %q)", out.Class, ledger.ClassDied, out.Detail)
	}
}

// The tail is cut at a byte offset but reaches the ledger's evidence column and
// the summary email, so it may not start mid-rune.
func TestRunStderrTailIsValidUTF8(t *testing.T) {
	// One odd byte before multibyte content, so the cut lands inside a rune.
	noise := "x" + strings.Repeat("é", stderrTailBytes)
	o := stubOptions(t, stubScript(stubInitLine, "cat <<'NOISE' >&2\n"+noise+"\nNOISE\nexit 3\n"))

	var transcript, stderr bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(out.Detail) {
		t.Errorf("detail is not valid UTF-8: %q", out.Detail)
	}
	if !strings.HasSuffix(out.Detail, "é") {
		t.Errorf("detail %q does not end in the tail of the stderr stream", out.Detail[max(0, len(out.Detail)-16):])
	}
}

// Without a deadline the drain before Wait can block the daemon forever, so
// the launch is refused rather than started.
func TestRunRequiresATimeout(t *testing.T) {
	o := stubOptions(t, stubScript(stubInitLine, "exit 0\n"))
	o.TimeoutSeconds = 0

	var transcript, stderr bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err == nil {
		t.Fatalf("outcome = %+v, want a refused launch", out)
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("err = %v, want one naming the timeout", err)
	}
}

// A cancelled context takes the same kill path as the timeout.
func TestRunContextCancellationKillsTheRun(t *testing.T) {
	o := stubOptions(t, stubScript(stubInitLine, "sleep 300\n"))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	defer cancel()

	var transcript, stderr bytes.Buffer
	out, err := Run(ctx, o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if out.Class != ledger.ClassStuck {
		t.Errorf("class = %q, want %q (detail %q)", out.Class, ledger.ClassStuck, out.Detail)
	}
}

// A successful run: the launch preparation Run owns, the §7.3 stdin framing
// and an empty class.
func TestRunSuccessPreparesTheRunDirectory(t *testing.T) {
	stdout := stubInitLine +
		`{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1,"total_cost_usd":0.0031,"session_id":"stub-session","usage":{"input_tokens":900,"output_tokens":120}}` + "\n"
	o := stubOptions(t, stubScript(stdout, "cat > stdin.json\npwd > cwd\nenv > env\n"))
	// Set on the daemon's own process, so their absence from the child below is
	// the environment being built rather than this machine simply lacking them.
	t.Setenv("SSH_AUTH_SOCK", "/private/tmp/agent.sock")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "inherited-oauth-token")

	var transcript, stderr bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if out.Class != "" {
		t.Fatalf("class = %q, want success (detail %q)", out.Class, out.Detail)
	}
	if out.Result == nil || !out.Result.CostReported || out.Result.TotalCostUSD != 0.0031 {
		t.Errorf("result = %+v, want a priced success", out.Result)
	}
	if transcript.String() != stdout {
		t.Errorf("transcript =\n%q\nwant\n%q", transcript.String(), stdout)
	}

	for _, dir := range []string{o.TmpDir(), o.ConfigDir(), o.OutDir()} {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			t.Errorf("%s: not a directory before the launch: %v", dir, err)
		}
	}
	settings, err := os.ReadFile(o.SettingsPath())
	if err != nil {
		t.Fatal(err)
	}
	want, err := SettingsJSON(o)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(settings, want) {
		t.Errorf("settings.json =\n%s\nwant\n%s", settings, want)
	}

	stdin, err := os.ReadFile(filepath.Join(o.RunDir, "stdin.json"))
	if err != nil {
		t.Fatal(err)
	}
	wantStdin, err := promptLine(o.Prompt)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdin, wantStdin) {
		t.Errorf("stdin = %s, want %s", stdin, wantStdin)
	}

	cwd, err := os.ReadFile(filepath.Join(o.RunDir, "cwd"))
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := filepath.EvalSymlinks(o.RunDir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(cwd)) != runDir {
		t.Errorf("child cwd = %q, want the run directory %q", strings.TrimSpace(string(cwd)), runDir)
	}

	// What the child observes, not what Environ returned: the §7.4 nine are
	// only what reaches it if nothing in between adds to them.
	wantEnv := map[string]string{
		"PATH":               o.PATH,
		"HOME":               o.HomeDir,
		"TMPDIR":             o.TmpDir(),
		"CLAUDE_CODE_TMPDIR": o.TmpDir(),
		"CLAUDE_CONFIG_DIR":  o.ConfigDir(),
		"ANTHROPIC_API_KEY":  o.APIKey,
		"DISABLE_TELEMETRY":  "1",
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
		"TZ": o.Timezone,
	}
	rawEnv, err := os.ReadFile(filepath.Join(o.RunDir, "env"))
	if err != nil {
		t.Fatal(err)
	}
	gotEnv := map[string]string{}
	for _, line := range strings.Split(strings.TrimSuffix(string(rawEnv), "\n"), "\n") {
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("env line %q is not name=value", line)
		}
		// The stub is a shell script and these three are the shell's own, set
		// after exec rather than passed to it.
		if name == "_" || name == "PWD" || name == "SHLVL" {
			continue
		}
		gotEnv[name] = value
	}
	if len(gotEnv) != len(wantEnv) {
		t.Errorf("child observed %d variables, want the §7.4 nine: %v", len(gotEnv), gotEnv)
	}
	for name, value := range wantEnv {
		if gotEnv[name] != value {
			t.Errorf("child %s = %q, want %q", name, gotEnv[name], value)
		}
	}
	for _, name := range []string{"SSH_AUTH_SOCK", "ANTHROPIC_AUTH_TOKEN"} {
		if _, ok := gotEnv[name]; ok {
			t.Errorf("%s reached the child from the daemon's own environment", name)
		}
	}
}

// Opt-in: this one spends real money, capped at max_budget_usd 0.05.
func TestRunLive(t *testing.T) {
	if os.Getenv("SHUTTLE_LIVE_CLAUDE") != "1" {
		t.Skip("set SHUTTLE_LIVE_CLAUDE=1 to run against the real binary")
	}
	bin := os.Getenv("SHUTTLE_CLAUDE_BIN")
	if bin == "" {
		bin = "/opt/homebrew/bin/claude"
	}
	dir := t.TempDir()
	o := Options{
		Bin:            bin,
		RunDir:         filepath.Join(dir, "run"),
		Auth:           AuthAPIKey,
		Model:          "haiku",
		MaxTurns:       1,
		MaxBudgetUSD:   0.05,
		Tools:          []string{"Read"},
		TimeoutSeconds: 120,
		APIKey:         liveAPIKey(t),
		Prompt:         "Reply with exactly the word ok.",
		PATH:           "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin",
		HomeDir:        dir,
		Timezone:       "America/Los_Angeles",
	}
	var transcript, stderr bytes.Buffer
	out, err := Run(context.Background(), o, &transcript, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if out.Class != "" {
		t.Fatalf("class = %q, detail %q, stderr %q", out.Class, out.Detail, stderr.String())
	}
	if out.Result == nil || !out.Result.CostReported {
		t.Fatalf("result = %+v, want one carrying a cost", out.Result)
	}
	t.Logf("session %s, %d turns, $%.4f", out.SessionID, out.Result.NumTurns, out.Result.TotalCostUSD)
}

// liveAPIKey prefers the configured Keychain item, the way a real run resolves
// it, and skips rather than fails when no key is available.
func liveAPIKey(t *testing.T) string {
	t.Helper()
	if c, err := config.Load(); err == nil {
		if key, err := c.AnthropicKey(); err == nil {
			return key
		}
	}
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		return key
	}
	t.Skip("no anthropic key: the config keychain item and ANTHROPIC_API_KEY both failed to resolve")
	return ""
}
