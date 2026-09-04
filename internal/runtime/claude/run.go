package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"shuttle/internal/ledger"
)

// killGrace is the §7.4 pause between SIGTERM and SIGKILL: the CLI exits 143
// on the term, terminating its own tree and running SessionEnd first. A var so
// the abandon-path test does not sit through two full grace periods.
var killGrace = 5 * time.Second

// stderrTailBytes bounds what the ledger's evidence column can carry. The full
// stream is on its way to stderr.log regardless; only the tail is held here.
const stderrTailBytes = 4096

// abandonedDetail is the evidence for a run whose drains outlived the kill: a
// descendant left the process group and still holds the pipes, so the
// transcript and the stderr tail stop wherever the kill found them.
const abandonedDetail = "descendants outlived SIGKILL; output ends at the kill"

// authDetail is the evidence for the auth class: the one line that says the
// credential itself was rejected (§7.6).
const authDetail = "system/" + subtypeAPIRetry + ": " + errorAuthenticationFailed

// Outcome is the harness verdict for one launch. Class is empty when the run
// succeeded, else a §7.6 class; Result is nil when no result line arrived.
type Outcome struct {
	Class     string
	Detail    string
	ExitCode  int
	SessionID string
	Result    *Result
	// TranscriptErr is the first failed transcript write, if any. The parse
	// path reads every line regardless, so a run that lost its only record
	// (§7.2) classifies like any other and this is the only sign of it.
	TranscriptErr error
	// StderrErr is the failed stderr copy, if any, and is the same case: §7.2
	// makes stderr.log the evidence behind the died and no_result classes, so
	// losing it would otherwise be invisible in exactly those runs.
	StderrErr error
}

// Run launches the model, streams its stdout to transcript verbatim and its
// stderr to stderrW, and classifies what happened (§7.4, §7.6). Both writers
// must be non-nil.
//
// The launch itself is no boundary: settings.json is a permission document the
// CLI applies to itself, so a direct launch holds the invoking user's whole
// filesystem and network reach. Options.Launcher is where the §11 OS boundary
// is interposed — the caller puts the srt invocation there and the process
// control here (drain, timeout, group kill) stays in one place. An empty
// Launcher is the direct launch §12's --unsafe-no-sandbox asks for.
//
// The returned error is harness-level only — the binary would not start, the
// run directory would not be created. A model run that failed is a non-nil
// Outcome carrying a class, never an error.
func Run(ctx context.Context, o Options, transcript, stderrW io.Writer) (*Outcome, error) {
	// A run with no deadline is an unbounded wait in a daemon: the pipes are
	// drained to EOF before Wait, so a descendant holding one open blocks here
	// forever and no ledger row is ever written. Checked here rather than in
	// requireLaunchable because the timeout is process control: neither the
	// argv nor the environment interpolates it.
	if o.TimeoutSeconds <= 0 {
		return nil, fmt.Errorf("timeout_seconds %d: a run must carry a positive timeout", o.TimeoutSeconds)
	}
	argv, err := Argv(o)
	if err != nil {
		return nil, err
	}
	env, err := Environ(o)
	if err != nil {
		return nil, err
	}
	settingsDoc, err := SettingsJSON(o)
	if err != nil {
		return nil, err
	}
	prompt, err := promptLine(o.Prompt)
	if err != nil {
		return nil, err
	}
	// The argv names settings.json and the environment names the other three,
	// so this package writes them before the launch that reads them.
	for _, dir := range []string{o.TmpDir(), o.ConfigDir(), o.OutDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(o.SettingsPath(), settingsDoc, 0o600); err != nil {
		return nil, err
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	// §7.3's prompt tells the model about ./inputs/ and ./out/.
	cmd.Dir = o.RunDir
	cmd.Env = env
	// The CLI spawns its own tree; the group is what the timeout kills.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		// os/exec closes the descriptors it opened from inside Start, which
		// these returns never reach. A daemon that leaked a pair per failed
		// launch would make fd exhaustion, the only condition these fail
		// under, permanent.
		stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		stdin.Close()
		stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", o.Bin, err)
	}
	// Setpgid makes the child its own group leader, so its pid is the pgid.
	pgid := cmd.Process.Pid

	go func() {
		// A child that exits before reading the prompt breaks this pipe. The
		// write error is not the run's verdict — the missing result is — so it
		// is dropped here rather than raced against the classification.
		stdin.Write(prompt)
		stdin.Close()
	}()

	// The drains write through these rather than to the caller's writers
	// directly, so the abandon path below can cut them loose.
	transcriptOut := &detachable{w: transcript}
	stderrOut := &detachable{w: stderrW}

	var (
		d      drain
		drains sync.WaitGroup
	)
	drains.Add(2)
	go func() {
		defer drains.Done()
		drainStdout(stdout, transcriptOut, &d)
	}()
	go func() {
		defer drains.Done()
		// The read side is this package's own pipe, so what the copy returns
		// is the sink's failure.
		_, err := io.Copy(io.MultiWriter(stderrOut, &d), stderr)
		d.recordStderrErr(err)
	}()

	// The pipes must be drained to EOF before Wait, which closes them. reapOnce
	// keeps that Wait and the abandon path's below to one reap between them:
	// two goroutines waiting on one pid race for its status.
	var reapOnce sync.Once
	waited := make(chan error, 1)
	go func() {
		drains.Wait()
		reapOnce.Do(func() { waited <- cmd.Wait() })
	}()

	timeout := time.NewTimer(time.Duration(o.TimeoutSeconds) * time.Second)
	defer timeout.Stop()
	// stuckDetail is set only when the harness stopped the run, which is the
	// first branch of the §7.6 precedence below.
	var stuckDetail string
	var abandoned bool
	select {
	case err = <-waited:
	case <-timeout.C:
		stuckDetail = fmt.Sprintf("timeout_seconds %d elapsed", o.TimeoutSeconds)
		abandoned, err = killGroup(pgid, waited)
	case <-ctx.Done():
		// A cancelled run is deliberately folded into stuck: §7.6 defines the
		// class as timeout_seconds elapsing and names none for a daemon
		// restart mid-run. Anything aggregating on class — streaks, incidents
		// — has to read Detail to tell a hang from a stop.
		stuckDetail = "stopped: " + ctx.Err().Error()
		abandoned, err = killGroup(pgid, waited)
	}
	if abandoned {
		// The drains are still running and still hold these writers; the
		// caller may close what it passed in the moment Run returns.
		transcriptOut.detach()
		stderrOut.detach()
		// cmd.Wait sits behind the drains in the waiter goroutine, which the
		// escaped descendant releases only when it closes the pipes, so the
		// SIGKILLed immediate child would stay an unreaped zombie until then
		// and for the daemon's lifetime if it never does. os.Process.Wait
		// reaps it without touching the pipes the drains are still reading.
		reapOnce.Do(func() { cmd.Process.Wait() })
		stuckDetail += "; " + abandonedDetail
	}
	// Read once, under the lock, because on the abandon path the drains are
	// writing the same fields as this reads them.
	stream, tail, transcriptErr, stderrErr := d.snapshot()

	exitCode := 0
	var exitErr *exec.ExitError
	switch {
	case errors.As(err, &exitErr):
		exitCode = exitErr.ExitCode()
	case err != nil:
		return nil, fmt.Errorf("wait %s: %w", o.Bin, err)
	}

	out := &Outcome{
		ExitCode:      exitCode,
		SessionID:     stream.SessionID,
		Result:        stream.Result,
		TranscriptErr: transcriptErr,
		StderrErr:     stderrErr,
	}
	switch {
	case stuckDetail != "":
		out.Class = ledger.ClassStuck
		out.Detail = stuckDetail
	case stream.Result != nil && stream.Result.Success():
		// Class stays empty: this is the only path that succeeded.
	case stream.Result != nil:
		switch {
		case stream.AuthFailed:
			out.Class = ledger.ClassAuth
			out.Detail = authDetail
		case stream.Result.Subtype == subtypeMaxTurns || stream.Result.Subtype == subtypeMaxBudget:
			out.Class = ledger.ClassBudget
			out.Detail = withTail("result subtype "+stream.Result.Subtype, tail)
		default:
			// §7.6 has no class for a terminal result carrying an error that is
			// neither auth nor budget; died is the deliberate choice for it
			// rather than a fallthrough. The detail names the subtype and the
			// stderr tail only — an exit code here would contradict died's
			// definition, which is a non-zero exit with no result at all.
			out.Class = ledger.ClassDied
			out.Detail = withTail("unrecognised failing result subtype "+stream.Result.Subtype, tail)
		}
	case stream.AuthFailed:
		out.Class = ledger.ClassAuth
		out.Detail = authDetail
	case exitCode == 0:
		out.Class = ledger.ClassNoResult
		out.Detail = withTail("exit 0 with no result", tail)
	default:
		out.Class = ledger.ClassDied
		out.Detail = withTail(fmt.Sprintf("exit %d with no result", exitCode), tail)
	}
	return out, nil
}

// killGroup stops the whole process group rather than the child alone: the CLI
// leaves its own tree behind a bare SIGTERM to the leader (§7.4). The first
// return reports that neither signal freed the wait, which is the caller's
// signal that the drain goroutines are still running and still writing.
func killGroup(pgid int, waited <-chan error) (bool, error) {
	syscall.Kill(-pgid, syscall.SIGTERM)
	var err error
	var reaped bool
	select {
	case err = <-waited:
		reaped = true
	case <-time.After(killGrace):
	}
	// Unconditional, because the leader exiting is not the group emptying: a
	// descendant that ignores the SIGTERM and closed the inherited pipes puts
	// the drains, and the wait behind them, at EOF while it is still a member.
	// The group id stays reserved while it has one, so this cannot land on an
	// unrelated group.
	syscall.Kill(-pgid, syscall.SIGKILL)
	if reaped {
		return false, err
	}
	select {
	case err = <-waited:
		return false, err
	case <-time.After(killGrace):
		// The signal reaches the group, not a descendant that left it by
		// calling setsid or by being reparented; such a descendant keeps the
		// inherited pipes open, so the drain before Wait never finishes and
		// waited never fires. Abandon it: the two drain goroutines, the waiter
		// goroutine behind them, the stdout and stderr read ends they hold,
		// and the stdin writer if the prompt outgrew the pipe buffer, all stay
		// until the descendant closes the pipes. They cost less than a daemon
		// wedged on this run.
		return true, nil
	}
}

// drain is everything the two drain goroutines produce. The mutex is not belt:
// on killGroup's abandon path they are still writing these fields while Run
// reads them to classify.
type drain struct {
	mu            sync.Mutex
	stream        Stream
	tail          tailWriter
	transcriptErr error
	stderrErr     error
}

// record feeds one stdout line to the parser and keeps the first transcript
// write failure. The parse path sees the line either way, so a lost write would
// otherwise classify as an ordinary run with no sign that §7.2's only record of
// what the model did is incomplete.
func (d *drain) record(line []byte, writeErr error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Empty is a line past maxParsedLineBytes: recorded for its write error,
	// never parsed.
	if len(line) > 0 {
		d.stream.Line(line)
	}
	if writeErr != nil && d.transcriptErr == nil {
		d.transcriptErr = writeErr
	}
}

// recordStderrErr keeps the stderr copy's verdict. io.Copy stops at the first
// failure, so there is only ever the one.
func (d *drain) recordStderrErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stderrErr = err
}

// Write is the stderr tail's sink.
func (d *drain) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.tail.Write(p)
}

// snapshot is everything the classification reads, taken in one hold so a live
// drain cannot tear it.
func (d *drain) snapshot() (Stream, string, error, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stream, d.tail.String(), d.transcriptErr, d.stderrErr
}

// detachable forwards to w until detach, after which writes go nowhere. Run
// detaches on the abandon path: the drains outlive the call there, and the
// caller owns the files it passed in — a doc comment would not stop it closing
// them under a live write.
type detachable struct {
	mu sync.Mutex
	w  io.Writer
}

func (d *detachable) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.w == nil {
		return len(p), nil
	}
	return d.w.Write(p)
}

func (d *detachable) detach() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.w = nil
}

// maxParsedLineBytes caps what the parser is given from one stdout line. One
// stream-json line carries a whole content block — weft measured a 42 KB
// tool_result line — so a megabyte clears a real one by more than an order of
// magnitude, and a child emitting a long run of bytes with no newline is not
// an unbounded allocation in the daemon: an abandoned drain keeps reading long
// after Run has returned.
const maxParsedLineBytes = 1 << 20

// drainStdout writes what it reads to the transcript byte for byte, newline
// included, and feeds each complete line to the parser. The read is in bounded
// chunks (bufio.Reader rather than bufio.Scanner, whose default 64 KB cap
// would turn a long line into an error) and a line past maxParsedLineBytes is
// written but not parsed: the transcript is the run's record and stays
// verbatim either way.
func drainStdout(r io.Reader, transcript io.Writer, d *drain) {
	br := bufio.NewReader(r)
	var (
		line      []byte
		writeErr  error
		oversized bool
	)
	for {
		chunk, err := br.ReadSlice('\n')
		if len(chunk) > 0 {
			if _, e := transcript.Write(chunk); e != nil && writeErr == nil {
				writeErr = e
			}
			oversized = oversized || len(line)+len(chunk) > maxParsedLineBytes
			if oversized {
				line = nil
			} else {
				line = append(line, chunk...)
			}
		}
		// A full read buffer with no newline in it is this line continuing.
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if len(line) > 0 || writeErr != nil {
			d.record(line, writeErr)
		}
		if err != nil {
			return
		}
		line, writeErr, oversized = nil, nil, false
	}
}

// promptLine is the §7.3 framing: one stream-json user message, then stdin
// closes. Marshalled rather than assembled, so a quote or a newline in the
// prompt cannot break the frame.
func promptLine(prompt string) ([]byte, error) {
	type block struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	type message struct {
		Role    string  `json:"role"`
		Content []block `json:"content"`
	}
	line, err := json.Marshal(struct {
		Type    string  `json:"type"`
		Message message `json:"message"`
	}{
		Type:    "user",
		Message: message{Role: "user", Content: []block{{Type: "text", Text: prompt}}},
	})
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// tailWriter keeps the last stderrTailBytes of what it is given, so a runaway
// stderr costs a fixed amount of memory.
type tailWriter struct {
	buf []byte
}

func (t *tailWriter) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > stderrTailBytes {
		t.buf = t.buf[len(t.buf)-stderrTailBytes:]
		// The cut is a byte offset and can land mid-rune; this tail reaches the
		// ledger's evidence column and the summary email, so it may not carry
		// the trailing bytes of a rune whose start was dropped.
		for len(t.buf) > 0 && !utf8.RuneStart(t.buf[0]) {
			t.buf = t.buf[1:]
		}
	}
	return len(p), nil
}

func (t *tailWriter) String() string { return strings.TrimSpace(string(t.buf)) }

func withTail(detail, tail string) string {
	if tail == "" {
		return detail
	}
	return detail + ": " + tail
}
