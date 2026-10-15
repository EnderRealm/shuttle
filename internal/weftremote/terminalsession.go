package weftremote

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"

	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/vt"
)

const (
	// terminalRetentionBytes is the newest output each session keeps for
	// replay by offset (§9.5).
	terminalRetentionBytes = 4 << 20
	// maxOutputFrame and maxSnapshotFrame bound one frame's decoded bytes.
	maxOutputFrame   = 64 << 10
	maxSnapshotFrame = 1 << 20
	// terminalHangupGrace is how long a hung-up shell has to exit before its
	// group is terminated and killed.
	terminalHangupGrace = 2 * time.Second
	// terminalOutputDrain is how long the end of a session waits for the
	// PTY's last output once its processes are gone.
	terminalOutputDrain = 2 * time.Second
	// frameWriteTimeout bounds one frame's write to a client; one that cannot
	// take it in that time is detached.
	frameWriteTimeout  = 60 * time.Second
	attachPingInterval = 30 * time.Second
	// maxControlFrames bounds the control frames waiting for one client: a
	// client that lets more pile up is not reading and is detached.
	maxControlFrames = 256
)

// retention is the newest terminalRetentionBytes of a session's output, by
// offset from the session's start.
type retention struct {
	buf []byte
	end int64
}

func (r *retention) start() int64 { return r.end - int64(len(r.buf)) }

func (r *retention) write(p []byte) {
	for len(p) > 0 {
		var n int
		if len(r.buf) < terminalRetentionBytes {
			n = min(len(p), terminalRetentionBytes-len(r.buf))
			r.buf = append(r.buf, p[:n]...)
		} else {
			n = copy(r.buf[r.end%terminalRetentionBytes:], p)
		}
		r.end += int64(n)
		p = p[n:]
	}
}

// read copies up to limit retained bytes from offset from, which is within
// the window.
func (r *retention) read(from int64, limit int) []byte {
	out := make([]byte, min(int64(limit), r.end-from))
	i := int(from % terminalRetentionBytes)
	n := copy(out, r.buf[i:min(len(r.buf), i+len(out))])
	copy(out[n:], r.buf)
	return out
}

// attachment is one client's WebSocket on a session. Output is not queued
// per attachment: next is the offset its next output frame starts at, read
// from the session's retention when the client can take it, so a slow client
// costs nothing and blocks nothing; one the retention passed is resynced.
type attachment struct {
	id      string
	conn    *websocket.Conn
	next    int64
	control []map[string]any
	wake    chan struct{}
	ctx     context.Context
	stop    context.CancelFunc
}

func (a *attachment) notify() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// terminalSession is a session this host process owns: its PTY and shell,
// the emulator fed every byte of its output, and its attachments. mu is the
// session lock; every ledger write for the session is made under it, so its
// terminal events are in the order its changes happened.
type terminalSession struct {
	svc          *Service
	id, kind     string
	path         string
	defaultTitle string
	started      chan struct{} // closed once the spawn succeeded or failed
	exited       chan struct{} // closed once the shell is reaped
	readDone     chan struct{}
	closeReq     chan struct{}
	done         chan struct{} // closed once the session is settled in the ledger

	mu         sync.Mutex
	emu        *vt.Terminal
	ring       retention
	cols, rows int
	state      string
	owner      string
	attached   []*attachment
	revision   int64
	closing    bool
	ended      bool
	endFrame   map[string]any
	ptmx       *os.File
	pid        int
	ttyDev     int
	exitCode   int
}

func newTerminalSession(s *Service, row *ledger.WeftTerminal) *terminalSession {
	return &terminalSession{
		svc: s, id: row.ID, kind: row.Kind, path: row.Path, defaultTitle: row.Title,
		started: make(chan struct{}), exited: make(chan struct{}), readDone: make(chan struct{}),
		closeReq: make(chan struct{}), done: make(chan struct{}),
		emu: vt.New(row.Cols, row.Rows), cols: row.Cols, rows: row.Rows, state: row.State, revision: row.Revision,
	}
}

func (ts *terminalSession) logf(format string, args ...any) {
	ts.svc.log.Printf("weft terminal host=%s session=%s: %s", ts.svc.hostID, ts.id, fmt.Sprintf(format, args...))
}

func (ts *terminalSession) title() string {
	if t := ts.emu.Title(); t != "" {
		return t
	}
	return ts.defaultTitle
}

// live is the session as its resource carries it now. The caller holds mu.
func (ts *terminalSession) live() ledger.WeftTerminalState {
	return ledger.WeftTerminalState{
		State: ts.state, Owner: ts.owner, Attachments: len(ts.attached), Cols: ts.cols, Rows: ts.rows,
		Offset: ts.ring.end, RetainedFrom: ts.ring.start(), Title: ts.title(),
	}
}

// record writes the session's state, owner and attachments with its terminal
// event. The caller holds mu.
func (ts *terminalSession) record() {
	ts.svc.stateMu.Lock()
	row, err := ts.svc.store.UpdateWeftTerminal(ts.id, ts.live())
	ts.svc.stateMu.Unlock()
	if err != nil {
		ts.logf("record state %s: %v", ts.state, err)
		return
	}
	ts.revision = row.Revision
}

// overlay is the resource with what the session holds now in place of what
// the ledger last recorded of its size, offsets and title.
func (ts *terminalSession) overlay(resource map[string]any) map[string]any {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.ended {
		return resource
	}
	resource["cols"], resource["rows"] = ts.cols, ts.rows
	resource["offset"], resource["retained_from"] = ts.ring.end, ts.ring.start()
	resource["title"] = ts.title()
	return resource
}

// loginShell is the user's login shell, as $SHELL names it.
func loginShell() string {
	if shell := os.Getenv("SHELL"); filepath.IsAbs(shell) {
		return shell
	}
	return "/bin/zsh"
}

// terminalEnv is the environment the login shell starts from: who the user
// is and the terminal it runs in. The shell's own login files build the rest;
// nothing of the daemon's environment beyond these crosses into it.
func terminalEnv(shell string) []string {
	env := []string{"TERM=xterm-256color", "COLORTERM=truecolor", "SHELL=" + shell}
	for _, key := range []string{"HOME", "USER", "LOGNAME", "PATH", "TMPDIR", "LANG"} {
		if v := os.Getenv(key); v != "" {
			env = append(env, key+"="+v)
		}
	}
	if os.Getenv("LANG") == "" {
		env = append(env, "LANG=en_US.UTF-8")
	}
	return env
}

// Seams over the shell's identity: the kernel's start time for it, and its
// durable record. Vars so a test can make either fail.
var (
	terminalProcessStart  = reap.ProcessStart
	recordTerminalProcess = (*ledger.Store).RecordWeftTerminalProcess
)

// shellLauncher is the command that starts a session's login shell, gated
// on the host: it is the leader of its own session, and it blocks reading
// the pipe it inherits as fd 3, executing shell only once the host writes a
// line there — which the host does only once the launcher's pid and start
// time are durably recorded. On EOF — the host abandoned it, or the daemon
// died — it exits without executing anything, so no shell runs that a
// restart cannot establish. shell is passed as $0, never part of the script;
// exec keeps the launcher's pid, so the recorded pid and start time are the
// shell's. The pipe's write end is returned; its read end is cmd.ExtraFiles[0],
// for the caller to close once the launcher has started.
func shellLauncher(shell string) (*exec.Cmd, *os.File, error) {
	gate, goAhead, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command("/bin/sh", "-c", `IFS= read -r go <&3 || exit 0; exec 3<&-; exec "$0" -l`, shell)
	cmd.ExtraFiles = []*os.File{gate}
	cmd.Env = terminalEnv(shell)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}
	return cmd, goAhead, nil
}

// start spawns the session's login shell, through shellLauncher, as the
// leader of its own session on a PTY of the session's size, records it, lets
// it execute the shell, and starts the goroutines that read its output, reap
// it and settle the session. A claude session types claude into the shell, so
// the shell outlives it.
//
// A launcher whose identity — pid and kernel start time — cannot be read and
// durably recorded is never let through to the shell and is killed before
// start returns: a restart could not establish it gone. It answers the
// launcher's pid when one was spawned, and unresolved says why what it
// started could not be established gone, if it could not.
func (ts *terminalSession) start() (pid int, unresolved string, err error) {
	shell := loginShell()
	ptmx, tty, err := pty.Open()
	if err != nil {
		return 0, "", err
	}
	defer tty.Close()
	if err := pty.Setsize(ptmx, &pty.Winsize{Cols: uint16(ts.cols), Rows: uint16(ts.rows)}); err != nil {
		ptmx.Close()
		return 0, "", err
	}
	info, err := tty.Stat()
	if err != nil {
		ptmx.Close()
		return 0, "", err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		ptmx.Close()
		return 0, "", fmt.Errorf("%s has no device number", tty.Name())
	}
	cmd, goAhead, err := shellLauncher(shell)
	if err != nil {
		ptmx.Close()
		return 0, "", err
	}
	cmd.Dir = ts.path
	cmd.Stdin, cmd.Stdout, cmd.Stderr = tty, tty, tty
	err = cmd.Start()
	cmd.ExtraFiles[0].Close()
	if err != nil {
		goAhead.Close()
		ptmx.Close()
		return 0, "", err
	}
	pid, dev := cmd.Process.Pid, int(st.Rdev)
	abandon := func(cause error) (int, string, error) {
		goAhead.Close()
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = cmd.Wait()
		left := clearTerminal(pid, dev, []int{pid})
		ptmx.Close()
		if len(left) > 0 {
			return pid, fmt.Sprintf("%v", left), cause
		}
		return pid, "", cause
	}
	at, err := terminalProcessStart(pid)
	if err != nil {
		return abandon(fmt.Errorf("pid %d: no kernel start time, so a restart could not establish it: %w", pid, err))
	}
	ts.mu.Lock()
	state := ts.state
	if state == ledger.WeftTerminalStarting {
		ts.state = ledger.WeftTerminalDetached
	}
	ts.svc.stateMu.Lock()
	row, err := recordTerminalProcess(ts.svc.store, ts.id, pid, at.UTC().Format(time.RFC3339Nano), ts.live())
	ts.svc.stateMu.Unlock()
	if err != nil {
		ts.state = state
		ts.mu.Unlock()
		return abandon(fmt.Errorf("record pid %d: %w", pid, err))
	}
	ts.ptmx, ts.pid, ts.ttyDev, ts.revision = ptmx, pid, dev, row.Revision
	ts.mu.Unlock()
	_, err = goAhead.Write([]byte("go\n"))
	goAhead.Close()
	if err != nil {
		ts.logf("let pid %d through to its shell: %v", pid, err)
	}
	go ts.read()
	go func() {
		err := cmd.Wait()
		ts.mu.Lock()
		ts.exitCode = exitCode(err)
		ts.mu.Unlock()
		close(ts.exited)
	}()
	go ts.supervise()
	ts.logf("spawned %s -l (pid %d) in %s at %dx%d", shell, pid, ts.path, ts.cols, ts.rows)
	if ts.kind == "claude" {
		// The launcher reads only its pipe, so this waits in the PTY for the
		// shell it executes.
		if _, err := ptmx.Write([]byte("claude\r")); err != nil {
			ts.logf("type claude: %v", err)
		}
	}
	return pid, "", nil
}

// read feeds every byte the PTY produces, in order, to the emulator and the
// retention, and wakes the attachments. It never waits on a client.
func (ts *terminalSession) read() {
	defer close(ts.readDone)
	buf := make([]byte, 32<<10)
	for {
		n, err := ts.ptmx.Read(buf)
		if n > 0 {
			ts.mu.Lock()
			ts.emu.Write(buf[:n])
			ts.ring.write(buf[:n])
			for _, a := range ts.attached {
				a.notify()
			}
			ts.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

// supervise ends the session when its shell exits or the operator closes it.
func (ts *terminalSession) supervise() {
	closedBy := ledger.WeftClosedByExit
	select {
	case <-ts.exited:
	case <-ts.closeReq:
		closedBy = ledger.WeftClosedByOperator
	}
	ts.finish(closedBy)
}

// foreground is the PTY's foreground process group, or the shell's own when
// the PTY cannot say.
func (ts *terminalSession) foreground() int {
	if pgrp, err := unix.IoctlGetInt(int(ts.ptmx.Fd()), unix.TIOCGPGRP); err == nil && pgrp > 0 {
		return pgrp
	}
	return ts.pid
}

// terminalClearRounds bounds how many times a terminal's session and PTY are
// listed and what is on them terminated before they are declared not
// established empty.
const terminalClearRounds = 5

// clearTerminal establishes gone everything a session's shell started: the
// groups named, then every group with a live member on the PTY dev or in the
// session the shell pid leads, terminated and re-listed until both hold
// nothing — a handler that starts a job in a new group as it is terminated
// is caught by the next listing. It answers why emptiness could not be
// established, nil once it was.
func clearTerminal(pid, dev int, groups []int) []string {
	for round := 0; round < terminalClearRounds; round++ {
		attached, err := ttyGroups(dev)
		if err != nil {
			return []string{err.Error()}
		}
		session, err := reap.SessionGroups(pid)
		if err != nil {
			return []string{err.Error()}
		}
		groups = append(append(groups, attached...), session...)
		if len(groups) == 0 {
			return nil
		}
		var left []string
		seen := map[int]bool{}
		for _, g := range groups {
			if seen[g] {
				continue
			}
			seen[g] = true
			if err := ensureProcessGroupGone(g); err != nil {
				left = append(left, err.Error())
			}
		}
		if len(left) > 0 {
			return left
		}
		groups = nil
	}
	return []string{fmt.Sprintf("session %d still held processes after %d rounds of termination", pid, terminalClearRounds)}
}

// finish ends the session in the order §9.4 requires: the process group hung
// up (on close), the shell reaped, every group on the PTY or in the shell's
// session established gone, re-listed until none is left — what the restart
// pass would check — the output drained, and only then the session closed in the ledger and its
// reservation share released. A group that outlives SIGKILL, or a listing
// that cannot be read, keeps the share.
func (ts *terminalSession) finish(closedBy string) {
	groups := []int{ts.pid}
	if fg := ts.foreground(); fg != ts.pid {
		groups = append(groups, fg)
	}
	if closedBy == ledger.WeftClosedByOperator {
		for _, g := range groups {
			_ = syscall.Kill(-g, syscall.SIGHUP)
		}
	}
	select {
	case <-ts.exited:
	case <-time.After(terminalHangupGrace):
	}
	unresolved := clearTerminal(ts.pid, ts.ttyDev, groups)
	if len(unresolved) == 0 {
		<-ts.exited
	}
	select {
	case <-ts.readDone:
	case <-time.After(terminalOutputDrain):
		ts.logf("the PTY's output did not end once its processes were gone; closing it")
	}
	_ = ts.ptmx.Close()

	ts.mu.Lock()
	var exit *int
	if code := ts.exitCode; closedBy == ledger.WeftClosedByExit && code >= 0 {
		exit = &code
	}
	detail := ""
	if len(unresolved) > 0 {
		detail = fmt.Sprintf("%v", unresolved)
		ts.logf("process groups could not be established gone, so the reservation share is kept: %s", detail)
	}
	ts.svc.stateMu.Lock()
	row, err := ts.svc.store.CloseWeftTerminal(ts.id, closedBy, exit, ts.live(), detail, ts.pid)
	ts.svc.stateMu.Unlock()
	if err != nil {
		ts.logf("record closed: %v", err)
	} else {
		ts.revision = row.Revision
	}
	ts.end(closedBy, exit)
	ts.mu.Unlock()
	ts.logf("closed by %s", closedBy)
	ts.settled()
}

// end marks the session over for its attachments: each sends what output
// remains, then ended. The caller holds mu.
func (ts *terminalSession) end(closedBy string, exit *int) {
	ts.ended = true
	ts.endFrame = ts.frame("ended", map[string]any{"state": ledger.WeftTerminalClosed, "exit_code": nil,
		"closed_by": closedBy})
	if exit != nil {
		ts.endFrame["exit_code"] = *exit
	}
	for _, a := range ts.attached {
		a.notify()
	}
}

// settled hands the ended session's reservation to the queue.
func (ts *terminalSession) settled() {
	ts.svc.forgetTerminal(ts.id)
	close(ts.done)
	ts.svc.dispatch()
}

// failed closes a session whose shell could not be spawned or recorded,
// releasing what it reserved unless something it started could not be
// established gone. pid is the launcher it spawned, 0 for none: recorded
// with the close, so a restart can check what the share is kept for.
func (ts *terminalSession) failed(cause error, unresolved string, pid int) {
	ts.logf("spawn failed: %v", cause)
	if unresolved != "" {
		ts.logf("process groups could not be established gone, so the reservation share is kept: %s", unresolved)
	}
	ts.mu.Lock()
	ts.svc.stateMu.Lock()
	_, err := ts.svc.store.CloseWeftTerminal(ts.id, ledger.WeftClosedByExit, nil, ts.live(), unresolved, pid)
	ts.svc.stateMu.Unlock()
	if err != nil {
		ts.logf("record closed: %v", err)
	}
	ts.end(ledger.WeftClosedByExit, nil)
	ts.mu.Unlock()
	ts.settled()
}

// requestClose starts the operator's close (§9.4) of a session at the
// revision the client was shown.
func (ts *terminalSession) requestClose(revision int64) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.ended || ts.closing {
		return &ledger.WeftRefusal{Code: "session_closed", Message: fmt.Sprintf("terminal %s is already closing", ts.id)}
	}
	if ts.revision != revision {
		return &ledger.WeftRefusal{Code: "revision_conflict",
			Message: fmt.Sprintf("terminal %s is at revision %d and the request was for %d", ts.id, ts.revision, revision)}
	}
	ts.closing = true
	close(ts.closeReq)
	return nil
}

// ---------------------------------------------------------------- frames

func (ts *terminalSession) frame(typ string, fields map[string]any) map[string]any {
	fields["contract_version"], fields["type"], fields["session_id"] = ServiceVersion, typ, ts.id
	return fields
}

// snapshotFrame is the emulator serialized at the current offset. The
// caller holds mu.
func (ts *terminalSession) snapshotFrame() map[string]any {
	data, scrollback := ts.emu.Snapshot(maxSnapshotFrame)
	return ts.frame("snapshot", map[string]any{"offset": ts.ring.end, "cols": ts.cols, "rows": ts.rows,
		"scrollback_lines": scrollback, "data": base64.StdEncoding.EncodeToString(data)})
}

func (ts *terminalSession) ownerFrame(to *attachment) map[string]any {
	role := "viewer"
	if ts.owner == to.id {
		role = "owner"
	}
	return ts.frame("owner", map[string]any{"owner": nullableText(ts.owner), "role": role})
}

func (ts *terminalSession) refusedFrame(code, message string) map[string]any {
	return ts.frame("refused", map[string]any{"code": code, "message": message})
}

// queue hands a control frame to an attachment. The caller holds mu.
func (a *attachment) queue(f map[string]any) {
	if len(a.control) >= maxControlFrames {
		a.stop()
		return
	}
	a.control = append(a.control, f)
	a.notify()
}

// nextFrame is the attachment's next frame: a control frame, or — when the
// retention has passed what it has sent — reset resync and a snapshot, or
// the next output, or once the session ended and every byte is sent, ended.
// last is true for ended.
func (ts *terminalSession) nextFrame(a *attachment) (f map[string]any, last bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	switch {
	case len(a.control) > 0:
		f, a.control = a.control[0], a.control[1:]
		return f, false
	case a.next < ts.ring.start():
		reset := ts.frame("reset", map[string]any{"reason": "resync", "requested": a.next,
			"oldest": ts.ring.start(), "offset": ts.ring.end})
		a.control = append(a.control, ts.snapshotFrame())
		a.next = ts.ring.end
		ts.logf("attachment %s fell behind the retained output; resynced at %d", a.id, a.next)
		return reset, false
	case a.next < ts.ring.end:
		data := ts.ring.read(a.next, maxOutputFrame)
		f = ts.frame("output", map[string]any{"offset": a.next, "data": base64.StdEncoding.EncodeToString(data)})
		a.next += int64(len(data))
		return f, false
	case ts.ended:
		return ts.endFrame, true
	}
	return nil, false
}

// write sends the attachment's frames as it can take them.
func (ts *terminalSession) write(a *attachment) {
	defer a.stop()
	ping := time.NewTicker(attachPingInterval)
	defer ping.Stop()
	for {
		f, last := ts.nextFrame(a)
		if f == nil {
			select {
			case <-a.wake:
			case <-a.ctx.Done():
				return
			case <-ping.C:
				ctx, cancel := context.WithTimeout(a.ctx, frameWriteTimeout)
				err := a.conn.Ping(ctx)
				cancel()
				if err != nil {
					return
				}
			}
			continue
		}
		doc, err := json.Marshal(f)
		if err != nil {
			ts.logf("encode %s frame: %v", f["type"], err)
			return
		}
		ctx, cancel := context.WithTimeout(a.ctx, frameWriteTimeout)
		err = a.conn.Write(ctx, websocket.MessageText, doc)
		cancel()
		if err != nil {
			return
		}
		if last {
			_ = a.conn.Close(websocket.StatusNormalClosure, closeReason("session ended"))
			return
		}
	}
}

// attach joins a client to the session (§9.5). The first attachment with no
// owner owns input and size; later ones are viewers. Under the session lock
// it either resumes output from an offset inside the retention, or — with no
// offset, or one the retention passed — serializes the snapshot and starts
// output at that same offset.
func (ts *terminalSession) attach(a *attachment, offset int64, hasOffset bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.ended {
		// It ended since the attach was checked: attached, then ended.
		a.queue(ts.frame("attached", map[string]any{"attachment_id": a.id, "role": "viewer",
			"owner": nil, "cols": ts.cols, "rows": ts.rows}))
		a.next = ts.ring.end
		return
	}
	role := "viewer"
	if ts.owner == "" {
		ts.owner, role = a.id, "owner"
	}
	ts.attached = append(ts.attached, a)
	ts.state = ledger.WeftTerminalLive
	ts.record()
	a.queue(ts.frame("attached", map[string]any{"attachment_id": a.id, "role": role,
		"owner": nullableText(ts.owner), "cols": ts.cols, "rows": ts.rows}))
	switch {
	case hasOffset && offset >= ts.ring.start():
		a.next = offset
	case hasOffset:
		a.queue(ts.frame("reset", map[string]any{"reason": "retention", "requested": offset,
			"oldest": ts.ring.start(), "offset": ts.ring.end}))
		a.queue(ts.snapshotFrame())
		a.next = ts.ring.end
	default:
		a.queue(ts.snapshotFrame())
		a.next = ts.ring.end
	}
	ts.logf("attachment %s attached as %s at offset %d", a.id, role, a.next)
}

// detach removes the attachment. The session never closes for it; an
// owner's detach leaves the session ownerless.
func (ts *terminalSession) detach(a *attachment) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	i := -1
	for j, b := range ts.attached {
		if b == a {
			i = j
		}
	}
	if i < 0 {
		return
	}
	ts.attached = append(ts.attached[:i], ts.attached[i+1:]...)
	wasOwner := ts.owner == a.id
	if wasOwner {
		ts.owner = ""
	}
	ts.logf("attachment %s detached", a.id)
	if ts.ended {
		return
	}
	if len(ts.attached) == 0 {
		ts.state = ledger.WeftTerminalDetached
	}
	ts.record()
	if wasOwner {
		for _, b := range ts.attached {
			b.queue(ts.ownerFrame(b))
		}
	}
}

// takeOver makes the attachment the owner; every attachment is told.
func (ts *terminalSession) takeOver(a *attachment) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.ended {
		a.queue(ts.refusedFrame("session_closed", fmt.Sprintf("terminal %s is closed", ts.id)))
		return
	}
	ts.owner = a.id
	ts.record()
	for _, b := range ts.attached {
		b.queue(ts.ownerFrame(b))
	}
	ts.logf("attachment %s took over", a.id)
}

// ownedBy answers whether the attachment may type or resize, queueing the
// refusal when it may not: what belongs to the owner, and verb is what taking
// over lets the attachment do.
func (ts *terminalSession) ownedBy(a *attachment, what, verb string) bool {
	if ts.ended {
		a.queue(ts.refusedFrame("session_closed", fmt.Sprintf("terminal %s is closed", ts.id)))
		return false
	}
	if ts.owner == a.id {
		return true
	}
	holder := "no attachment"
	if ts.owner != "" {
		holder = ts.owner
	}
	a.queue(ts.refusedFrame("not_owner", fmt.Sprintf("%s belongs to %s; take over to %s", what, holder, verb)))
	return false
}

// input writes the owner's bytes to the PTY.
func (ts *terminalSession) input(a *attachment, data []byte) {
	<-ts.started
	ts.mu.Lock()
	ok := ts.ownedBy(a, "input", "type")
	ptmx := ts.ptmx
	ts.mu.Unlock()
	if !ok || ptmx == nil {
		return
	}
	if _, err := ptmx.Write(data); err != nil {
		ts.logf("write input: %v", err)
	}
}

// resize applies the owner's window size with TIOCSWINSZ and sends SIGWINCH
// to the foreground process group even when the size did not change, so a
// full-screen program repaints at the owner's size (§9.5).
func (ts *terminalSession) resize(a *attachment, cols, rows int) {
	<-ts.started
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if !ts.ownedBy(a, "the size", "resize") || ts.ptmx == nil {
		return
	}
	ts.cols, ts.rows = cols, rows
	ts.emu.Resize(cols, rows)
	if err := pty.Setsize(ts.ptmx, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)}); err != nil {
		ts.logf("resize to %dx%d: %v", cols, rows, err)
		return
	}
	if err := syscall.Kill(-ts.foreground(), syscall.SIGWINCH); err != nil && !errors.Is(err, syscall.ESRCH) {
		ts.logf("SIGWINCH: %v", err)
	}
}
