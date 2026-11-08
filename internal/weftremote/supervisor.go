package weftremote

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/keychain"
	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/sandbox"
)

const workerPath = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Users/steve/.local/bin"

var segmentKillGrace = 5 * time.Second

const segmentKillPoll = 50 * time.Millisecond

var afterWorkerSpawn = func() {}

// containSession is the sweep of what is left of a segment's worker session.
// A var so a test can watch it, or drive a session that will not empty.
var containSession = reap.ContainSession

type activeSegment struct {
	stdin    io.WriteCloser
	contract string
	pgid     int
	done     chan struct{}
	force    chan struct{}
	// mu orders the commands written to stdin; commands records each one in
	// the segment's run directory.
	mu       sync.Mutex
	commands io.Writer
}

// send writes one command to the worker's stdin (§11.4) and records it.
func (a *activeSegment) send(command map[string]any) error {
	command["contract_version"] = a.contract
	doc, err := json.Marshal(command)
	if err != nil {
		return err
	}
	doc = append(doc, '\n')
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.commands != nil {
		_, _ = a.commands.Write(doc)
	}
	_, err = a.stdin.Write(doc)
	return err
}

// Supervisor owns every worker process independently of HTTP connections.
type Supervisor struct {
	cfg      *config.Config
	store    *ledger.Store
	home     string
	log      *log.Logger
	stateMu  *sync.RWMutex
	mu       sync.Mutex
	active   map[string]*activeSegment
	stopping map[string]bool
	// findKey is the test seam over keychain.Find; nil reads the real Keychain.
	findKey func(string) (string, error)
	// settled runs once a segment is settled — reaped, failed or cancelled
	// before spawn — which is when its slot and reservation are free.
	settled func()
	// reapDispatch is what a reap commit is settled under: dispatch over a
	// fresh tk read, so the reaped run is evaluated for host cleanup in that
	// commit (§6.8). Nil settles under dispatchBase, which cleans nothing.
	reapDispatch func() ledger.WeftDispatch
	// hostID joins every line logged about a segment (weft-service/v2 §4.2).
	hostID string
	// worker is the last probe of worker_bin (§2.6); probeMu serializes
	// probing so one change is probed once.
	worker  atomic.Pointer[workerInfo]
	probeMu sync.Mutex
	// output wakes the output streams when a chunk is retained.
	output *broadcast
	// sharedRoot is the root a new run's worktree is derived under, whose
	// worktrees/ is every segment's --worktree-root: config.HostedSharedRoot,
	// which a test moves.
	sharedRoot string
	// killUser kills every process of a segment's agent user; a test hands
	// it a process table of its own.
	killUser func(user string) ([]reap.UserProcess, error)
}

func NewSupervisor(cfg *config.Config, store *ledger.Store, home string, logger *log.Logger, locks ...*sync.RWMutex) *Supervisor {
	stateMu := &sync.RWMutex{}
	if len(locks) > 0 && locks[0] != nil {
		stateMu = locks[0]
	}
	return &Supervisor{
		cfg: cfg, store: store, home: home, log: logger, stateMu: stateMu,
		active: make(map[string]*activeSegment), stopping: make(map[string]bool), output: newBroadcast(),
		sharedRoot: config.HostedSharedRoot, killUser: reap.KillUser,
	}
}

func (s *Supervisor) Start(runID string) {
	if err := s.run(runID); err != nil {
		s.log.Printf("weft run=%s: %v", runID, err)
	}
	if s.settled != nil {
		s.settled()
	}
}

func (s *Supervisor) Stop(runID string) {
	s.mu.Lock()
	s.stopping[runID] = true
	active := s.active[runID]
	s.mu.Unlock()
	if active == nil {
		return
	}
	_ = active.send(map[string]any{"command": "stop"})
	select {
	case <-active.done:
		return
	case <-time.After(10 * time.Second):
	}
	_ = syscall.Kill(-active.pgid, syscall.SIGTERM)
	select {
	case <-active.done:
		return
	case <-time.After(5 * time.Second):
	}
	_ = syscall.Kill(-active.pgid, syscall.SIGKILL)
	select {
	case active.force <- struct{}{}:
	default:
	}
}

// isStopping reports whether an operator stop of the run is in flight.
func (s *Supervisor) isStopping(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopping[runID]
}

// landServed is whether a worker can land and reconcile (§2.3): the host
// passes --land to every segment while it can.
func landServed(w *workerInfo) bool { return w.has("land") && w.has("reconcile") }

// dispatchBase is what dispatch mints runs and claims segments under: the
// last probe, as the capabilities a client reads report it. It never probes,
// so it is safe inside a ledger transaction.
func (s *Supervisor) dispatchBase() ledger.WeftDispatch {
	return ledger.WeftDispatch{
		SharedRoot: s.sharedRoot, RunDirRoot: filepath.Join(s.home, "runs", ledger.WeftAgent),
		AgentUsers: s.cfg.Weft.AgentUsers, Land: landServed(s.worker.Load()),
	}
}

// worktreeRoot is the --worktree-root every segment is launched with.
func (s *Supervisor) worktreeRoot() string { return filepath.Join(s.sharedRoot, sandbox.WorktreesDir) }

// hosted reports whether the run's worktree is under the worktree root,
// where agent users can reach it; a run launched before they ran its
// segments has its worktree under weft_root.
func (s *Supervisor) hosted(run *ledger.WeftRun) bool {
	return strings.HasPrefix(run.Worktree, s.worktreeRoot()+string(filepath.Separator))
}

// maintenance reports whether a segment kind launches no model: it runs as
// the operator, holds no agent user and holds no slot (§11.1).
func maintenance(kind string) bool {
	return oneOf(kind, ledger.WeftSegmentReconcile, ledger.WeftSegmentCleanup, ledger.WeftSegmentArchive)
}

// run runs the run's latest segment to its settlement, then starts the
// segments that settlement claimed: a reconcile for a landing it lost, or
// the run a fresh entry's archive minted.
func (s *Supervisor) run(runID string) error {
	spawn, err := s.runSegment(runID)
	for _, id := range spawn {
		go s.Start(id)
	}
	return err
}

// launch is what a segment's worker is started with.
type launch struct {
	argv, env []string
	contract  string
	worker    *workerInfo
	land      bool
}

func (s *Supervisor) runSegment(runID string) ([]string, error) {
	s.stateMu.RLock()
	run, err := s.store.GetWeftRun(runID)
	s.stateMu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(run.Segments) == 0 {
		return nil, fmt.Errorf("run has no segment")
	}
	segment := run.Segments[len(run.Segments)-1]
	if run.Stopped && run.Phase == ledger.WeftPhaseEnded && !maintenance(segment.Kind) {
		s.stateMu.Lock()
		err := s.store.CancelQueuedWeftSegment(run.ID, segment.Segment)
		s.stateMu.Unlock()
		s.mu.Lock()
		delete(s.stopping, run.ID)
		s.mu.Unlock()
		return nil, err
	}
	if !segment.RunDir.Valid {
		return nil, fmt.Errorf("segment %d has no run directory", segment.Segment)
	}
	runDir := segment.RunDir.String
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	if refusal := s.worktreeMoved(run, segment); refusal != "" {
		// The worker would act on another worktree than the one the operator
		// authorized: the entry is refused and nothing is launched.
		// It is a refusal, not host trouble: it counts toward no failure
		// streak.
		s.log.Printf("weft segment %s: refused before spawn: %s", segmentIDs(s.hostID, run.ID, segment, ""), refusal)
		s.stateMu.Lock()
		held, err := s.store.RefuseWeftMaintenance(run.ID, segment.Segment, refusal)
		s.stateMu.Unlock()
		if held {
			s.log.Printf("weft segment %s: host cleanup hold set, the host will not queue a cleanup of this run again",
				segmentIDs(s.hostID, run.ID, segment, ""))
		}
		return nil, err
	}
	var l *launch
	if maintenance(segment.Kind) {
		l, err = s.prepareMaintenance(run, segment, runDir)
	} else {
		l, err = s.preparePipeline(run, segment, runDir)
	}
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	argv, env, contract, worker := l.argv, l.env, l.contract, l.worker
	if err := writeRunRecord(runDir, run, segment, contract, argv, env, 0); err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	eventsFile, err := os.OpenFile(filepath.Join(runDir, "events.ndjson"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	defer eventsFile.Close()
	stderrFile, err := os.OpenFile(filepath.Join(runDir, "stderr.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	defer stderrFile.Close()
	commandsFile, err := os.OpenFile(filepath.Join(runDir, "commands.ndjson"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	defer commandsFile.Close()

	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	defer gateWriter.Close()
	gatedArgv := append([]string{"-c", `IFS= read -r _ <&3 || exit 125; exec "$@"`, "shuttle-worker-gate"}, argv...)
	cmd := exec.Command("/bin/sh", gatedArgv...)
	cmd.Dir = run.Repo
	cmd.Env = env
	// A session leader leads its own process group too. Its session is what
	// holds the git and tk it starts once they lead groups of their own.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{gateReader}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		gateReader.Close()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	stdout, stdoutChild, err := os.Pipe()
	if err != nil {
		gateReader.Close()
		stdin.Close()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	stderr, stderrChild, err := os.Pipe()
	if err != nil {
		gateReader.Close()
		stdin.Close()
		stdout.Close()
		stdoutChild.Close()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	cmd.Stdout = stdoutChild
	cmd.Stderr = stderrChild
	if err := cmd.Start(); err != nil {
		gateReader.Close()
		stdin.Close()
		stdout.Close()
		stdoutChild.Close()
		stderr.Close()
		stderrChild.Close()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	gateReader.Close()
	if err := writeRunRecord(runDir, run, segment, contract, argv, env, cmd.Process.Pid); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		stdin.Close()
		stdout.Close()
		stdoutChild.Close()
		stderr.Close()
		stderrChild.Close()
		_ = cmd.Wait()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	afterWorkerSpawn()
	// The child inherited these write ends. Closing the parent's copies makes
	// EOF observable without letting cmd.Wait close the readers under drains.
	stdoutChild.Close()
	stderrChild.Close()
	defer stdout.Close()
	defer stderr.Close()
	active := &activeSegment{
		stdin: stdin, contract: contract, pgid: cmd.Process.Pid, done: make(chan struct{}), force: make(chan struct{}, 1),
		commands: commandsFile,
	}
	s.mu.Lock()
	s.active[run.ID] = active
	stopping := s.stopping[run.ID]
	s.mu.Unlock()
	// An operator's stop lasts the attempt, so it holds for the reconcile of
	// it: the worker observes and records, and takes no step (§6.6).
	if stopping || (run.Stopped && segment.Kind == ledger.WeftSegmentReconcile) {
		_ = active.send(map[string]any{"command": "stop"})
	}
	defer func() {
		stdin.Close()
		s.mu.Lock()
		if s.active[run.ID] == active {
			delete(s.active, run.ID)
			delete(s.stopping, run.ID)
		}
		s.mu.Unlock()
		close(active.done)
	}()
	s.stateMu.Lock()
	err = s.store.MarkWeftRunning(run.ID, segment.Segment, cmd.Process.Pid, contract)
	s.stateMu.Unlock()
	if err != nil {
		gateWriter.Close()
		_ = cmd.Wait()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	landing := ""
	if l.land {
		landing = " to land"
	}
	s.log.Printf("weft segment %s: launched %s segment%s, pid %d, under %s",
		segmentIDs(s.hostID, run.ID, segment, ""), segment.Kind, landing, cmd.Process.Pid, contract)
	if _, err := gateWriter.Write([]byte("run\n")); err != nil {
		gateWriter.Close()
		_ = cmd.Wait()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, fmt.Errorf("release worker start gate: %w", err))
	}
	gateWriter.Close()
	stderrDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(stderrFile, stderr)
		close(stderrDone)
	}()
	eventCh := make(chan workerRead, 1)
	go func() { eventCh <- s.readEvents(run, segment, contract, worker, l.land, active, stdout, eventsFile) }()
	// The worker is not reaped until its session is swept: its zombie holds
	// the pid that names the session.
	exitCh := make(chan error, 1)
	go func() { exitCh <- waitExited(cmd.Process.Pid) }()

	timeoutSeconds := s.cfg.Weft.SegmentTimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = 21600
	}
	timer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	defer timer.Stop()
	timeoutC := timer.C
	var (
		exitErr    error
		read       workerRead
		gotWait    bool
		gotRead    bool
		failClass  string
		failDetail string
		killC      <-chan time.Time
	)
	for !gotWait || !gotRead {
		select {
		case exitErr = <-exitCh:
			gotWait = true
		case read = <-eventCh:
			gotRead = true
			if read.err != nil && !gotWait {
				failClass, failDetail = ledger.ClassProtocol, read.err.Error()
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
				killC = time.After(5 * time.Second)
			}
		case <-timeoutC:
			failClass = ledger.ClassStuck
			failDetail = fmt.Sprintf("segment_timeout_seconds %d elapsed", timeoutSeconds)
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			killC = time.After(5 * time.Second)
			timeoutC = nil
		case <-killC:
			if !gotWait {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			if !gotRead {
				_ = stdout.Close()
			}
			killC = nil
		case <-active.force:
			if failClass == "" {
				failClass, failDetail = ledger.ClassStuck, "worker did not stop within 15 seconds"
			}
			if !gotRead {
				_ = stdout.Close()
			}
		}
	}
	// The agent's processes share the worker's process group and session,
	// where the operator's signals do not reach them: the group and session
	// sweeps would outlast their SIGKILL. So the user goes first, and again
	// once the operator's processes are gone, since one of those could have
	// launched another as the user in between.
	if err := s.containUser(run, segment); err != nil {
		return nil, err
	}
	if err := ensureProcessGroupGone(cmd.Process.Pid); err != nil {
		return nil, err
	}
	if exitErr != nil {
		return nil, fmt.Errorf("weft segment %s: %w; reservation retained", segmentIDs(s.hostID, run.ID, segment, ""), exitErr)
	}
	if err := s.containSession(run, segment, cmd.Process.Pid); err != nil {
		return nil, err
	}
	if err := s.containUser(run, segment); err != nil {
		return nil, err
	}
	waitErr := cmd.Wait()
	select {
	case <-stderrDone:
	case <-time.After(segmentKillGrace):
		_ = stderr.Close()
		<-stderrDone
	}
	exitCode := exitCode(waitErr)
	if failClass == "" && read.err != nil {
		failClass, failDetail = ledger.ClassProtocol, read.err.Error()
	}
	if failClass == "" && !read.terminal {
		switch {
		case exitCode == 0:
			failClass, failDetail = ledger.ClassNoResult, "worker exited 0 with no terminal event"
		case exitCode == 64:
			failClass, failDetail = ledger.ClassProtocol, "worker exited 64 with no terminal event"
		default:
			failClass, failDetail = ledger.ClassDied, fmt.Sprintf("worker exited %d with no terminal event", exitCode)
		}
	}
	if failClass == "" && exitCode != 0 {
		failClass, failDetail = ledger.ClassDied, fmt.Sprintf("worker exited %d after a terminal event", exitCode)
	}
	if failClass != "" {
		s.log.Printf("weft segment %s: failed %s, exit %d: %s",
			segmentIDs(s.hostID, run.ID, segment, read.stage), failClass, exitCode, failDetail)
	} else {
		s.log.Printf("weft segment %s: settled after its terminal event", segmentIDs(s.hostID, run.ID, segment, read.stage))
	}
	spawn, err := s.settle(run, segment, failClass, failDetail, read.archive)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"contract_version": contract, "terminal": read.terminal,
		"failure_class": nullableText(failClass), "detail": nullableText(failDetail),
		"exit_code": exitCode,
	}
	doc, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(filepath.Join(runDir, "result.json"), doc, 0o600); err != nil {
		return spawn, err
	}
	return spawn, nil
}

// preparePipeline is a launch, answer or resume segment's preflight: the
// segment's agent user and a worker that can launch as it, a worktree that
// user can reach, the answer file written, and --land passed while the worker
// can land (§7.1).
func (s *Supervisor) preparePipeline(run *ledger.WeftRun, segment ledger.WeftSegment, runDir string) (*launch, error) {
	if !segment.AgentUser.Valid {
		return nil, fmt.Errorf("segment %d holds no agent user: it was claimed before hosted segments ran as agent users", segment.Segment)
	}
	if !s.hosted(run) {
		return nil, fmt.Errorf("run %s's worktree %s is not under %s, where agent users can reach it; start the run fresh", run.ID, run.Worktree, s.worktreeRoot())
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	if err := executable(s.cfg.Weft.WorkerBin); err != nil {
		return nil, err
	}
	l := &launch{worker: s.refreshWorker(), contract: WorkerContractV2}
	if !l.worker.has("agent_user") {
		return nil, fmt.Errorf("%s does not speak %s with the agent_user capability", s.cfg.Weft.WorkerBin, WorkerContractV2)
	}
	l.land = landServed(l.worker)
	oauthToken, err := s.oauthToken()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(sandbox.SegmentTmpDir(runDir), 0o700); err != nil {
		return nil, err
	}
	answerPath, err := s.writeAnswer(run, segment, runDir, l.contract)
	if err != nil {
		return nil, err
	}
	l.argv = s.argv(run, segment, l.contract, answerPath, l.land)
	l.env = s.environ(homeDir, runDir, oauthToken)
	return l, nil
}

// prepareMaintenance is a reconcile, cleanup or archive segment's preflight
// (§11.1): it launches no model, so it runs as the operator with no agent
// user, needing only a weft-worker/v2 worker with its kind's capability.
func (s *Supervisor) prepareMaintenance(run *ledger.WeftRun, segment ledger.WeftSegment, runDir string) (*launch, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	if err := executable(s.cfg.Weft.WorkerBin); err != nil {
		return nil, err
	}
	l := &launch{worker: s.refreshWorker(), contract: WorkerContractV2}
	if !l.worker.has(segment.Kind) {
		return nil, fmt.Errorf("%s does not speak %s with the %s capability", s.cfg.Weft.WorkerBin, WorkerContractV2, segment.Kind)
	}
	oauthToken, err := s.oauthToken()
	if err != nil {
		return nil, err
	}
	l.argv = []string{
		s.cfg.Weft.WorkerBin, segment.Kind, "--contract", WorkerContractV2,
		"--run", run.ID, "--segment", fmt.Sprint(segment.Segment),
		"--ticket", run.Ticket, "--repo", run.Repo, "--weft-root", s.cfg.Weft.Root,
	}
	// A run whose segments were launched as agent users has its worktree and
	// base record where its maintenance can find them only under the same
	// --worktree-root (§11.1); one launched before has them under weft_root.
	if s.hosted(run) {
		l.argv = append(l.argv, "--worktree-root", s.worktreeRoot())
	}
	switch segment.Kind {
	case ledger.WeftSegmentReconcile:
		l.argv = append(l.argv, "--worker-run", latestWorkerBefore(run, segment.Segment))
		if commit, base := reconcileCommit(run); commit != "" {
			l.argv = append(l.argv, "--commit", commit, "--base", base)
		}
	case ledger.WeftSegmentArchive:
		s.stateMu.RLock()
		head, err := s.store.WeftArchiveHead(run.ID)
		s.stateMu.RUnlock()
		if err != nil {
			return nil, err
		}
		l.argv = append(l.argv, "--archive-ref", ledger.WeftArchiveRef(run.ID), "--head", head)
	}
	l.env = s.environ(homeDir, runDir, oauthToken)
	return l, nil
}

// worktreeMoved is why a cleanup or archive may not run: the worktree it
// derives as configured now is not the run's, which the operator authorized.
// "" when it is. A run launched as agent users derives under the shared root,
// one launched before under weft_root.
func (s *Supervisor) worktreeMoved(run *ledger.WeftRun, segment ledger.WeftSegment) string {
	if segment.Kind != ledger.WeftSegmentCleanup && segment.Kind != ledger.WeftSegmentArchive {
		return ""
	}
	root := s.cfg.Weft.Root
	if s.hosted(run) {
		root = s.sharedRoot
	}
	worktree, err := sandbox.WorktreePath(root, run.Repo, run.Ticket)
	switch {
	case err != nil:
		return fmt.Sprintf("not launched: the %s worktree under %s cannot be derived: %v", segment.Kind, root, err)
	case worktree != run.Worktree:
		return fmt.Sprintf("not launched: under %s the %s would act on %s, not run %s's worktree %s",
			root, segment.Kind, worktree, run.ID, run.Worktree)
	}
	return ""
}

// reconcileCommit is the commit and base a reconcile may land: only the head
// the host recorded graded finished (§7.4). Without it the reconcile
// observes and lands nothing.
func reconcileCommit(run *ledger.WeftRun) (commit, base string) {
	if run.Outcome.String != "finished" || !run.Head.Valid || !run.Base.Valid {
		return "", ""
	}
	return run.Head.String, run.Base.String
}

// settle records a reaped segment: completed after its terminal event, or
// failed with class. It answers the runs whose segments that claimed: the
// run of a lost landing, whose reconcile was dispatched under its row, or the
// run a fresh entry's archive minted.
func (s *Supervisor) settle(run *ledger.WeftRun, segment ledger.WeftSegment, class, detail string, archive *ledger.WeftArchiveReport) ([]string, error) {
	ids := segmentIDs(s.hostID, run.ID, segment, "")
	d := s.dispatchBase()
	if s.reapDispatch != nil {
		d = s.reapDispatch()
	}
	if segment.Kind == ledger.WeftSegmentArchive {
		s.stateMu.Lock()
		spawn, changes, err := s.store.SettleWeftArchive(run.ID, segment.Segment, archive, class, detail, d, archiveHead)
		s.stateMu.Unlock()
		for _, c := range changes {
			s.log.Printf("weft archive %s entry=%s: %s %s (%s)", ids, c.EntryID, c.Action, c.RunID, c.Detail)
		}
		return spawn, err
	}
	s.stateMu.Lock()
	reconcile, spawn, changes, err := s.store.SettleWeftSegment(run.ID, segment.Segment, class, detail, d)
	s.stateMu.Unlock()
	for _, c := range changes {
		line := fmt.Sprintf("weft queue %s entry=%s: %s %s %s", ids, c.EntryID, c.Action, c.Kind, c.Ticket)
		if c.Detail != "" {
			line += " (" + c.Detail + ")"
		}
		s.log.Print(line)
	}
	if err != nil || !reconcile {
		return spawn, err
	}
	s.log.Printf("weft landing %s: the landing segment ended without its terminal event; reconcile dispatched under its row", ids)
	return append(spawn, run.ID), nil
}

// ensureProcessGroupGone closes the same lifecycle boundary the repository
// reservation protects. The leader being reaped is not enough: a model or
// anchor child may still hold the worker's process group and sandbox access.
func ensureProcessGroupGone(pgid int) error {
	live, err := processGroupLive(pgid)
	if err != nil {
		return err
	}
	if !live {
		return nil
	}
	err = syscall.Kill(-pgid, syscall.SIGTERM)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("terminate worker process group %d: %w", pgid, err)
	}
	if waitProcessGroupGone(pgid, segmentKillGrace) {
		return nil
	}
	err = syscall.Kill(-pgid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("kill worker process group %d: %w", pgid, err)
	}
	if !waitProcessGroupGone(pgid, segmentKillGrace) {
		return fmt.Errorf("worker process group %d outlived SIGKILL; reservation retained", pgid)
	}
	return nil
}

// containUser is the rest of that boundary for a segment that runs as an
// agent user: every process of the user is killed, which reaches a
// descendant that called setsid. The user is the segment's alone, so nothing
// of it is another's. Until the kill establishes that none is left the
// segment is not settled, so its user and its reservation are retained.
func (s *Supervisor) containUser(run *ledger.WeftRun, segment ledger.WeftSegment) error {
	if !segment.AgentUser.Valid {
		return nil
	}
	ids := segmentIDs(s.hostID, run.ID, segment, "")
	found, err := s.killUser(segment.AgentUser.String)
	if len(found) > 0 {
		names := make([]string, len(found))
		for i, p := range found {
			names[i] = p.String()
		}
		s.log.Printf("weft segment %s: agent user %s: killed %d process(es) %s", ids, segment.AgentUser.String, len(found), strings.Join(names, " "))
	}
	if err != nil {
		return fmt.Errorf("weft segment %s: containment: %w; reservation retained", ids, err)
	}
	return nil
}

// containSession is the rest of that boundary for every segment kind: the
// git and tk a worker runs unsandboxed lead process groups of their own in
// the session the worker leads, so a worker that dies leaves them running — a
// landing step's push still moving the remote a reconcile would observe. sid
// is the worker's pid, not reaped yet. Until the session holds nothing the
// segment is not settled, so no reconcile is dispatched and its reservation
// is retained; the worker's zombie is kept with it, holding the session id.
func (s *Supervisor) containSession(run *ledger.WeftRun, segment ledger.WeftSegment, sid int) error {
	ids := segmentIDs(s.hostID, run.ID, segment, "")
	c, err := containSession(sid)
	if c.Found() {
		s.log.Printf("weft segment %s: session containment %s", ids, c)
	}
	if err != nil {
		return fmt.Errorf("weft segment %s: session containment: %w; reservation retained", ids, err)
	}
	return nil
}

func waitProcessGroupGone(pgid int, grace time.Duration) bool {
	deadline := time.Now().Add(grace)
	for {
		live, err := processGroupLive(pgid)
		if err == nil && !live {
			return true
		}
		if err != nil {
			return false
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(segmentKillPoll)
	}
}

func (s *Supervisor) failBeforeSpawn(run *ledger.WeftRun, segment ledger.WeftSegment, class string, cause error) ([]string, error) {
	s.log.Printf("weft segment %s: failed %s before spawn: %v", segmentIDs(s.hostID, run.ID, segment, ""), class, cause)
	// An archive that never ran wrote nothing: its fresh entry is refused,
	// never re-queued to fail again.
	var archive *ledger.WeftArchiveReport
	if segment.Kind == ledger.WeftSegmentArchive {
		archive = &ledger.WeftArchiveReport{Detail: cause.Error()}
	}
	spawn, err := s.settle(run, segment, class, cause.Error(), archive)
	if err != nil {
		return spawn, fmt.Errorf("%v; settle segment: %w", cause, err)
	}
	return spawn, cause
}

func executable(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%q is not an absolute executable path", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("%s is not executable", path)
	}
	return nil
}

// argv is a pipeline segment's command line: as its agent user, with --land
// while the host lands (§7.1, §11.6). It carries no credential: argv is
// visible to every user on the machine, the agents included.
func (s *Supervisor) argv(run *ledger.WeftRun, segment ledger.WeftSegment, contract, answerPath string, land bool) []string {
	argv := []string{
		s.cfg.Weft.WorkerBin, "segment", "--contract", contract,
		"--run", run.ID, "--segment", fmt.Sprint(segment.Segment),
		"--ticket", run.Ticket, "--repo", run.Repo, "--weft-root", s.cfg.Weft.Root,
		"--agent-user", segment.AgentUser.String, "--worktree-root", s.worktreeRoot(),
	}
	if land {
		argv = append(argv, "--land")
	}
	resumed := latestWorkerBefore(run, segment.Segment)
	if resumed != "" {
		argv = append(argv, "--resume", resumed)
	}
	if answerPath != "" && resumed != "" {
		argv = append(argv, "--answer-file", answerPath)
	}
	return argv
}

// oauthToken reads the segment's Claude credential. An agent user cannot read
// the operator's login Keychain, so the subscription token the CLI keeps
// there has to reach the worker by environment, and the agent through the
// sudoers env_keep; the error names the item and never the value.
func (s *Supervisor) oauthToken() (string, error) {
	item := s.cfg.WeftOAuthKeychainItem()
	find := keychain.Find
	if s.findKey != nil {
		find = s.findKey
	}
	token, err := find(item)
	if err != nil {
		return "", fmt.Errorf("weft.oauth_keychain_item: %w", err)
	}
	return token, nil
}

// environ builds the worker's from-scratch environment (§4.1). The OAuth token
// is the one secret in it: run.json records names only, and nothing else
// under the run directory is written from this slice.
func (s *Supervisor) environ(homeDir, runDir, oauthToken string) []string {
	user := filepath.Base(homeDir)
	return []string{
		"PATH=" + workerPath, "HOME=" + homeDir, "USER=" + user, "LOGNAME=" + user,
		"TMPDIR=" + sandbox.SegmentTmpDir(runDir), "CLAUDE_CODE_TMPDIR=" + sandbox.SegmentTmpDir(runDir),
		"LOOM_HOME=" + s.cfg.Paths.LoomHome, "TZ=" + s.cfg.Timezone, "LANG=en_US.UTF-8",
		"DISABLE_TELEMETRY=1", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1",
		"CLAUDE_CODE_OAUTH_TOKEN=" + oauthToken,
	}
}

// writeAnswer writes the segment's answer file: v1 §4.1's, or
// answer-file.json's under weft-worker/v2, which adds the answer's id.
func (s *Supervisor) writeAnswer(run *ledger.WeftRun, segment ledger.WeftSegment, runDir, contract string) (string, error) {
	if !segment.AnswerID.Valid {
		return "", nil
	}
	s.stateMu.RLock()
	answer, err := s.store.WeftAnswer(segment.AnswerID.String)
	if err != nil {
		s.stateMu.RUnlock()
		return "", err
	}
	question, err := s.store.WeftQuestion(answer.QuestionID)
	s.stateMu.RUnlock()
	if err != nil {
		return "", err
	}
	path := filepath.Join(runDir, "answer.json")
	fields := map[string]any{
		"contract_version": contract, "question_id": question.ID,
		"question": question.Text, "answer": answer.Text,
	}
	if contract == WorkerContractV2 {
		fields["answer_id"] = answer.ID
	}
	doc, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func writeRunRecord(runDir string, run *ledger.WeftRun, segment ledger.WeftSegment, contract string, argv, env []string, pid int) error {
	names := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		names = append(names, name)
	}
	var recordedPID any
	if pid > 0 {
		recordedPID = pid
	}
	doc, err := json.MarshalIndent(map[string]any{
		"contract_version": contract, "run_id": run.ID, "segment": segment.Segment,
		"kind": segment.Kind, "ticket": run.Ticket, "argv": argv, "environment": names,
		"claimed_at": segment.ClaimedAt, "pid": recordedPID,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, "run.json"), doc, 0o600)
}

func latestWorkerBefore(run *ledger.WeftRun, segment int) string {
	var latest string
	for _, prior := range run.Segments {
		if prior.Segment >= segment {
			break
		}
		if prior.WorkerRunID.Valid {
			latest = prior.WorkerRunID.String
		}
	}
	return latest
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	return -1
}

func nullSQL(value *string) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *value, Valid: true}
}

func nullInt64(value *int64) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}

func text(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
