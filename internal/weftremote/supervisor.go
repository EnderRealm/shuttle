package weftremote

import (
	"context"
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
	"shuttle/internal/sandbox"
)

const workerPath = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Users/steve/.local/bin"

var segmentKillGrace = 5 * time.Second

const segmentKillPoll = 50 * time.Millisecond

var afterWorkerSpawn = func() {}

type activeSegment struct {
	stdin    io.WriteCloser
	contract string
	pgid     int
	done     chan struct{}
	force    chan struct{}
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
	// hostID joins every line logged about a segment (weft-service/v2 §4.2).
	hostID string
	// worker is the last probe of worker_bin (§2.6); probeMu serializes
	// probing so one change is probed once.
	worker  atomic.Pointer[workerInfo]
	probeMu sync.Mutex
	// output wakes the output streams when a chunk is retained.
	output *broadcast
}

func NewSupervisor(cfg *config.Config, store *ledger.Store, home string, logger *log.Logger, locks ...*sync.RWMutex) *Supervisor {
	stateMu := &sync.RWMutex{}
	if len(locks) > 0 && locks[0] != nil {
		stateMu = locks[0]
	}
	return &Supervisor{
		cfg: cfg, store: store, home: home, log: logger, stateMu: stateMu,
		active: make(map[string]*activeSegment), stopping: make(map[string]bool), output: newBroadcast(),
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
	command := map[string]any{"contract_version": active.contract, "command": "stop"}
	doc, _ := json.Marshal(command)
	_, _ = active.stdin.Write(append(doc, '\n'))
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

func (s *Supervisor) run(runID string) error {
	s.stateMu.RLock()
	run, err := s.store.GetWeftRun(runID)
	s.stateMu.RUnlock()
	if err != nil {
		return err
	}
	if len(run.Segments) == 0 {
		return fmt.Errorf("run has no segment")
	}
	segment := run.Segments[len(run.Segments)-1]
	if run.Stopped && run.Phase == ledger.WeftPhaseEnded {
		s.stateMu.Lock()
		err := s.store.CancelQueuedWeftSegment(run.ID, segment.Segment)
		s.stateMu.Unlock()
		s.mu.Lock()
		delete(s.stopping, run.ID)
		s.mu.Unlock()
		return err
	}
	if !segment.RunDir.Valid {
		return fmt.Errorf("segment %d has no run directory", segment.Segment)
	}
	runDir := segment.RunDir.String
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	catalog, err := loadTickets(s.cfg.Paths.TKConfig)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	userTempDir, err := sandbox.UserTempDir()
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	coding := sandbox.CodingOptions{
		SRTBin: s.cfg.Paths.SRTBin, RunDir: runDir, HomeDir: homeDir, PATH: workerPath,
		Repo: run.Repo, TicketsRoot: catalog.config.CentralRoot, Worktree: run.Worktree,
		UserTempDir:    userTempDir,
		AllowedDomains: s.cfg.Weft.Sandbox.AllowedDomains,
		AllowWrite:     s.cfg.Weft.Sandbox.AllowWrite,
	}
	if err := executable(s.cfg.Weft.WorkerBin); err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	// A worker that speaks weft-worker/v2 is launched under it; one that does
	// not keeps the developmental slice's weft-remote/v1 dispatch (§2.4).
	worker := s.refreshWorker()
	contract := ContractVersion
	if worker.speaks(WorkerContractV2) {
		contract = WorkerContractV2
	}
	oauthToken, err := s.oauthToken()
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	if err := executable(s.cfg.Paths.SRTBin); err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassSandbox, err)
	}
	profilePath, err := sandbox.WriteCodingProfile(coding)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassSandbox, err)
	}
	checkCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = sandbox.CheckCoding(checkCtx, coding)
	cancel()
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassSandbox, err)
	}
	answerPath, err := s.writeAnswer(run, segment, runDir, contract)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	argv := s.argv(run, segment, contract, profilePath, answerPath)
	env := s.environ(homeDir, runDir, oauthToken)
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

	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	defer gateWriter.Close()
	gatedArgv := append([]string{"-c", `IFS= read -r _ <&3 || exit 125; exec "$@"`, "shuttle-worker-gate"}, argv...)
	cmd := exec.Command("/bin/sh", gatedArgv...)
	cmd.Dir = run.Repo
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	}
	s.mu.Lock()
	s.active[run.ID] = active
	stopping := s.stopping[run.ID]
	s.mu.Unlock()
	if stopping {
		command, _ := json.Marshal(map[string]any{"contract_version": contract, "command": "stop"})
		_, _ = stdin.Write(append(command, '\n'))
	}
	defer func() {
		stdin.Close()
		s.mu.Lock()
		delete(s.active, run.ID)
		delete(s.stopping, run.ID)
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
	s.log.Printf("weft segment %s: launched %s segment, pid %d, under %s",
		segmentIDs(s.hostID, run.ID, segment, ""), segment.Kind, cmd.Process.Pid, contract)
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
	go func() { eventCh <- s.readEvents(run, segment, contract, worker, stdout, eventsFile) }()
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	timeoutSeconds := s.cfg.Weft.SegmentTimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = 21600
	}
	timer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	defer timer.Stop()
	timeoutC := timer.C
	var (
		waitErr    error
		read       workerRead
		gotWait    bool
		gotRead    bool
		failClass  string
		failDetail string
		killC      <-chan time.Time
	)
	for !gotWait || !gotRead {
		select {
		case waitErr = <-waitCh:
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
	if err := ensureProcessGroupGone(cmd.Process.Pid); err != nil {
		return err
	}
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
		s.stateMu.Lock()
		err := s.store.FailWeftSegment(run.ID, segment.Segment, failClass, failDetail)
		if err == nil {
			err = s.store.ReleaseWeftRepository(run.ID)
		}
		s.stateMu.Unlock()
		if err != nil {
			return err
		}
	} else {
		s.log.Printf("weft segment %s: settled after its terminal event", segmentIDs(s.hostID, run.ID, segment, read.stage))
		s.stateMu.Lock()
		err := s.store.CompleteWeftSegment(run.ID, segment.Segment)
		s.stateMu.Unlock()
		if err != nil {
			return err
		}
	}
	result := map[string]any{
		"contract_version": ContractVersion, "terminal": read.terminal,
		"failure_class": nullableText(failClass), "detail": nullableText(failDetail),
		"exit_code": exitCode,
	}
	doc, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(filepath.Join(runDir, "result.json"), doc, 0o600); err != nil {
		return err
	}
	return nil
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

func (s *Supervisor) failBeforeSpawn(run *ledger.WeftRun, segment ledger.WeftSegment, class string, cause error) error {
	s.log.Printf("weft segment %s: failed %s before spawn: %v", segmentIDs(s.hostID, run.ID, segment, ""), class, cause)
	s.stateMu.Lock()
	err := s.store.FailWeftSegment(run.ID, segment.Segment, class, cause.Error())
	if err == nil {
		err = s.store.ReleaseWeftRepository(run.ID)
	}
	s.stateMu.Unlock()
	if err != nil {
		return fmt.Errorf("%v; settle segment: %w", cause, err)
	}
	return cause
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

// argv never carries --land: landing is not served by this host, so no
// worker is asked to take a landing step (weft-service/v2 §7.1).
func (s *Supervisor) argv(run *ledger.WeftRun, segment ledger.WeftSegment, contract, profilePath, answerPath string) []string {
	argv := []string{
		s.cfg.Weft.WorkerBin, "segment", "--contract", contract,
		"--run", run.ID, "--segment", fmt.Sprint(segment.Segment),
		"--ticket", run.Ticket, "--repo", run.Repo, "--weft-root", s.cfg.Weft.Root,
		"--sandbox-profile", profilePath,
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

// oauthToken reads the segment's Claude credential. The coding profile denies
// ~/Library/Keychains, so the subscription token the CLI keeps in the login
// Keychain has to reach the worker by environment; the error names the item
// and never the value.
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
		"TMPDIR=" + filepath.Join(runDir, "tmp"), "CLAUDE_CODE_TMPDIR=" + filepath.Join(runDir, "tmp"),
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
