package weftremote

import (
	"bufio"
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
	stdin io.WriteCloser
	pgid  int
	done  chan struct{}
	force chan struct{}
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
}

func NewSupervisor(cfg *config.Config, store *ledger.Store, home string, logger *log.Logger, locks ...*sync.RWMutex) *Supervisor {
	stateMu := &sync.RWMutex{}
	if len(locks) > 0 && locks[0] != nil {
		stateMu = locks[0]
	}
	return &Supervisor{
		cfg: cfg, store: store, home: home, log: logger, stateMu: stateMu,
		active: make(map[string]*activeSegment), stopping: make(map[string]bool),
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
	command := map[string]any{"contract_version": ContractVersion, "command": "stop"}
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
	answerPath, err := s.writeAnswer(run, segment, runDir)
	if err != nil {
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
	argv := s.argv(run, segment, profilePath, answerPath)
	env := s.environ(homeDir, runDir, oauthToken)
	if err := writeRunRecord(runDir, run, segment, argv, env, 0); err != nil {
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
	if err := writeRunRecord(runDir, run, segment, argv, env, cmd.Process.Pid); err != nil {
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
		stdin: stdin, pgid: cmd.Process.Pid, done: make(chan struct{}), force: make(chan struct{}, 1),
	}
	s.mu.Lock()
	s.active[run.ID] = active
	stopping := s.stopping[run.ID]
	s.mu.Unlock()
	if stopping {
		command, _ := json.Marshal(map[string]any{"contract_version": ContractVersion, "command": "stop"})
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
	err = s.store.MarkWeftRunning(run.ID, segment.Segment, cmd.Process.Pid)
	s.stateMu.Unlock()
	if err != nil {
		gateWriter.Close()
		_ = cmd.Wait()
		return s.failBeforeSpawn(run, segment, ledger.ClassPreflight, err)
	}
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
	go func() { eventCh <- s.readEvents(run, segment, stdout, eventsFile) }()
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

func (s *Supervisor) argv(run *ledger.WeftRun, segment ledger.WeftSegment, profilePath, answerPath string) []string {
	argv := []string{
		s.cfg.Weft.WorkerBin, "segment", "--contract", ContractVersion,
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

func (s *Supervisor) writeAnswer(run *ledger.WeftRun, segment ledger.WeftSegment, runDir string) (string, error) {
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
	doc, err := json.Marshal(map[string]any{
		"contract_version": ContractVersion, "question_id": question.ID,
		"question": question.Text, "answer": answer.Text,
	})
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func writeRunRecord(runDir string, run *ledger.WeftRun, segment ledger.WeftSegment, argv, env []string, pid int) error {
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
		"contract_version": ContractVersion, "run_id": run.ID, "segment": segment.Segment,
		"kind": segment.Kind, "ticket": run.Ticket, "argv": argv, "environment": names,
		"claimed_at": segment.ClaimedAt, "pid": recordedPID,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, "run.json"), doc, 0o600)
}

type workerRead struct {
	terminal bool
	err      error
}

func (s *Supervisor) readEvents(run *ledger.WeftRun, segment ledger.WeftSegment, stdout io.Reader, record io.Writer) workerRead {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	expectedSeq := 1
	first := true
	workerRunID := ""
	terminal := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if _, err := record.Write(append(line, '\n')); err != nil {
			return workerRead{err: err}
		}
		if terminal {
			return workerRead{terminal: true, err: fmt.Errorf("worker event followed a terminal event")}
		}
		var kind struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(line, &kind); err != nil {
			return workerRead{err: fmt.Errorf("decode worker event: %w", err)}
		}
		switch kind.Event {
		case "hello":
			var event helloEvent
			if err := decodeStrict(line, &event); err != nil {
				return workerRead{err: err}
			}
			if err := validateEnvelope(event.envelope, run, segment, expectedSeq, workerRunID); err != nil {
				return workerRead{err: err}
			}
			if !first || event.Worktree != run.Worktree || event.Ticket != run.Ticket || event.Project != run.Project {
				return workerRead{err: fmt.Errorf("hello does not match the claimed run")}
			}
			if event.Branch != run.Branch || event.Criteria == "" {
				return workerRead{err: fmt.Errorf("hello branch or criteria does not match the protocol")}
			}
			for _, stage := range event.Stages {
				if stage.Stage == "" || stage.Backend == "" {
					return workerRead{err: fmt.Errorf("hello carries an invalid stage plan")}
				}
			}
			expectedResume := latestWorkerBefore(run, segment.Segment)
			if text(event.Resumed) != expectedResume {
				return workerRead{err: fmt.Errorf("hello resumed %q, want %q", text(event.Resumed), expectedResume)}
			}
			expectedAnswer := ""
			if segment.AnswerID.Valid {
				s.stateMu.RLock()
				answer, err := s.store.WeftAnswer(segment.AnswerID.String)
				s.stateMu.RUnlock()
				if err != nil {
					return workerRead{err: err}
				}
				expectedAnswer = answer.QuestionID
			}
			if text(event.Answer) != expectedAnswer {
				return workerRead{err: fmt.Errorf("hello answer %q, want %q", text(event.Answer), expectedAnswer)}
			}
			workerRunID = event.WorkerRunID
			s.stateMu.Lock()
			err := s.store.RecordWeftHello(run.ID, segment.Segment, event.WorkerRunID, event.Base, event.Criteria)
			s.stateMu.Unlock()
			if err != nil {
				return workerRead{err: err}
			}
		case "stage":
			var event stageEvent
			if err := decodeStrict(line, &event); err != nil {
				return workerRead{err: err}
			}
			if err := validateEnvelope(event.envelope, run, segment, expectedSeq, workerRunID); err != nil {
				return workerRead{err: err}
			}
			if first {
				return workerRead{err: fmt.Errorf("first worker event is stage, want hello")}
			}
			if event.Stage == "" || event.Occurrence < 1 || event.Attempt < 1 ||
				!oneOf(event.State, "running", "done", "failed", "needs_input") {
				return workerRead{err: fmt.Errorf("invalid stage transition")}
			}
			stage := ledger.WeftStage{
				RunID: run.ID, Segment: segment.Segment, Stage: event.Stage,
				Occurrence: event.Occurrence, Attempt: event.Attempt,
				Backend: nullSQL(event.Backend), State: event.State, Session: nullSQL(event.Session),
				Detail: nullSQL(event.Detail), ExitCode: nullInt64(event.ExitCode),
				Turns: nullInt64(event.Turns), Spend: event.Spend,
				Processing: nullInt64(event.ProcessingMS), Tokens: event.Tokens, At: event.TS,
			}
			s.stateMu.Lock()
			err := s.store.RecordWeftStage(stage)
			s.stateMu.Unlock()
			if err != nil {
				return workerRead{err: err}
			}
		case "parked":
			var event parkedEvent
			if err := decodeStrict(line, &event); err != nil {
				return workerRead{err: err}
			}
			if err := validateEnvelope(event.envelope, run, segment, expectedSeq, workerRunID); err != nil {
				return workerRead{err: err}
			}
			if first {
				return workerRead{err: fmt.Errorf("first worker event is parked, want hello")}
			}
			if event.WorkerOutcome != "blocked" || event.Question == "" ||
				len([]byte(event.Question)) > maxAnswerBytes || event.Occurrence < 1 || event.Attempt < 1 {
				return workerRead{err: fmt.Errorf("invalid parked event")}
			}
			s.stateMu.Lock()
			_, err := s.store.RecordWeftParked(run.ID, segment.Segment, event.Stage,
				event.Occurrence, event.Attempt, event.Question, event.Head, event.Summary)
			s.stateMu.Unlock()
			if err != nil {
				return workerRead{err: err}
			}
			terminal = true
		case "ended":
			var event endedEvent
			if err := decodeStrict(line, &event); err != nil {
				return workerRead{err: err}
			}
			// A refusal before hello precedes weft's started line, so it has no
			// worker_run_id yet.
			refusal := first && event.Failure != nil
			if err := validateEnvelopeAt(event.envelope, run, segment, expectedSeq, workerRunID, refusal); err != nil {
				return workerRead{err: err}
			}
			if first && event.Failure == nil {
				return workerRead{err: fmt.Errorf("first ended event must carry a worker refusal")}
			}
			if !oneOf(event.Outcome, "finished", "halted", "failed", "stopped") {
				return workerRead{err: fmt.Errorf("invalid ended outcome %q", event.Outcome)}
			}
			// weft-service/v2 §6.7: an answer or resume segment refused before
			// any stage because the default branch moved past the run's base.
			// The run is held for the operator, its answer unconsumed.
			if text(event.Failure) == ledger.WeftActionStaleBase {
				if !refusal || event.Outcome != "failed" || segment.Kind == "launch" {
					return workerRead{err: fmt.Errorf("stale_base is a refusal of an answer or resume segment before hello")}
				}
				s.stateMu.Lock()
				err := s.store.RecordWeftStaleBase(run.ID, segment.Segment, text(event.Detail))
				s.stateMu.Unlock()
				if err != nil {
					return workerRead{err: err}
				}
				terminal = true
				break
			}
			haltStage, haltReason, haltDetail := "", "", ""
			if event.Halt != nil {
				haltStage, haltReason, haltDetail = event.Halt.Stage, event.Halt.Reason, event.Halt.Detail
			}
			s.stateMu.Lock()
			err := s.store.RecordWeftEnded(run.ID, segment.Segment, event.Outcome, event.Stopped,
				haltStage, haltReason, haltDetail, text(event.Failure), text(event.Detail),
				event.Head, event.Summary)
			s.stateMu.Unlock()
			if err != nil {
				return workerRead{err: err}
			}
			terminal = true
		default:
			return workerRead{err: fmt.Errorf("unknown worker event %q", kind.Event)}
		}
		first = false
		expectedSeq++
	}
	if err := scanner.Err(); err != nil {
		return workerRead{terminal: terminal, err: err}
	}
	return workerRead{terminal: terminal}
}

type envelope struct {
	ContractVersion string `json:"contract_version"`
	RunID           string `json:"run_id"`
	WorkerRunID     string `json:"worker_run_id"`
	Segment         int    `json:"segment"`
	Seq             int    `json:"seq"`
	TS              string `json:"ts"`
	Event           string `json:"event"`
}

type helloEvent struct {
	envelope
	WorkerVersion string            `json:"worker_version"`
	Ticket        string            `json:"ticket"`
	Project       string            `json:"project"`
	Branch        string            `json:"branch"`
	Worktree      string            `json:"worktree"`
	Base          string            `json:"base"`
	Criteria      string            `json:"criteria"`
	Resumed       *string           `json:"resumed"`
	Inherited     []json.RawMessage `json:"inherited"`
	Answer        *string           `json:"answer"`
	Stages        []helloStage      `json:"stages"`
}

type helloStage struct {
	Stage   string `json:"stage"`
	Backend string `json:"backend"`
}

type stageEvent struct {
	envelope
	Stage        string          `json:"stage"`
	Backend      *string         `json:"backend"`
	Occurrence   int             `json:"occurrence"`
	Attempt      int             `json:"attempt"`
	State        string          `json:"state"`
	Session      *string         `json:"session"`
	Detail       *string         `json:"detail"`
	ExitCode     *int64          `json:"exit_code"`
	Turns        *int64          `json:"turns"`
	Spend        json.RawMessage `json:"spend"`
	ProcessingMS *int64          `json:"processing_ms"`
	Tokens       json.RawMessage `json:"tokens"`
}

type parkedEvent struct {
	envelope
	Stage         string          `json:"stage"`
	Occurrence    int             `json:"occurrence"`
	Attempt       int             `json:"attempt"`
	WorkerOutcome string          `json:"worker_outcome"`
	Question      string          `json:"question"`
	Head          string          `json:"head"`
	Summary       json.RawMessage `json:"summary"`
}

type endedEvent struct {
	envelope
	Outcome string          `json:"outcome"`
	Stopped bool            `json:"stopped"`
	Halt    *halt           `json:"halt"`
	Failure *string         `json:"failure"`
	Detail  *string         `json:"detail"`
	Head    string          `json:"head"`
	Summary json.RawMessage `json:"summary"`
}

type halt struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

func validateEnvelope(e envelope, run *ledger.WeftRun, segment ledger.WeftSegment, seq int, workerID string) error {
	return validateEnvelopeAt(e, run, segment, seq, workerID, false)
}

// validateEnvelopeAt checks an event's envelope; preHello admits the null
// worker_run_id of a refusal written before weft's started line.
func validateEnvelopeAt(e envelope, run *ledger.WeftRun, segment ledger.WeftSegment, seq int, workerID string, preHello bool) error {
	if e.ContractVersion != ContractVersion || e.RunID != run.ID || e.Segment != segment.Segment || e.Seq != seq {
		return fmt.Errorf("worker event envelope does not match the launched segment")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.TS); err != nil {
		return fmt.Errorf("worker event timestamp: %w", err)
	}
	if preHello && e.WorkerRunID == "" {
		return nil
	}
	if !validProtocolID(e.WorkerRunID) || (workerID != "" && e.WorkerRunID != workerID) {
		return fmt.Errorf("worker_run_id changed within the segment")
	}
	return nil
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
