package ledger

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"shuttle/internal/sandbox"
)

const (
	WeftAgent   = "weft-worker"
	TriggerWeft = "weft"

	WeftPhaseQueued  = "queued"
	WeftPhaseRunning = "running"
	WeftPhaseParked  = "parked"
	WeftPhaseEnded   = "ended"

	ClassProtocol = "protocol"
	ClassUnknown  = "unknown"
)

var (
	ErrWeftRunInFlight     = errors.New("weft run already in flight")
	ErrWeftHostPaused      = errors.New("weft worker is paused")
	ErrWeftRepositoryBusy  = errors.New("weft repository is busy")
	ErrWeftQuestionPending = errors.New("weft question is not pending")
	ErrWeftRunOver         = errors.New("weft run is over")
	ErrWeftRunNotOver      = errors.New("weft run is not over")
	ErrWeftRunNotResumable = errors.New("weft run is not resumable")
	ErrWeftSegmentLimit    = errors.New("weft segment limit reached")
	ErrWeftWorktreeMissing = errors.New("weft worktree is missing")
)

// RepositoryBusyError carries the run holding a repository reservation.
type RepositoryBusyError struct{ HeldBy string }

func (e *RepositoryBusyError) Error() string {
	return fmt.Sprintf("%v: held by %s", ErrWeftRepositoryBusy, e.HeldBy)
}
func (e *RepositoryBusyError) Unwrap() error { return ErrWeftRepositoryBusy }

// WeftRun is the durable API run joined to its segments and stage records.
type WeftRun struct {
	ID         string
	Ticket     string
	Project    string
	Repo       string
	Branch     string
	Worktree   string
	Base       sql.NullString
	Criteria   sql.NullString
	Phase      string
	Outcome    sql.NullString
	HaltStage  sql.NullString
	HaltReason sql.NullString
	HaltDetail sql.NullString
	Head       sql.NullString
	Stopped    bool
	Summary    json.RawMessage
	CreatedAt  string
	UpdatedAt  string
	EndedAt    sql.NullString

	RepositoryID     string
	EntryID          sql.NullString
	Origin           string
	RecordedUnder    string
	ArchiveID        sql.NullString
	SettlementState  string
	SettlementStep   sql.NullString
	SettlementReason sql.NullString
	SettlementDetail sql.NullString
	SettlementBranch sql.NullString
	SettlementCommit sql.NullString
	ActionRequired   sql.NullString
	ActionDetail     sql.NullString
	Disposition      string
	ArchiveRef       sql.NullString
	Supersedes       sql.NullString
	SupersededBy     sql.NullString
	Revision         int64

	Segments []WeftSegment
	Stages   []WeftStage
	Question *WeftQuestion
}

type WeftSegment struct {
	RunID       string
	Segment     int
	Kind        string
	LedgerRunID string
	WorkerRunID sql.NullString
	AnswerID    sql.NullString
	Terminal    sql.NullString
	Failure     sql.NullString
	RunDir      sql.NullString
	ClaimedAt   string
	StartedAt   sql.NullString
	EndedAt     sql.NullString
}

type WeftStage struct {
	RunID      string
	Segment    int
	Stage      string
	Occurrence int
	Attempt    int
	Backend    sql.NullString
	State      string
	Session    sql.NullString
	Detail     sql.NullString
	ExitCode   sql.NullInt64
	Turns      sql.NullInt64
	Spend      json.RawMessage
	Processing sql.NullInt64
	Tokens     json.RawMessage
	At         string
}

type WeftQuestion struct {
	ID         string
	RunID      string
	Ticket     string
	Segment    int
	Stage      string
	Occurrence int
	Attempt    int
	Text       string
	AskedAt    string
	State      string
	AnswerID   sql.NullString
	Answer     *WeftAnswer
}

type WeftAnswer struct {
	ID              string
	QuestionID      string
	RunID           string
	Text            string
	SubmittedAt     string
	Segment         int
	EntryID         sql.NullString
	ConsumedSegment sql.NullInt64
}

type WeftEvent struct {
	Seq     int64
	TS      string
	RunID   string
	Type    string
	Payload json.RawMessage
}

type WeftRequest struct {
	TokenID   string
	Key       string
	Method    string
	Path      string
	BodySHA   string
	Status    int
	Response  []byte
	CreatedAt string
}

type WeftLaunch struct {
	Ticket, Project, Repo, Branch, Worktree, RunDirRoot string
}

// ClaimWeft creates an API run, its first phase-1 ledger segment and the
// repository reservation in one transaction. It is weft-remote/v1's
// admission, so the run is recorded under that protocol, which never lands.
// Nothing launches the segments it claims in this build until v2 dispatch
// wires the supervisor back in.
func (s *Store) ClaimWeft(p WeftLaunch) (*WeftRun, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := requireWeftAvailable(tx, p.Ticket); err != nil {
		return nil, err
	}
	now := s.now()
	runID, err := newRemoteID("r", now)
	if err != nil {
		return nil, err
	}
	ledgerID, err := NewRunID(WeftAgent, now.UTC())
	if err != nil {
		return nil, err
	}
	stamp := weftTimestamp(now)
	runDir := filepath.Join(p.RunDirRoot, ledgerID)
	repositoryID := sandbox.RepositoryID(p.Repo)
	if _, err := tx.Exec(`INSERT INTO weft_runs(
		id,ticket,project,repo,branch,worktree,phase,created_at,updated_at,
		repository_id,origin,recorded_under,disposition,settlement_state,settlement_detail)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		runID, p.Ticket, p.Project, p.Repo, p.Branch, p.Worktree,
		WeftPhaseQueued, stamp, stamp, repositoryID, WeftOriginHosted, WeftRecordedV1,
		WeftDispositionRetained, WeftSettlementNone, WeftV1SettlementDetail); err != nil {
		return nil, err
	}
	if err := acquireRepository(tx, repositoryID, p.Repo, runID, stamp); err != nil {
		return nil, err
	}
	if err := insertWeftLedgerRunTx(tx, ledgerID, runDir, stamp, now); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO weft_segments(run_id,segment,kind,ledger_run_id)
		VALUES(?,?,?,?)`, runID, 1, "launch", ledgerID); err != nil {
		return nil, err
	}
	if err := appendRunEventTx(tx, runID, stamp); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

func requireWeftAvailable(tx *sql.Tx, ticket string) error {
	var paused bool
	if err := tx.QueryRow(`SELECT COALESCE((SELECT paused FROM agents WHERE name = ?),0)`, WeftAgent).Scan(&paused); err != nil {
		return err
	}
	if paused {
		return ErrWeftHostPaused
	}
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM weft_runs WHERE ticket=? AND phase IN (?,?,?)`,
		ticket, WeftPhaseQueued, WeftPhaseRunning, WeftPhaseParked).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return ErrWeftRunInFlight
	}
	return nil
}

func acquireRepository(tx *sql.Tx, repositoryID, repo, runID, at string) error {
	_, err := tx.Exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
		VALUES(?,?,'run',?,?)`, repositoryID, repo, runID, at)
	if err == nil {
		return nil
	}
	var held string
	if scanErr := tx.QueryRow(`SELECT holder_id FROM weft_reservations WHERE repository_id=?`, repositoryID).Scan(&held); scanErr == nil {
		return &RepositoryBusyError{HeldBy: held}
	}
	return err
}

// ClaimWeftAnswer atomically consumes a pending question, claims the next
// segment and reacquires the repository reservation.
func (s *Store) ClaimWeftAnswer(runID, questionID, text, runDirRoot string) (*WeftRun, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return nil, err
	}
	if run.Phase == WeftPhaseEnded {
		return nil, ErrWeftRunOver
	}
	if run.Phase != WeftPhaseParked {
		return nil, ErrWeftQuestionPending
	}
	if err := requireWeftAgentAvailable(tx); err != nil {
		return nil, err
	}
	segment, err := nextSegmentTx(tx, runID)
	if err != nil {
		return nil, err
	}
	if segment > 8 {
		return nil, ErrWeftSegmentLimit
	}
	answerID, err := newRemoteID("a", s.now())
	if err != nil {
		return nil, err
	}
	stamp := weftTimestamp(s.now())
	res, err := tx.Exec(`UPDATE weft_questions SET state='answered',answer_id=?
		WHERE id=? AND run_id=? AND state='pending'`, answerID, questionID, runID)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, ErrWeftQuestionPending
	}
	if err := acquireRepository(tx, run.RepositoryID, run.Repo, runID, stamp); err != nil {
		return nil, err
	}
	ledgerID, err := insertWeftSegmentTx(tx, runID, segment, "answer", answerID, runDirRoot, s.now())
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO weft_answers(id,question_id,run_id,text,submitted_at,segment)
		VALUES(?,?,?,?,?,?)`, answerID, questionID, runID, text, stamp, segment); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=NULL,stopped=0,summary=NULL,updated_at=?,ended_at=NULL,
		revision=revision+1 WHERE id=?`, WeftPhaseQueued, stamp, runID); err != nil {
		return nil, err
	}
	question, err := getQuestionTx(tx, questionID)
	if err != nil {
		return nil, err
	}
	// v2's answer event names the entry an answer was queued as; a v1 answer
	// has none, so its recording is the question event alone.
	question.Answer = &WeftAnswer{ID: answerID, QuestionID: questionID, RunID: runID, Text: text,
		SubmittedAt: stamp, Segment: segment}
	if err := appendQuestionEventTx(tx, question, stamp); err != nil {
		return nil, err
	}
	if err := appendRunEventTx(tx, runID, stamp); err != nil {
		return nil, err
	}
	_ = ledgerID
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

func requireWeftAgentAvailable(tx *sql.Tx) error {
	var paused bool
	if err := tx.QueryRow(`SELECT COALESCE((SELECT paused FROM agents WHERE name=?),0)`, WeftAgent).Scan(&paused); err != nil {
		return err
	}
	if paused {
		return ErrWeftHostPaused
	}
	return nil
}

func nextSegmentTx(tx *sql.Tx, runID string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COALESCE(MAX(segment),0)+1 FROM weft_segments WHERE run_id=?`, runID).Scan(&n)
	return n, err
}

func insertWeftSegmentTx(tx *sql.Tx, runID string, segment int, kind, answerID, runDirRoot string, now time.Time) (string, error) {
	ledgerID, err := NewRunID(WeftAgent, now.UTC())
	if err != nil {
		return "", err
	}
	stamp := weftTimestamp(now)
	runDir := filepath.Join(runDirRoot, ledgerID)
	if err := insertWeftLedgerRunTx(tx, ledgerID, runDir, stamp, now); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`INSERT INTO weft_segments(run_id,segment,kind,ledger_run_id,answer_id)
		VALUES(?,?,?,?,?)`, runID, segment, kind, ledgerID, nullString(answerID)); err != nil {
		return "", err
	}
	return ledgerID, nil
}

func insertWeftLedgerRunTx(tx *sql.Tx, ledgerID, runDir, claimedAt string, now time.Time) error {
	for offset := range 1024 {
		_, err := tx.Exec(`INSERT INTO runs(id,agent,trigger_kind,scheduled_for,claimed_at,status,run_dir)
			VALUES(?,?,?,?,?,?,?)`, ledgerID, WeftAgent, TriggerWeft,
			now.UTC().Add(time.Duration(offset)*time.Nanosecond).Format(time.RFC3339Nano),
			claimedAt, StatusClaimed, runDir)
		if err == nil {
			return nil
		}
		if !isSlotTaken(err) {
			return err
		}
	}
	return fmt.Errorf("could not allocate a unique weft scheduled_for instant")
}

// ClaimWeftResume claims another segment for a stopped or host-failed run.
func (s *Store) ClaimWeftResume(runID, runDirRoot string) (*WeftRun, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return nil, err
	}
	if run.Phase != WeftPhaseEnded || (!run.Stopped && run.Outcome.String != "failed") {
		return nil, ErrWeftRunNotResumable
	}
	var active int
	if err := tx.QueryRow(`SELECT count(*) FROM weft_runs WHERE ticket=? AND id<>? AND phase IN (?,?,?)`,
		run.Ticket, runID, WeftPhaseQueued, WeftPhaseRunning, WeftPhaseParked).Scan(&active); err != nil {
		return nil, err
	}
	if active > 0 {
		return nil, ErrWeftRunInFlight
	}
	if err := requireWeftAgentAvailable(tx); err != nil {
		return nil, err
	}
	var lastFailure sql.NullString
	if err := tx.QueryRow(`SELECT failure FROM weft_segments WHERE run_id=? ORDER BY segment DESC LIMIT 1`, runID).Scan(&lastFailure); err != nil {
		return nil, err
	}
	if !run.Stopped && !isHostFailure(lastFailure.String) {
		return nil, ErrWeftRunNotResumable
	}
	segment, err := nextSegmentTx(tx, runID)
	if err != nil {
		return nil, err
	}
	if segment > 8 {
		return nil, ErrWeftSegmentLimit
	}
	stamp := weftTimestamp(s.now())
	if err := acquireRepository(tx, run.RepositoryID, run.Repo, runID, stamp); err != nil {
		return nil, err
	}
	var lastAnswer, lastWorker sql.NullString
	if err := tx.QueryRow(`SELECT answer_id,worker_run_id FROM weft_segments WHERE run_id=? ORDER BY segment DESC LIMIT 1`, runID).
		Scan(&lastAnswer, &lastWorker); err != nil {
		return nil, err
	}
	var answerID string
	if !lastWorker.Valid && lastAnswer.Valid {
		answerID = lastAnswer.String
	}
	if _, err := insertWeftSegmentTx(tx, runID, segment, "resume", answerID, runDirRoot, s.now()); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=NULL,stopped=0,updated_at=?,ended_at=NULL,
		revision=revision+1 WHERE id=?`, WeftPhaseQueued, stamp, runID); err != nil {
		return nil, err
	}
	if err := appendRunEventTx(tx, runID, stamp); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

func isHostFailure(v string) bool {
	switch v {
	case ClassDied, ClassNoResult, ClassStuck, ClassProtocol, ClassUnknown, ClassSandbox, ClassPreflight:
		return true
	}
	return false
}

// MarkWeftRunning records the child process before any worker event is read.
func (s *Store) MarkWeftRunning(runID string, segment, pid int) error {
	return s.withTx(func(tx *sql.Tx) error {
		var ledgerID string
		if err := tx.QueryRow(`SELECT ledger_run_id FROM weft_segments WHERE run_id=? AND segment=?`,
			runID, segment).Scan(&ledgerID); err != nil {
			return err
		}
		if _, err := requireStatus(tx, ledgerID, StatusRunning, StatusClaimed); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE runs SET status=?,started_at=?,pid=? WHERE id=?`,
			StatusRunning, weftTimestamp(s.now()), pid, ledgerID)
		return err
	})
}

// RecordWeftHello accepts the first worker event and moves the API run to
// running. The supervisor has already checked that hello acknowledges the
// segment's answer, so the answer is consumed by this segment.
func (s *Store) RecordWeftHello(runID string, segment int, workerRunID, base, criteria string) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET worker_run_id=? WHERE run_id=? AND segment=? AND worker_run_id IS NULL`,
			workerRunID, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_answers SET consumed_segment=? WHERE consumed_segment IS NULL
			AND id=(SELECT answer_id FROM weft_segments WHERE run_id=? AND segment=?)`, segment, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET base=?,criteria=?,phase=?,updated_at=?,revision=revision+1 WHERE id=?`,
			base, criteria, WeftPhaseRunning, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

func (s *Store) RecordWeftStage(stage WeftStage) error {
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO weft_stages(
			run_id,segment,stage,occurrence,attempt,backend,state,session,detail,exit_code,turns,spend,processing_ms,tokens,at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(run_id,segment,stage,occurrence,attempt) DO UPDATE SET
			backend=excluded.backend,state=excluded.state,session=excluded.session,detail=excluded.detail,
			exit_code=excluded.exit_code,turns=excluded.turns,spend=excluded.spend,
			processing_ms=excluded.processing_ms,tokens=excluded.tokens,at=excluded.at`,
			stage.RunID, stage.Segment, stage.Stage, stage.Occurrence, stage.Attempt,
			stage.Backend, stage.State, stage.Session, stage.Detail, stage.ExitCode, stage.Turns,
			nullJSON(stage.Spend), stage.Processing, nullJSON(stage.Tokens), stage.At); err != nil {
			return err
		}
		if err := aggregateWeftSegmentTx(tx, stage.RunID, stage.Segment); err != nil {
			return err
		}
		return appendStageEventTx(tx, stage)
	})
}

// RecordWeftParked records a pending question; process settlement waits for reap.
func (s *Store) RecordWeftParked(runID string, segment int, stage string, occurrence, attempt int, question, head string, summary json.RawMessage) (string, error) {
	qid, err := newRemoteID("q", s.now())
	if err != nil {
		return "", err
	}
	err = s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='parked' WHERE run_id=? AND segment=?`, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,head=?,summary=?,updated_at=?,revision=revision+1 WHERE id=?`,
			WeftPhaseParked, head, string(summary), stamp, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO weft_questions(
			id,run_id,segment,stage,occurrence,attempt,text,asked_at,state)
			VALUES(?,?,?,?,?,?,?,?,?)`, qid, runID, segment, stage, occurrence, attempt, question, stamp, "pending"); err != nil {
			return err
		}
		q, err := getQuestionTx(tx, qid)
		if err != nil {
			return err
		}
		if err := appendQuestionEventTx(tx, q, stamp); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
	return qid, err
}

// RecordWeftEnded records the worker's terminal event; settlement waits for reap.
func (s *Store) RecordWeftEnded(runID string, segment int, outcome string, stopped bool, haltStage, haltReason, haltDetail, failure, detail, head string, summary json.RawMessage) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='ended',failure=? WHERE run_id=? AND segment=?`,
			nullString(failure), runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=?,halt_stage=?,halt_reason=?,halt_detail=?,
			head=?,stopped=?,summary=?,updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`,
			WeftPhaseEnded, outcome, nullString(haltStage), nullString(haltReason),
			nullString(firstNonempty(haltDetail, detail)), nullString(head), stopped,
			string(summary), stamp, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// CompleteWeftSegment settles the phase-1 row and releases the repository only
// after the supervisor has reaped the worker process. A terminal worker event
// alone deliberately does neither.
func (s *Store) CompleteWeftSegment(runID string, segment int) error {
	var event Event
	err := s.withTx(func(tx *sql.Tx) error {
		ledgerID, err := segmentLedgerID(tx, runID, segment)
		if err != nil {
			return err
		}
		event = Event{Agent: WeftAgent, RunID: ledgerID}
		var last sql.NullString
		if err := tx.QueryRow(`SELECT (SELECT status FROM runs WHERE agent=? AND status IN (?,?)
			ORDER BY ended_at DESC,rowid DESC LIMIT 1)`, WeftAgent, StatusCompleted, StatusFailed).Scan(&last); err != nil {
			return err
		}
		event.Recovered = last.String == StatusFailed
		if err := completeSegmentTx(tx, ledgerID, weftTimestamp(s.now())); err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM weft_reservations WHERE holder_kind='run' AND holder_id=?`, runID)
		return err
	})
	if err == nil && s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return err
}

func completeSegmentTx(tx *sql.Tx, ledgerID, stamp string) error {
	agent, err := requireStatus(tx, ledgerID, StatusCompleted, StatusClaimed, StatusRunning)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE runs SET status=?,ended_at=?,failure_class=NULL,error_signature=NULL WHERE id=?`,
		StatusCompleted, stamp, ledgerID); err != nil {
		return err
	}
	return recordSuccess(tx, agent, stamp)
}

// FailWeftSegment settles a host-side process failure and leaves the run resumable.
func (s *Store) FailWeftSegment(runID string, segment int, class, detail string) error {
	var event Event
	err := s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		var stopped bool
		if err := tx.QueryRow(`SELECT stopped FROM weft_runs WHERE id=?`, runID).Scan(&stopped); err != nil {
			return err
		}
		ledgerID, err := segmentLedgerID(tx, runID, segment)
		if err != nil {
			return err
		}
		agent, err := requireStatus(tx, ledgerID, StatusFailed, StatusClaimed, StatusRunning)
		if err != nil {
			return err
		}
		signature := Signature(class, detail)
		event = Event{Agent: agent, RunID: ledgerID, Class: class, Signature: signature}
		var paused bool
		if err := tx.QueryRow(`SELECT COALESCE((SELECT paused FROM agents WHERE name=?),0)`, agent).Scan(&paused); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE runs SET status=?,ended_at=?,failure_class=?,error_signature=? WHERE id=?`,
			StatusFailed, stamp, class, signature, ledgerID); err != nil {
			return err
		}
		if err := recordFailure(tx, agent, class, stamp); err != nil {
			return err
		}
		if err := upsertIncident(tx, agent, signature, ledgerID, stamp); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRow(`SELECT id,count FROM incidents WHERE agent=? AND signature=? AND acknowledged_at IS NULL`,
			agent, signature).Scan(&event.IncidentID, &count); err != nil {
			return err
		}
		event.NewIncident = count == 1
		if !paused {
			var reason sql.NullString
			if err := tx.QueryRow(`SELECT paused_reason FROM agents WHERE name=?`, agent).Scan(&reason); err != nil {
				return err
			}
			event.PausedReason = reason.String
		}
		if _, err := tx.Exec(`UPDATE weft_segments SET failure=? WHERE run_id=? AND segment=?`, class, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE run_id=? AND state='pending'`, runID); err != nil {
			return err
		}
		outcome := "failed"
		var summary any
		if stopped {
			outcome = "stopped"
			summary = `{"state":"unavailable","reason":"stopped by the operator"}`
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=?,halt_detail=?,summary=COALESCE(?,summary),updated_at=?,ended_at=?,
			revision=revision+1 WHERE id=?`,
			WeftPhaseEnded, outcome, detail, summary, stamp, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
	if err == nil && s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return err
}

// MarkWeftStopped makes an operator stop durable before the process is asked
// to cooperate. The supervisor subsequently records the worker's ended event
// or a host-side failure, both preserving the stopped outcome.
func (s *Store) MarkWeftStopped(runID string) error {
	return s.withTx(func(tx *sql.Tx) error {
		run, err := getWeftRunTx(tx, runID)
		if err != nil {
			return err
		}
		if run.Phase == WeftPhaseEnded {
			return ErrWeftRunOver
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_runs SET stopped=1,phase=?,outcome='stopped',
			summary=?,updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`, WeftPhaseEnded,
			`{"state":"unavailable","reason":"stopped by the operator"}`, stamp, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// CancelQueuedWeftSegment settles a stop that won the race before the worker
// was spawned. It is an operator stop, not a worker failure or incident.
func (s *Store) CancelQueuedWeftSegment(runID string, segment int) error {
	return s.withTx(func(tx *sql.Tx) error {
		ledgerID, err := segmentLedgerID(tx, runID, segment)
		if err != nil {
			return err
		}
		agent, err := requireStatus(tx, ledgerID, StatusCompleted, StatusClaimed)
		if err != nil {
			return err
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE runs SET status=?,ended_at=? WHERE id=?`, StatusCompleted, stamp, ledgerID); err != nil {
			return err
		}
		if err := recordSuccess(tx, agent, stamp); err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM weft_reservations WHERE holder_kind='run' AND holder_id=?`, runID)
		return err
	})
}

func segmentLedgerID(tx *sql.Tx, runID string, segment int) (string, error) {
	var id string
	err := tx.QueryRow(`SELECT ledger_run_id FROM weft_segments WHERE run_id=? AND segment=?`, runID, segment).Scan(&id)
	return id, err
}

// StopWeftParked closes a run with no live process and withdraws its question.
func (s *Store) StopWeftParked(runID string) (*WeftRun, error) {
	err := s.withTx(func(tx *sql.Tx) error {
		run, err := getWeftRunTx(tx, runID)
		if err != nil {
			return err
		}
		if run.Phase == WeftPhaseEnded {
			return ErrWeftRunOver
		}
		if run.Phase != WeftPhaseParked {
			return fmt.Errorf("run %s has a live segment", runID)
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE run_id=? AND state='pending'`, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome='stopped',stopped=1,
			summary=?,updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`, WeftPhaseEnded,
			`{"state":"unavailable","reason":"stopped by the operator"}`, stamp, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
	if err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

// ReleaseWeftRepository releases only the reservation held by this run.
func (s *Store) ReleaseWeftRepository(runID string) error {
	_, err := s.db.Exec(`DELETE FROM weft_reservations WHERE holder_kind='run' AND holder_id=?`, runID)
	return err
}

// ReconcileWeftUnknown projects a phase-1 restart closure into the API ledger
// and releases the reservation only after reap established the process gone.
func (s *Store) ReconcileWeftUnknown(ledgerRunID string) error {
	return s.withTx(func(tx *sql.Tx) error {
		var runID string
		var segment int
		err := tx.QueryRow(`SELECT run_id,segment FROM weft_segments WHERE ledger_run_id=?`, ledgerRunID).
			Scan(&runID, &segment)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET failure=? WHERE run_id=? AND segment=?`,
			ClassUnknown, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE run_id=? AND state='pending'`, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome='failed',halt_detail=?,
			updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`, WeftPhaseEnded,
			"daemon restarted before the segment settled", stamp, stamp, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM weft_reservations WHERE holder_kind='run' AND holder_id=?`, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

func (s *Store) GetWeftRun(id string) (*WeftRun, error) {
	run, err := getWeftRunDB(s.db, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("weft run %s: not found", id)
	}
	if err != nil {
		return nil, err
	}
	if err := s.loadWeftChildren(run); err != nil {
		return nil, err
	}
	return run, nil
}

func (s *Store) WeftRuns(ticket, state string) ([]*WeftRun, error) {
	return s.QueryWeftRuns(WeftRunQuery{Ticket: ticket, State: state})
}

// weftRunColumns is every weft_runs column scanWeftRun reads, in its order.
const weftRunColumns = `id,ticket,project,repo,branch,worktree,base,criteria,phase,outcome,
	halt_stage,halt_reason,halt_detail,head,stopped,summary,created_at,updated_at,ended_at,
	repository_id,entry_id,origin,recorded_under,archive_id,settlement_state,settlement_step,
	settlement_reason,settlement_detail,settlement_branch,settlement_commit,action_required,
	action_detail,disposition,archive_ref,supersedes,superseded_by,revision`

func getWeftRunTx(tx *sql.Tx, id string) (*WeftRun, error) {
	return scanWeftRun(tx.QueryRow(`SELECT `+weftRunColumns+` FROM weft_runs WHERE id=?`, id))
}

func getWeftRunDB(db *sql.DB, id string) (*WeftRun, error) {
	return scanWeftRun(db.QueryRow(`SELECT `+weftRunColumns+` FROM weft_runs WHERE id=?`, id))
}

func scanWeftRun(sc scanner) (*WeftRun, error) {
	var run WeftRun
	var summary sql.NullString
	err := sc.Scan(&run.ID, &run.Ticket, &run.Project, &run.Repo, &run.Branch, &run.Worktree,
		&run.Base, &run.Criteria, &run.Phase, &run.Outcome, &run.HaltStage, &run.HaltReason,
		&run.HaltDetail, &run.Head, &run.Stopped, &summary, &run.CreatedAt, &run.UpdatedAt, &run.EndedAt,
		&run.RepositoryID, &run.EntryID, &run.Origin, &run.RecordedUnder, &run.ArchiveID,
		&run.SettlementState, &run.SettlementStep, &run.SettlementReason, &run.SettlementDetail,
		&run.SettlementBranch, &run.SettlementCommit, &run.ActionRequired, &run.ActionDetail,
		&run.Disposition, &run.ArchiveRef, &run.Supersedes, &run.SupersededBy, &run.Revision)
	if summary.Valid {
		run.Summary = json.RawMessage(summary.String)
	}
	return &run, err
}

func (s *Store) loadWeftChildren(run *WeftRun) error {
	rows, err := s.db.Query(`SELECT ws.run_id,ws.segment,ws.kind,ws.ledger_run_id,ws.worker_run_id,
		ws.answer_id,ws.terminal,ws.failure,r.run_dir,r.claimed_at,r.started_at,r.ended_at
		FROM weft_segments ws JOIN runs r ON r.id=ws.ledger_run_id
		WHERE ws.run_id=? ORDER BY ws.segment`, run.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var seg WeftSegment
		if err := rows.Scan(&seg.RunID, &seg.Segment, &seg.Kind, &seg.LedgerRunID,
			&seg.WorkerRunID, &seg.AnswerID, &seg.Terminal, &seg.Failure,
			&seg.RunDir, &seg.ClaimedAt, &seg.StartedAt, &seg.EndedAt); err != nil {
			rows.Close()
			return err
		}
		run.Segments = append(run.Segments, seg)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = s.db.Query(`SELECT run_id,segment,stage,occurrence,attempt,backend,state,session,
		detail,exit_code,turns,spend,processing_ms,tokens,at FROM weft_stages
		WHERE run_id=? ORDER BY rowid`, run.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var st WeftStage
		var spend, tokens sql.NullString
		if err := rows.Scan(&st.RunID, &st.Segment, &st.Stage, &st.Occurrence, &st.Attempt,
			&st.Backend, &st.State, &st.Session, &st.Detail, &st.ExitCode, &st.Turns,
			&spend, &st.Processing, &tokens, &st.At); err != nil {
			rows.Close()
			return err
		}
		if spend.Valid {
			st.Spend = json.RawMessage(spend.String)
		}
		if tokens.Valid {
			st.Tokens = json.RawMessage(tokens.String)
		}
		run.Stages = append(run.Stages, st)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	q, err := s.PendingWeftQuestion(run.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		q = nil
	}
	run.Question = q
	return nil
}

func (s *Store) PendingWeftQuestion(runID string) (*WeftQuestion, error) {
	return scanQuestion(s.db.QueryRow(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.run_id=? AND q.state='pending'`, runID))
}

func (s *Store) WeftQuestion(id string) (*WeftQuestion, error) {
	q, err := scanQuestion(s.db.QueryRow(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.id=?`, id))
	if err != nil {
		return nil, err
	}
	if q.AnswerID.Valid {
		q.Answer, err = s.weftAnswer(q.AnswerID.String)
	}
	return q, err
}

func (s *Store) PendingWeftQuestions() ([]*WeftQuestion, error) {
	rows, err := s.db.Query(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.state='pending' ORDER BY q.asked_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*WeftQuestion
	for rows.Next() {
		q, err := scanQuestion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func getQuestionTx(tx *sql.Tx, id string) (*WeftQuestion, error) {
	return scanQuestion(tx.QueryRow(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.id=?`, id))
}

func scanQuestion(sc scanner) (*WeftQuestion, error) {
	var q WeftQuestion
	err := sc.Scan(&q.ID, &q.RunID, &q.Ticket, &q.Segment, &q.Stage, &q.Occurrence,
		&q.Attempt, &q.Text, &q.AskedAt, &q.State, &q.AnswerID)
	return &q, err
}

func (s *Store) weftAnswer(id string) (*WeftAnswer, error) {
	var a WeftAnswer
	err := s.db.QueryRow(`SELECT id,question_id,run_id,text,submitted_at,segment,entry_id,consumed_segment
		FROM weft_answers WHERE id=?`, id).
		Scan(&a.ID, &a.QuestionID, &a.RunID, &a.Text, &a.SubmittedAt, &a.Segment, &a.EntryID, &a.ConsumedSegment)
	return &a, err
}

func (s *Store) WeftAnswer(id string) (*WeftAnswer, error) {
	return s.weftAnswer(id)
}

// WeftAsOf is the last seq the state stream allocated. It is read from the
// autoincrement counter rather than the retained rows, so it never moves
// backwards when retention prunes the newest events.
func (s *Store) WeftAsOf() (int64, error) {
	var seq int64
	err := s.db.QueryRow(`SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='weft_events'),0)`).Scan(&seq)
	return seq, err
}

func (s *Store) WeftEventBounds() (oldest, newest int64, err error) {
	err = s.db.QueryRow(`SELECT COALESCE(MIN(seq),0),COALESCE(MAX(seq),0) FROM weft_events`).Scan(&oldest, &newest)
	return
}

func (s *Store) PruneWeftRetention(before time.Time) error {
	cutoff := weftTimestamp(before)
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM weft_events WHERE ts<?`, cutoff); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM weft_requests WHERE created_at<?`, cutoff)
		return err
	})
}

func (s *Store) WeftEventsAfter(seq int64) ([]WeftEvent, error) {
	rows, err := s.db.Query(`SELECT seq,ts,run_id,type,payload FROM weft_events WHERE seq>? ORDER BY seq`, seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WeftEvent
	for rows.Next() {
		var e WeftEvent
		var payload string
		if err := rows.Scan(&e.Seq, &e.TS, &e.RunID, &e.Type, &payload); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) GetWeftRequest(tokenID, key string) (*WeftRequest, error) {
	var r WeftRequest
	err := s.db.QueryRow(`SELECT token_id,key,method,path,body_sha256,status,response,created_at
		FROM weft_requests WHERE token_id=? AND key=?`, tokenID, key).
		Scan(&r.TokenID, &r.Key, &r.Method, &r.Path, &r.BodySHA, &r.Status, &r.Response, &r.CreatedAt)
	return &r, err
}

func (s *Store) PutWeftRequest(r WeftRequest) error {
	_, err := s.db.Exec(`INSERT INTO weft_requests(token_id,key,method,path,body_sha256,status,response,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, r.TokenID, r.Key, r.Method, r.Path, r.BodySHA, r.Status, r.Response, r.CreatedAt)
	return err
}

func TokenID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func BodySHA(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// appendRunEventTx writes the v2 run projection (weft-service/v2 §10): every
// run transition, its end included, is one run event.
func appendRunEventTx(tx *sql.Tx, runID, stamp string) error {
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return err
	}
	var segment int
	if err := tx.QueryRow(`SELECT segment FROM weft_segments WHERE run_id=? ORDER BY segment DESC LIMIT 1`, runID).
		Scan(&segment); err != nil {
		return err
	}
	return appendWeftEventTx(tx, runID, "run", stamp, func(seq int64) any {
		return runEventPayload{
			eventBase: newEventBase(seq, stamp, runID),
			State:     stateOf(run.Phase), Phase: WeftV2Phase(run.Phase), Outcome: nullable(run.Outcome),
			Failed: failedOf(run), Stopped: run.Stopped, Segment: segment,
			ActionRequired: nullable(run.ActionRequired), Landing: run.SettlementState,
			Disposition: run.Disposition, Revision: run.Revision,
		}
	})
}

func appendStageEventTx(tx *sql.Tx, stage WeftStage) error {
	return appendWeftEventTx(tx, stage.RunID, "stage", stage.At, func(seq int64) any {
		return stageEventPayload{
			eventBase: newEventBase(seq, stage.At, stage.RunID),
			Stage: stageEventData{
				Segment: stage.Segment, Stage: stage.Stage, Backend: nullable(stage.Backend),
				Occurrence: stage.Occurrence, Attempt: stage.Attempt, State: stage.State,
				Session: nullable(stage.Session), Detail: nullable(stage.Detail),
				ExitCode: nullableInt(stage.ExitCode), Turns: nullableInt(stage.Turns),
				Spend: rawOrNil(stage.Spend), ProcessingMS: nullableInt(stage.Processing),
				Tokens: rawOrNil(stage.Tokens), At: stage.At,
			},
		}
	})
}

func appendQuestionEventTx(tx *sql.Tx, question *WeftQuestion, stamp string) error {
	var answer any
	if a := question.Answer; a != nil {
		answer = questionAnswerData{
			AnswerID: a.ID, Text: a.Text, SubmittedAt: a.SubmittedAt,
			EntryID: nullable(a.EntryID), ConsumedSegment: nullableInt(a.ConsumedSegment),
		}
	}
	return appendWeftEventTx(tx, question.RunID, "question", stamp, func(seq int64) any {
		return questionEventPayload{
			eventBase: newEventBase(seq, stamp, question.RunID),
			Question: questionEventData{
				QuestionID: question.ID, RunID: question.RunID, Ticket: question.Ticket,
				Segment: question.Segment, Stage: question.Stage, Occurrence: question.Occurrence,
				Attempt: question.Attempt, Text: question.Text, AskedAt: question.AskedAt,
				State: question.State, Answer: answer,
			},
		}
	})
}

func appendWeftEventTx(tx *sql.Tx, runID, typ, stamp string, payload func(int64) any) error {
	res, err := tx.Exec(`INSERT INTO weft_events(ts,run_id,type,payload) VALUES(?,?,?,'{}')`, stamp, runID, typ)
	if err != nil {
		return err
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return err
	}
	doc, err := json.Marshal(payload(seq))
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE weft_events SET payload=? WHERE seq=?`, string(doc), seq)
	return err
}

type eventBase struct {
	ContractVersion string `json:"contract_version"`
	Seq             int64  `json:"seq"`
	TS              string `json:"ts"`
	RunID           string `json:"run_id"`
}

func newEventBase(seq int64, stamp, runID string) eventBase {
	return eventBase{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp, RunID: runID}
}

type runEventPayload struct {
	eventBase
	State          string `json:"state"`
	Phase          string `json:"phase"`
	Outcome        any    `json:"outcome"`
	Failed         bool   `json:"failed"`
	Stopped        bool   `json:"stopped"`
	Segment        int    `json:"segment"`
	ActionRequired any    `json:"action_required"`
	Landing        string `json:"landing"`
	Disposition    string `json:"disposition"`
	Revision       int64  `json:"revision"`
}

type stageEventPayload struct {
	eventBase
	Stage stageEventData `json:"stage"`
}

type stageEventData struct {
	Segment      int    `json:"segment"`
	Stage        string `json:"stage"`
	Backend      any    `json:"backend"`
	Occurrence   int    `json:"occurrence"`
	Attempt      int    `json:"attempt"`
	State        string `json:"state"`
	Session      any    `json:"session"`
	Detail       any    `json:"detail"`
	ExitCode     any    `json:"exit_code"`
	Turns        any    `json:"turns"`
	Spend        any    `json:"spend"`
	ProcessingMS any    `json:"processing_ms"`
	Tokens       any    `json:"tokens"`
	At           string `json:"at"`
}

type questionEventPayload struct {
	eventBase
	Question questionEventData `json:"question"`
}

type questionEventData struct {
	QuestionID string `json:"question_id"`
	RunID      string `json:"run_id"`
	Ticket     string `json:"ticket"`
	Segment    int    `json:"segment"`
	Stage      string `json:"stage"`
	Occurrence int    `json:"occurrence"`
	Attempt    int    `json:"attempt"`
	Text       string `json:"text"`
	AskedAt    string `json:"asked_at"`
	State      string `json:"state"`
	Answer     any    `json:"answer"`
}

type questionAnswerData struct {
	AnswerID        string `json:"answer_id"`
	Text            string `json:"text"`
	SubmittedAt     string `json:"submitted_at"`
	EntryID         any    `json:"entry_id"`
	ConsumedSegment any    `json:"consumed_segment"`
}

func aggregateWeftSegmentTx(tx *sql.Tx, runID string, segment int) error {
	rows, err := tx.Query(`SELECT turns,spend,tokens FROM weft_stages WHERE run_id=? AND segment=?`, runID, segment)
	if err != nil {
		return err
	}
	var (
		turns, input, output             int64
		turnsSeen, inputSeen, outputSeen bool
		cost                             float64
		costSeen, unpriced               bool
	)
	for rows.Next() {
		var n sql.NullInt64
		var spendDoc, tokensDoc sql.NullString
		if err := rows.Scan(&n, &spendDoc, &tokensDoc); err != nil {
			rows.Close()
			return err
		}
		if n.Valid {
			turns, turnsSeen = turns+n.Int64, true
		}
		if spendDoc.Valid {
			var spend struct {
				USD      float64 `json:"usd"`
				Unpriced int64   `json:"unpriced"`
			}
			if json.Unmarshal([]byte(spendDoc.String), &spend) == nil {
				cost, costSeen = cost+spend.USD, true
				unpriced = unpriced || spend.Unpriced > 0
			}
		}
		if tokensDoc.Valid {
			var tokens struct {
				Input  int64 `json:"input"`
				Output int64 `json:"output"`
			}
			if json.Unmarshal([]byte(tokensDoc.String), &tokens) == nil {
				input, output = input+tokens.Input, output+tokens.Output
				inputSeen, outputSeen = true, true
			}
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var ledgerID string
	if err := tx.QueryRow(`SELECT ledger_run_id FROM weft_segments WHERE run_id=? AND segment=?`, runID, segment).Scan(&ledgerID); err != nil {
		return err
	}
	var costValue any
	if costSeen && !unpriced {
		costValue = cost
	}
	var turnsValue, inputValue, outputValue any
	if turnsSeen {
		turnsValue = turns
	}
	if inputSeen {
		inputValue = input
	}
	if outputSeen {
		outputValue = output
	}
	_, err = tx.Exec(`UPDATE runs SET num_turns=?,cost_usd=?,input_tokens=?,output_tokens=? WHERE id=?`,
		turnsValue, costValue, inputValue, outputValue, ledgerID)
	return err
}

// WeftV2Phase is weft-service/v2's name for a run phase: a v1 run claimed but
// not yet started is starting.
func WeftV2Phase(phase string) string {
	if phase == WeftPhaseQueued {
		return "starting"
	}
	return phase
}

func stateOf(phase string) string {
	switch phase {
	case WeftPhaseParked:
		return "blocked"
	case WeftPhaseEnded:
		return "over"
	default:
		return "active"
	}
}

func failedOf(run *WeftRun) bool {
	return run.Phase == WeftPhaseEnded && run.Outcome.String == "failed"
}

func nullable(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}
func nullableInt(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}
func rawOrNil(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	return v
}
func nullJSON(v json.RawMessage) sql.NullString {
	return sql.NullString{String: string(v), Valid: len(v) > 0 && string(v) != "null"}
}

func newRemoteID(prefix string, now time.Time) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s-%s", prefix, now.UTC().Format("20060102T150405Z"), hex.EncodeToString(b[:])), nil
}

func weftTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
