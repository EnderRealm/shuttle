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
	// Liveness is the latest liveness the worker reported, as its resource
	// (weft-service/v2 §8.3), or nil.
	Liveness json.RawMessage

	Segments []WeftSegment
	Stages   []WeftStage
	Question *WeftQuestion
	// Steps are the landing's external steps as the host recorded them (§7.1).
	Steps []WeftLandStep
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
	// WorkerContract is the worker protocol the segment was launched under;
	// NULL is a segment launched before it was recorded, under weft-remote/v1.
	WorkerContract sql.NullString
	// AgentUser is the agent user the segment runs as, held from its claim
	// until it settles; NULL for a maintenance segment and for one claimed
	// before hosted segments ran as agent users.
	AgentUser sql.NullString
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

// WeftQuestion is a park in either form (§6.5): Questions, a structured
// park's questions as the contract's JSON array, or, when it is nil, the
// legacy free-text Text.
type WeftQuestion struct {
	ID         string
	RunID      string
	Ticket     string
	Segment    int
	Stage      string
	Occurrence int
	Attempt    int
	Text       string
	Questions  json.RawMessage
	AskedAt    string
	State      string
	AnswerID   sql.NullString
	Answer     *WeftAnswer
}

// WeftAnswer is an answer in its question's form: Answers, a structured
// answer's entries as the contract's JSON array, or, when it is nil, the
// legacy free-text Text.
type WeftAnswer struct {
	ID              string
	QuestionID      string
	RunID           string
	Text            string
	Answers         json.RawMessage
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
	// AgentUser is the agent user the first segment is claimed with.
	AgentUser string
}

// ClaimWeft creates an API run, its first phase-1 ledger segment and the
// repository reservation in one transaction. It is weft-remote/v1's
// admission, so the run is recorded under that protocol, which never lands.
// weft-service/v2 mints its runs at dispatch instead (weftqueue.go).
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
	runID, err := mintWeftRunTx(tx, p, now, "", WeftRecordedV1, WeftSettlementNone, WeftV1SettlementDetail)
	if err != nil {
		return nil, err
	}
	if err := acquireRepository(tx, sandbox.RepositoryID(p.Repo), p.Repo, WeftHolderRun, runID, weftTimestamp(now)); err != nil {
		return nil, err
	}
	if err := appendRunEventTx(tx, runID, weftTimestamp(now)); err != nil {
		return nil, err
	}
	if err := s.commit(tx); err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

// mintWeftRunTx creates a run and its first launch segment with the
// segment's phase-1 ledger row, its settlement as given. The caller takes
// the repository for it and writes the run event.
func mintWeftRunTx(tx *sql.Tx, p WeftLaunch, now time.Time, entryID, recordedUnder, settlement, settlementDetail string) (string, error) {
	runID, err := newRemoteID("r", now)
	if err != nil {
		return "", err
	}
	ledgerID, err := NewRunID(WeftAgent, now.UTC())
	if err != nil {
		return "", err
	}
	stamp := weftTimestamp(now)
	runDir := filepath.Join(p.RunDirRoot, ledgerID)
	repositoryID := sandbox.RepositoryID(p.Repo)
	if _, err := tx.Exec(`INSERT INTO weft_runs(
		id,ticket,project,repo,branch,worktree,phase,created_at,updated_at,
		repository_id,entry_id,origin,recorded_under,disposition,settlement_state,settlement_detail)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		runID, p.Ticket, p.Project, p.Repo, p.Branch, p.Worktree,
		WeftPhaseQueued, stamp, stamp, repositoryID, nullString(entryID), WeftOriginHosted, recordedUnder,
		WeftDispositionRetained, settlement, nullString(settlementDetail)); err != nil {
		return "", err
	}
	if err := insertWeftLedgerRunTx(tx, ledgerID, runDir, stamp, now); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`INSERT INTO weft_segments(run_id,segment,kind,ledger_run_id,agent_user)
		VALUES(?,?,?,?,?)`, runID, 1, "launch", ledgerID, nullString(p.AgentUser)); err != nil {
		return "", err
	}
	return runID, nil
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
	if segment > WeftSegmentLimit {
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
	if _, err := claimNextSegmentTx(tx, run, "answer", answerID, "", runDirRoot, s.now()); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET summary=NULL WHERE id=?`, runID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO weft_answers(id,question_id,run_id,text,submitted_at,segment)
		VALUES(?,?,?,?,?,?)`, answerID, questionID, runID, text, stamp, segment); err != nil {
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
	if err := s.commit(tx); err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

// WeftSegmentLimit is the most segments one run may claim (v1 §3.8).
const WeftSegmentLimit = 8

// claimNextSegmentTx claims the run's next segment of kind — an answer or a
// resume — as agentUser with its phase-1 ledger row, takes the repository
// reservation and moves the run back to queued, keeping the last summary
// until the worker reports another. The caller writes the run event.
func claimNextSegmentTx(tx *sql.Tx, run *WeftRun, kind, answerID, agentUser, runDirRoot string, now time.Time) (int, error) {
	segment, err := nextSegmentTx(tx, run.ID)
	if err != nil {
		return 0, err
	}
	if segment > WeftSegmentLimit {
		return 0, ErrWeftSegmentLimit
	}
	stamp := weftTimestamp(now)
	if err := acquireRepository(tx, run.RepositoryID, run.Repo, WeftHolderRun, run.ID, stamp); err != nil {
		return 0, err
	}
	if _, err := insertWeftSegmentTx(tx, run.ID, segment, kind, answerID, agentUser, runDirRoot, now); err != nil {
		return 0, err
	}
	// A new pipeline segment is a new attempt: a stop of an earlier one does
	// not carry into it.
	if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=NULL,stopped=0,updated_at=?,ended_at=NULL,
		revision=revision+1 WHERE id=?`, WeftPhaseQueued, stamp, run.ID); err != nil {
		return 0, err
	}
	return segment, nil
}

// resumeAnswerTx is the answer a resume segment carries (v1 §3.8): the last
// segment's, when that segment never said hello and so never consumed it.
func resumeAnswerTx(tx *sql.Tx, runID string) (string, error) {
	var lastAnswer, lastWorker sql.NullString
	if err := tx.QueryRow(`SELECT answer_id,worker_run_id FROM weft_segments WHERE run_id=? AND kind IN `+weftPipelineKinds+`
		ORDER BY segment DESC LIMIT 1`, runID).
		Scan(&lastAnswer, &lastWorker); err != nil {
		return "", err
	}
	if !lastWorker.Valid && lastAnswer.Valid {
		return lastAnswer.String, nil
	}
	return "", nil
}

// weftResumableTx is the one rule for a run another segment may resume
// (docs/weft-landing-lifecycle.md §4, v1 §3.8). The run is live history, over,
// held for nothing, and has no landing — none settled and no step recorded, so
// a new segment never meets an earlier landing's rows. It ended stopped, or
// failed under one of the host's classes: the rule reads the outcome, never
// the stop flag, so a finished grade is never answered by a resume.
func weftResumableTx(tx *sql.Tx, run *WeftRun) (bool, error) {
	if run.Origin == WeftOriginArchive || run.Disposition == WeftDispositionArchived || run.Phase != WeftPhaseEnded ||
		run.ActionRequired.String == WeftActionStaleBase || run.ActionRequired.String == WeftActionLandingUnsettled ||
		run.SettlementState != WeftSettlementNone {
		return false, nil
	}
	var steps int
	if err := tx.QueryRow(`SELECT count(*) FROM weft_land_steps WHERE run_id=?`, run.ID).Scan(&steps); err != nil {
		return false, err
	}
	switch {
	case steps > 0:
		return false, nil
	case run.Outcome.String == "stopped":
		return true, nil
	case run.Outcome.String != "failed":
		return false, nil
	}
	// A failed the worker graded is its ended's grade (v1 §3.8), never
	// resumable, even when its segment is then lost before ended.
	var lastFailure sql.NullString
	var graded bool
	if err := tx.QueryRow(`SELECT failure,graded FROM weft_segments WHERE run_id=? AND kind IN `+weftPipelineKinds+`
		ORDER BY segment DESC LIMIT 1`, run.ID).Scan(&lastFailure, &graded); err != nil {
		return false, err
	}
	return isHostFailure(lastFailure.String) && !graded, nil
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

func insertWeftSegmentTx(tx *sql.Tx, runID string, segment int, kind, answerID, agentUser, runDirRoot string, now time.Time) (string, error) {
	ledgerID, err := NewRunID(WeftAgent, now.UTC())
	if err != nil {
		return "", err
	}
	stamp := weftTimestamp(now)
	runDir := filepath.Join(runDirRoot, ledgerID)
	if err := insertWeftLedgerRunTx(tx, ledgerID, runDir, stamp, now); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`INSERT INTO weft_segments(run_id,segment,kind,ledger_run_id,answer_id,agent_user)
		VALUES(?,?,?,?,?,?)`, runID, segment, kind, ledgerID, nullString(answerID), nullString(agentUser)); err != nil {
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
	resumable, err := weftResumableTx(tx, run)
	if err != nil {
		return nil, err
	}
	if !resumable {
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
	answerID, err := resumeAnswerTx(tx, runID)
	if err != nil {
		return nil, err
	}
	if _, err := claimNextSegmentTx(tx, run, "resume", answerID, "", runDirRoot, s.now()); err != nil {
		return nil, err
	}
	if err := appendRunEventTx(tx, runID, weftTimestamp(s.now())); err != nil {
		return nil, err
	}
	if err := s.commit(tx); err != nil {
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

// MarkWeftRunning records the child process, and the worker contract it was
// launched under, before any worker event is read.
func (s *Store) MarkWeftRunning(runID string, segment, pid int, contract string) error {
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE weft_segments SET worker_contract=? WHERE run_id=? AND segment=?`,
			contract, runID, segment); err != nil {
			return err
		}
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
// segment's answer, so the answer is consumed by this segment. A prior
// segment's liveness says nothing about this one, so it is cleared.
func (s *Store) RecordWeftHello(runID string, segment int, workerRunID, base, criteria string) error {
	return s.recordWeftHello(runID, segment, workerRunID, base, criteria, nil)
}

// RecordWeftLandingHello is a weft-worker/v2 hello. A segment launched with
// --land is a landing segment from here (§7.4) and its run's settlement is
// pending; one launched without it, on a host that no longer lands, leaves a
// pending settlement not_applicable.
func (s *Store) RecordWeftLandingHello(runID string, segment int, workerRunID, base, criteria string, land bool) error {
	return s.recordWeftHello(runID, segment, workerRunID, base, criteria, &land)
}

func (s *Store) recordWeftHello(runID string, segment int, workerRunID, base, criteria string, land *bool) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET worker_run_id=? WHERE run_id=? AND segment=? AND worker_run_id IS NULL`,
			workerRunID, runID, segment); err != nil {
			return err
		}
		switch {
		case land != nil && *land:
			if _, err := tx.Exec(`UPDATE weft_segments SET land=1 WHERE run_id=? AND segment=?`, runID, segment); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_step=NULL,settlement_reason=NULL,
				settlement_detail=NULL,settlement_branch=NULL,settlement_commit=NULL WHERE id=?`, WeftSettlementPending, runID); err != nil {
				return err
			}
		case land != nil:
			if _, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_detail=? WHERE id=? AND settlement_state=?`,
				WeftSettlementNone, WeftLandingNotServed, runID, WeftSettlementPending); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE weft_answers SET consumed_segment=? WHERE consumed_segment IS NULL
			AND id=(SELECT answer_id FROM weft_segments WHERE run_id=? AND segment=?)`, segment, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET base=?,criteria=?,phase=?,liveness=NULL,updated_at=?,revision=revision+1 WHERE id=?`,
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

// RecordWeftParked records a pending question — the legacy question, or a
// structured park's questions when questions is non-nil; process settlement
// waits for reap.
func (s *Store) RecordWeftParked(runID string, segment int, stage string, occurrence, attempt int, question string, questions json.RawMessage, head string, summary json.RawMessage) (string, error) {
	qid, err := newRemoteID("q", s.now())
	if err != nil {
		return "", err
	}
	err = s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		var phase string
		if err := tx.QueryRow(`SELECT phase FROM weft_runs WHERE id=?`, runID).Scan(&phase); err != nil {
			return err
		}
		if phase == WeftPhaseSettling {
			return fmt.Errorf("parked while the run's finished grade is settling")
		}
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='parked' WHERE run_id=? AND segment=?`, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,head=?,summary=?,updated_at=?,revision=revision+1 WHERE id=?`,
			WeftPhaseParked, head, string(summary), stamp, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO weft_questions(
			id,run_id,segment,stage,occurrence,attempt,text,questions,asked_at,state)
			VALUES(?,?,?,?,?,?,?,?,?,?)`, qid, runID, segment, stage, occurrence, attempt, question,
			nullJSON(questions), stamp, "pending"); err != nil {
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

// RecordWeftEnded records the worker's terminal event; settlement waits for
// reap. A weft-service/v2 run's operator stop lasts the attempt, so no ended
// clears it; a weft-remote/v1 run takes the worker's flag.
func (s *Store) RecordWeftEnded(runID string, segment int, outcome string, stopped bool, haltStage, haltReason, haltDetail, failure, detail, head string, summary json.RawMessage) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='ended',failure=? WHERE run_id=? AND segment=?`,
			nullString(failure), runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=?,halt_stage=?,halt_reason=?,halt_detail=?,
			head=?,stopped=CASE WHEN recorded_under=? THEN ? ELSE MAX(stopped,?) END,summary=?,updated_at=?,ended_at=?,
			revision=revision+1 WHERE id=?`,
			WeftPhaseEnded, outcome, nullString(haltStage), nullString(haltReason),
			nullString(firstNonempty(haltDetail, detail)), nullString(head), WeftRecordedV1, stopped, stopped,
			string(summary), stamp, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// RecordWeftGradedBeforeSummary records the grade of a segment launched
// without --land whose summary follows it: the outcome and head stand, and
// the run stays live through its summary step until RecordWeftEnded closes
// it. A weft-service/v2 operator stop lasts the attempt, so no grade clears it.
func (s *Store) RecordWeftGradedBeforeSummary(runID string, outcome string, stopped bool, haltStage, haltReason, haltDetail, detail, head string) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_runs SET outcome=?,halt_stage=?,halt_reason=?,halt_detail=?,
			head=?,stopped=MAX(stopped,?),updated_at=?,revision=revision+1 WHERE id=?`,
			outcome, nullString(haltStage), nullString(haltReason), nullString(firstNonempty(haltDetail, detail)),
			nullString(head), stopped, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// RecordWeftStaleBase records an answer or resume segment the worker refused
// before hello because the repository's default branch moved past the run's
// base (weft-service/v2 §6.7). The run is held blocked with stale_base for the
// operator: branch, worktree and any accepted answer are kept, the answer
// unconsumed, and nothing relaunches it. Settlement waits for reap.
func (s *Store) RecordWeftStaleBase(runID string, segment int, detail string) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='ended',failure=? WHERE run_id=? AND segment=?`,
			WeftActionStaleBase, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=NULL,action_required=?,action_detail=?,
			updated_at=?,revision=revision+1 WHERE id=?`,
			WeftPhaseParked, WeftActionStaleBase, detail, stamp, runID); err != nil {
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
		seg, err := segmentStateTx(tx, runID, segment)
		if err != nil {
			return err
		}
		event, err = completeWeftSegmentTx(tx, runID, seg, s.now())
		return err
	})
	if err == nil && s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return err
}

func completeWeftSegmentTx(tx *sql.Tx, runID string, seg *segmentState, now time.Time) (Event, error) {
	event := Event{Agent: WeftAgent, RunID: seg.ledgerID}
	var last sql.NullString
	if err := tx.QueryRow(`SELECT (SELECT status FROM runs WHERE agent=? AND status IN (?,?)
		ORDER BY ended_at DESC,rowid DESC LIMIT 1)`, WeftAgent, StatusCompleted, StatusFailed).Scan(&last); err != nil {
		return event, err
	}
	event.Recovered = last.String == StatusFailed
	stamp := weftTimestamp(now)
	if err := completeSegmentTx(tx, seg.ledgerID, stamp); err != nil {
		return event, err
	}
	return event, settleReapedTx(tx, runID, seg, stamp)
}

// SettleWeftSegment settles a reaped segment as CompleteWeftSegment does, or
// with class as FailWeftSegment does, and in the same commit evaluates its
// run for a host-started cleanup under d (§6.8): a run's last process reaped
// is when it can newly qualify. The reap of a cleanup worker is not: one that
// ended without reporting would be queued and relaunched in a loop. spawn
// names the runs whose cleanup that dispatched, for the caller to start.
func (s *Store) SettleWeftSegment(runID string, segment int, class, detail string, d WeftDispatch) (reconcile bool, spawn []string, changes []WeftChange, err error) {
	var event Event
	err = s.WeftTx(func(t *WeftTx) error {
		seg, err := segmentStateTx(t.tx, runID, segment)
		if err != nil {
			return err
		}
		if class == "" {
			event, err = completeWeftSegmentTx(t.tx, runID, seg, s.now())
		} else {
			event, reconcile, err = failWeftSegmentTx(t.tx, runID, seg, class, detail, s.now())
		}
		if err != nil {
			return err
		}
		if seg.kind != WeftSegmentCleanup {
			if err := t.CleanFinishedRun(runID, d); err != nil {
				return err
			}
		}
		spawn, changes = t.Spawn, t.Changes
		return nil
	})
	if err == nil && s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return reconcile, spawn, changes, err
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

// FailWeftSegment settles a host-side process failure. A pipeline segment
// leaves the run resumable; a landing segment gone without its terminal event
// while its landing is open is never replayed — its run goes reconciling and
// reconcile is true: a reconcile segment of the run is claimed under its
// reservation, for the caller to start (§7.4). One whose grade already closed
// the landing keeps that grade and is settled as reaped. A reconcile or
// cleanup segment changes the run only as its own kind settles.
func (s *Store) FailWeftSegment(runID string, segment int, class, detail string) (reconcile bool, err error) {
	var event Event
	err = s.withTx(func(tx *sql.Tx) error {
		seg, err := segmentStateTx(tx, runID, segment)
		if err != nil {
			return err
		}
		event, reconcile, err = failWeftSegmentTx(tx, runID, seg, class, detail, s.now())
		return err
	})
	if err == nil && s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return reconcile, err
}

func failWeftSegmentTx(tx *sql.Tx, runID string, seg *segmentState, class, detail string, now time.Time) (event Event, reconcile bool, err error) {
	stamp := weftTimestamp(now)
	if event, err = failSegmentRowTx(tx, runID, seg, class, detail, stamp); err != nil {
		return event, false, err
	}
	open, err := landingOpenTx(tx, runID)
	if err != nil {
		return event, false, err
	}
	switch {
	case seg.kind == WeftSegmentReconcile || seg.kind == WeftSegmentCleanup:
		return event, false, settleLostMaintenanceTx(tx, runID, seg, "the "+seg.kind+" worker ended without reporting: "+detail, stamp)
	case seg.land && (seg.terminal.Valid || !open):
		// The worker's terminal report stands, or its grade already
		// closed a landing it never started (T3); the run reads the
		// failure class recorded for its segment.
		if _, err := tx.Exec(`UPDATE weft_runs SET updated_at=?,revision=revision+1 WHERE id=?`, stamp, runID); err != nil {
			return event, false, err
		}
		if err := appendRunEventTx(tx, runID, stamp); err != nil {
			return event, false, err
		}
		return event, false, settleReapedTx(tx, runID, seg, stamp)
	case seg.land:
		reconcile, err = loseLandingTx(tx, runID, seg, now)
		return event, reconcile, err
	}
	var stopped bool
	if err := tx.QueryRow(`SELECT stopped FROM weft_runs WHERE id=?`, runID).Scan(&stopped); err != nil {
		return event, false, err
	}
	if _, err := tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE run_id=? AND state='pending'`, runID); err != nil {
		return event, false, err
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
		return event, false, err
	}
	if _, err := settleUngradedTx(tx, runID); err != nil {
		return event, false, err
	}
	if err := appendRunEventTx(tx, runID, stamp); err != nil {
		return event, false, err
	}
	if err := releaseRunTx(tx, runID, stamp); err != nil {
		return event, false, err
	}
	return event, false, capacityStepTx(tx, stamp)
}

// failSegmentRowTx records a segment's host-side failure on its phase-1 row —
// its class, the failure streak and the incident — and on the segment and the
// stages it left running.
func failSegmentRowTx(tx *sql.Tx, runID string, seg *segmentState, class, detail, stamp string) (Event, error) {
	agent, err := requireStatus(tx, seg.ledgerID, StatusFailed, StatusClaimed, StatusRunning)
	if err != nil {
		return Event{}, err
	}
	signature := Signature(class, detail)
	event := Event{Agent: agent, RunID: seg.ledgerID, Class: class, Signature: signature}
	if _, err := tx.Exec(`UPDATE runs SET status=?,ended_at=?,failure_class=?,error_signature=? WHERE id=?`,
		StatusFailed, stamp, class, signature, seg.ledgerID); err != nil {
		return event, err
	}
	if event.PausedReason, err = recordFailure(tx, agent, class, stamp); err != nil {
		return event, err
	}
	if err := upsertIncident(tx, agent, signature, seg.ledgerID, stamp); err != nil {
		return event, err
	}
	var count int
	if err := tx.QueryRow(`SELECT id,count FROM incidents WHERE agent=? AND signature=? AND acknowledged_at IS NULL`,
		agent, signature).Scan(&event.IncidentID, &count); err != nil {
		return event, err
	}
	event.NewIncident = count == 1
	if _, err := tx.Exec(`UPDATE weft_segments SET failure=? WHERE run_id=? AND segment=?`, class, runID, seg.segment); err != nil {
		return event, err
	}
	return event, closeRunningStagesTx(tx, runID, seg, detail, stamp)
}

// closeRunningStagesTx fails each stage a segment settled without its
// terminal event left running, with the segment's failure detail: its worker
// is gone, so nothing will report that stage again. A segment whose terminal
// event was recorded keeps its stages as the worker reported them.
func closeRunningStagesTx(tx *sql.Tx, runID string, seg *segmentState, detail, stamp string) error {
	if seg.terminal.Valid {
		return nil
	}
	rows, err := tx.Query(`SELECT `+weftStageColumns+` FROM weft_stages
		WHERE run_id=? AND segment=? AND state='running' ORDER BY rowid`, runID, seg.segment)
	if err != nil {
		return err
	}
	var running []WeftStage
	for rows.Next() {
		st, err := scanWeftStage(rows)
		if err != nil {
			rows.Close()
			return err
		}
		running = append(running, st)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, st := range running {
		st.State, st.Detail, st.At = "failed", nullString(detail), stamp
		if _, err := tx.Exec(`UPDATE weft_stages SET state=?,detail=?,at=?
			WHERE run_id=? AND segment=? AND stage=? AND occurrence=? AND attempt=?`,
			st.State, st.Detail, st.At, st.RunID, st.Segment, st.Stage, st.Occurrence, st.Attempt); err != nil {
			return err
		}
		if err := appendStageEventTx(tx, st); err != nil {
			return err
		}
	}
	return nil
}

// MarkWeftStopped makes an operator stop durable before the process is asked
// to cooperate. The supervisor subsequently records the worker's ended event
// or a host-side failure, both preserving the stopped outcome.
func (s *Store) MarkWeftStopped(runID string) error {
	return s.withTx(func(tx *sql.Tx) error {
		return markWeftStoppedTx(tx, runID, weftTimestamp(s.now()))
	})
}

func markWeftStoppedTx(tx *sql.Tx, runID, stamp string) error {
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return err
	}
	if run.Phase == WeftPhaseEnded {
		return ErrWeftRunOver
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET stopped=1,phase=?,outcome='stopped',
		summary=?,updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`, WeftPhaseEnded,
		weftStoppedSummary, stamp, stamp, runID); err != nil {
		return err
	}
	return appendRunEventTx(tx, runID, stamp)
}

const weftStoppedSummary = `{"state":"unavailable","reason":"stopped by the operator"}`

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
		changed, err := settleUngradedTx(tx, runID)
		if err != nil {
			return err
		}
		if changed {
			if _, err := tx.Exec(`UPDATE weft_runs SET updated_at=?,revision=revision+1 WHERE id=?`, stamp, runID); err != nil {
				return err
			}
			if err := appendRunEventTx(tx, runID, stamp); err != nil {
				return err
			}
		}
		if err := releaseRunTx(tx, runID, stamp); err != nil {
			return err
		}
		return capacityStepTx(tx, stamp)
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
		return stopWeftParkedTx(tx, runID, weftTimestamp(s.now()))
	})
	if err != nil {
		return nil, err
	}
	return s.GetWeftRun(runID)
}

func stopWeftParkedTx(tx *sql.Tx, runID, stamp string) error {
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
	pending, err := pendingWeftQuestion(tx, runID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		pending = nil
	case err != nil:
		return err
	}
	if _, err := tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE run_id=? AND state='pending'`, runID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome='stopped',stopped=1,
		summary=?,updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`, WeftPhaseEnded,
		weftStoppedSummary, stamp, stamp, runID); err != nil {
		return err
	}
	if _, err := settleUngradedTx(tx, runID); err != nil {
		return err
	}
	if pending != nil {
		withdrawn, err := getQuestionTx(tx, pending.ID)
		if err != nil {
			return err
		}
		if err := appendQuestionEventTx(tx, withdrawn, stamp); err != nil {
			return err
		}
	}
	return appendRunEventTx(tx, runID, stamp)
}

// ReleaseWeftRepository releases only the reservation held by this run.
func (s *Store) ReleaseWeftRepository(runID string) error {
	return s.withTx(func(tx *sql.Tx) error {
		return releaseRunTx(tx, runID, weftTimestamp(s.now()))
	})
}

// WeftAgentSegment is the hosted weft segment a phase-1 row is, when that
// segment holds an agent user.
type WeftAgentSegment struct {
	RunID     string
	Segment   int
	AgentUser string
}

// WeftSegmentAgent answers, for a phase-1 row that is a hosted weft segment
// holding an agent user, the segment and its user, and ok false for any
// other row.
func (s *Store) WeftSegmentAgent(ledgerRunID string) (WeftAgentSegment, bool, error) {
	var a WeftAgentSegment
	err := s.db.QueryRow(`SELECT run_id,segment,agent_user FROM weft_segments WHERE ledger_run_id=? AND agent_user IS NOT NULL`,
		ledgerRunID).Scan(&a.RunID, &a.Segment, &a.AgentUser)
	if errors.Is(err, sql.ErrNoRows) {
		return WeftAgentSegment{}, false, nil
	}
	if err != nil {
		return WeftAgentSegment{}, false, err
	}
	return a, true, nil
}

// WeftLegacyCodingWorktree answers, for a phase-1 row that is a hosted weft
// segment launched before agent users — one holding no agent user, of any
// kind but the maintenance kinds, which launch no model and never hold one —
// its run's worktree, and ok false for any other row.
func (s *Store) WeftLegacyCodingWorktree(ledgerRunID string) (string, bool, error) {
	var worktree string
	err := s.db.QueryRow(`SELECT w.worktree FROM weft_segments s JOIN weft_runs w ON w.id=s.run_id
		WHERE s.ledger_run_id=? AND s.agent_user IS NULL AND s.kind NOT IN (?,?,?)`, ledgerRunID,
		WeftSegmentReconcile, WeftSegmentCleanup, WeftSegmentArchive).Scan(&worktree)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return worktree, true, nil
}

// ReconcileWeftUnknown projects a phase-1 restart closure into the API ledger
// and releases the reservation only after reap established the process gone.
// It is §12 steps 1 and 2: a landing segment the pass closed without its
// terminal event while its landing was open moves its run to reconciling,
// passes its row to settlement and claims the reconcile Restart starts — one
// whose grade closed the landing is settled as reaped; a reconcile or cleanup segment
// settles as its own kind; an archive segment is left to step 5.
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
		seg, err := segmentStateTx(tx, runID, segment)
		if err != nil {
			return err
		}
		const restarted = "daemon restarted before the segment settled"
		if seg.kind == WeftSegmentArchive {
			return nil
		}
		if err := closeRunningStagesTx(tx, runID, seg, restarted, stamp); err != nil {
			return err
		}
		switch {
		case seg.kind == WeftSegmentReconcile || seg.kind == WeftSegmentCleanup:
			if !seg.terminal.Valid {
				if _, err := tx.Exec(`UPDATE weft_segments SET failure=? WHERE run_id=? AND segment=?`,
					ClassUnknown, runID, segment); err != nil {
					return err
				}
			}
			return settleLostMaintenanceTx(tx, runID, seg, restarted, stamp)
		case seg.land && seg.terminal.Valid:
			return settleReapedTx(tx, runID, seg, stamp)
		case seg.land:
			if _, err := tx.Exec(`UPDATE weft_segments SET failure=? WHERE run_id=? AND segment=?`,
				ClassUnknown, runID, segment); err != nil {
				return err
			}
			open, err := landingOpenTx(tx, runID)
			if err != nil {
				return err
			}
			if open {
				_, err := loseLandingTx(tx, runID, seg, s.now())
				return err
			}
			// Graded other than finished (T3): nothing lands, so there is
			// nothing to reconcile; the grade stands with the class.
			if _, err := tx.Exec(`UPDATE weft_runs SET updated_at=?,revision=revision+1 WHERE id=?`, stamp, runID); err != nil {
				return err
			}
			if err := appendRunEventTx(tx, runID, stamp); err != nil {
				return err
			}
			return settleReapedTx(tx, runID, seg, stamp)
		}
		if _, err := tx.Exec(`UPDATE weft_segments SET failure=? WHERE run_id=? AND segment=?`,
			ClassUnknown, runID, segment); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE run_id=? AND state='pending'`, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome='failed',halt_detail=?,
			updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`, WeftPhaseEnded,
			restarted, stamp, stamp, runID); err != nil {
			return err
		}
		if _, err := settleUngradedTx(tx, runID); err != nil {
			return err
		}
		if err := appendRunEventTx(tx, runID, stamp); err != nil {
			return err
		}
		if err := releaseRunTx(tx, runID, stamp); err != nil {
			return err
		}
		return capacityStepTx(tx, stamp)
	})
}

func (s *Store) GetWeftRun(id string) (*WeftRun, error) {
	return getWeftRunFull(s.db, id)
}

// querier is what a read needs, so one read serves the store and a
// transaction alike: the ledger has one connection, and a read issued on the
// store from inside a transaction would wait on that transaction forever.
type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

func getWeftRunFull(q querier, id string) (*WeftRun, error) {
	run, err := scanWeftRun(q.QueryRow(`SELECT `+weftRunColumns+` FROM weft_runs WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("weft run %s: not found", id)
	}
	if err != nil {
		return nil, err
	}
	if err := loadWeftChildren(q, run); err != nil {
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
	action_detail,disposition,archive_ref,supersedes,superseded_by,revision,liveness`

func getWeftRunTx(tx *sql.Tx, id string) (*WeftRun, error) {
	return scanWeftRun(tx.QueryRow(`SELECT `+weftRunColumns+` FROM weft_runs WHERE id=?`, id))
}

func scanWeftRun(sc scanner) (*WeftRun, error) {
	var run WeftRun
	var summary, liveness sql.NullString
	err := sc.Scan(&run.ID, &run.Ticket, &run.Project, &run.Repo, &run.Branch, &run.Worktree,
		&run.Base, &run.Criteria, &run.Phase, &run.Outcome, &run.HaltStage, &run.HaltReason,
		&run.HaltDetail, &run.Head, &run.Stopped, &summary, &run.CreatedAt, &run.UpdatedAt, &run.EndedAt,
		&run.RepositoryID, &run.EntryID, &run.Origin, &run.RecordedUnder, &run.ArchiveID,
		&run.SettlementState, &run.SettlementStep, &run.SettlementReason, &run.SettlementDetail,
		&run.SettlementBranch, &run.SettlementCommit, &run.ActionRequired, &run.ActionDetail,
		&run.Disposition, &run.ArchiveRef, &run.Supersedes, &run.SupersededBy, &run.Revision, &liveness)
	if summary.Valid {
		run.Summary = json.RawMessage(summary.String)
	}
	if liveness.Valid {
		run.Liveness = json.RawMessage(liveness.String)
	}
	return &run, err
}

func loadWeftChildren(q querier, run *WeftRun) error {
	rows, err := q.Query(`SELECT ws.run_id,ws.segment,ws.kind,ws.ledger_run_id,ws.worker_run_id,
		ws.answer_id,ws.terminal,ws.failure,ws.worker_contract,ws.agent_user,r.run_dir,r.claimed_at,r.started_at,r.ended_at
		FROM weft_segments ws JOIN runs r ON r.id=ws.ledger_run_id
		WHERE ws.run_id=? ORDER BY ws.segment`, run.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var seg WeftSegment
		if err := rows.Scan(&seg.RunID, &seg.Segment, &seg.Kind, &seg.LedgerRunID,
			&seg.WorkerRunID, &seg.AnswerID, &seg.Terminal, &seg.Failure, &seg.WorkerContract, &seg.AgentUser,
			&seg.RunDir, &seg.ClaimedAt, &seg.StartedAt, &seg.EndedAt); err != nil {
			rows.Close()
			return err
		}
		run.Segments = append(run.Segments, seg)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = q.Query(`SELECT `+weftStageColumns+` FROM weft_stages WHERE run_id=? ORDER BY rowid`, run.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		st, err := scanWeftStage(rows)
		if err != nil {
			rows.Close()
			return err
		}
		run.Stages = append(run.Stages, st)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	question, err := pendingWeftQuestion(q, run.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		question = nil
	}
	run.Question = question
	run.Steps, err = loadLandSteps(q, run.ID)
	return err
}

// weftStageColumns is the column list scanWeftStage reads.
const weftStageColumns = `run_id,segment,stage,occurrence,attempt,backend,state,session,
	detail,exit_code,turns,spend,processing_ms,tokens,at`

func scanWeftStage(sc scanner) (WeftStage, error) {
	var st WeftStage
	var spend, tokens sql.NullString
	if err := sc.Scan(&st.RunID, &st.Segment, &st.Stage, &st.Occurrence, &st.Attempt,
		&st.Backend, &st.State, &st.Session, &st.Detail, &st.ExitCode, &st.Turns,
		&spend, &st.Processing, &tokens, &st.At); err != nil {
		return st, err
	}
	if spend.Valid {
		st.Spend = json.RawMessage(spend.String)
	}
	if tokens.Valid {
		st.Tokens = json.RawMessage(tokens.String)
	}
	return st, nil
}

func (s *Store) PendingWeftQuestion(runID string) (*WeftQuestion, error) {
	return pendingWeftQuestion(s.db, runID)
}

func pendingWeftQuestion(q querier, runID string) (*WeftQuestion, error) {
	return scanQuestion(q.QueryRow(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.questions,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.run_id=? AND q.state='pending'`, runID))
}

func (s *Store) WeftQuestion(id string) (*WeftQuestion, error) {
	return weftQuestion(s.db, id)
}

func weftQuestion(db querier, id string) (*WeftQuestion, error) {
	q, err := scanQuestion(db.QueryRow(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.questions,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.id=?`, id))
	if err != nil {
		return nil, err
	}
	if q.AnswerID.Valid {
		q.Answer, err = weftAnswer(db, q.AnswerID.String)
	}
	return q, err
}

func (s *Store) PendingWeftQuestions() ([]*WeftQuestion, error) {
	rows, err := s.db.Query(`SELECT q.id,q.run_id,r.ticket,q.segment,q.stage,q.occurrence,
		q.attempt,q.text,q.questions,q.asked_at,q.state,q.answer_id FROM weft_questions q
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
		q.attempt,q.text,q.questions,q.asked_at,q.state,q.answer_id FROM weft_questions q
		JOIN weft_runs r ON r.id=q.run_id WHERE q.id=?`, id))
}

func scanQuestion(sc scanner) (*WeftQuestion, error) {
	var q WeftQuestion
	var questions sql.NullString
	err := sc.Scan(&q.ID, &q.RunID, &q.Ticket, &q.Segment, &q.Stage, &q.Occurrence,
		&q.Attempt, &q.Text, &questions, &q.AskedAt, &q.State, &q.AnswerID)
	if questions.Valid {
		q.Questions = json.RawMessage(questions.String)
	}
	return &q, err
}

func weftAnswer(q querier, id string) (*WeftAnswer, error) {
	var a WeftAnswer
	var answers sql.NullString
	err := q.QueryRow(`SELECT id,question_id,run_id,text,answers,submitted_at,segment,entry_id,consumed_segment
		FROM weft_answers WHERE id=?`, id).
		Scan(&a.ID, &a.QuestionID, &a.RunID, &a.Text, &answers, &a.SubmittedAt, &a.Segment, &a.EntryID, &a.ConsumedSegment)
	if answers.Valid {
		a.Answers = json.RawMessage(answers.String)
	}
	return &a, err
}

func (s *Store) WeftAnswer(id string) (*WeftAnswer, error) {
	return weftAnswer(s.db, id)
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

// WeftPruned counts the rows one retention prune removed.
type WeftPruned struct{ Events, Output, Requests int64 }

// PruneWeftRetention drops what is retained only for a window: state events
// older than events, the output of every run that ended before it
// (weft-service/v2 §8.1), and idempotency records older than requests (§4.2).
// Runs, segments, stages, questions and answers are authoritative and never
// pruned.
func (s *Store) PruneWeftRetention(events, requests time.Time) (WeftPruned, error) {
	var pruned WeftPruned
	cutoff := weftTimestamp(events)
	err := s.withTx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM weft_events WHERE ts<?`, cutoff)
		if err != nil {
			return err
		}
		if pruned.Events, err = res.RowsAffected(); err != nil {
			return err
		}
		res, err = tx.Exec(`DELETE FROM weft_output WHERE run_id IN
			(SELECT id FROM weft_runs WHERE phase=? AND ended_at<?)`, WeftPhaseEnded, cutoff)
		if err != nil {
			return err
		}
		if pruned.Output, err = res.RowsAffected(); err != nil {
			return err
		}
		res, err = tx.Exec(`DELETE FROM weft_requests WHERE created_at<?`, weftTimestamp(requests))
		if err != nil {
			return err
		}
		pruned.Requests, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return WeftPruned{}, err
	}
	return pruned, nil
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
	if err := tx.QueryRow(`SELECT COALESCE(MAX(segment),0) FROM weft_segments WHERE run_id=?`, runID).
		Scan(&segment); err != nil {
		return err
	}
	if segment == 0 && run.Origin == WeftOriginArchive {
		segment = WeftArchivedSegment
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
		data := questionAnswerData{
			AnswerID: a.ID, Answers: a.Answers, SubmittedAt: a.SubmittedAt,
			EntryID: nullable(a.EntryID), ConsumedSegment: nullableInt(a.ConsumedSegment),
		}
		if a.Answers == nil {
			data.Text = &a.Text
		}
		answer = data
	}
	var text *string
	if question.Questions == nil {
		text = &question.Text
	}
	return appendWeftEventTx(tx, question.RunID, "question", stamp, func(seq int64) any {
		return questionEventPayload{
			eventBase: newEventBase(seq, stamp, question.RunID),
			Question: questionEventData{
				QuestionID: question.ID, RunID: question.RunID, Ticket: question.Ticket,
				Segment: question.Segment, Stage: question.Stage, Occurrence: question.Occurrence,
				Attempt: question.Attempt, Text: text, Questions: question.Questions,
				AskedAt: question.AskedAt, State: question.State, Answer: answer,
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

// questionEventData is the question resource in either form (§6.5): text
// for a legacy park, questions for a structured one, never both.
type questionEventData struct {
	QuestionID string          `json:"question_id"`
	RunID      string          `json:"run_id"`
	Ticket     string          `json:"ticket"`
	Segment    int             `json:"segment"`
	Stage      string          `json:"stage"`
	Occurrence int             `json:"occurrence"`
	Attempt    int             `json:"attempt"`
	Text       *string         `json:"text,omitempty"`
	Questions  json.RawMessage `json:"questions,omitempty"`
	AskedAt    string          `json:"asked_at"`
	State      string          `json:"state"`
	Answer     any             `json:"answer"`
}

type questionAnswerData struct {
	AnswerID        string          `json:"answer_id"`
	Text            *string         `json:"text,omitempty"`
	Answers         json.RawMessage `json:"answers,omitempty"`
	SubmittedAt     string          `json:"submitted_at"`
	EntryID         any             `json:"entry_id"`
	ConsumedSegment any             `json:"consumed_segment"`
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

// NewRemoteID mints a weft-service/v2 id of the given prefix (§4.1):
// <prefix>-<stamp>-<8 hex>.
func NewRemoteID(prefix string, now time.Time) (string, error) { return newRemoteID(prefix, now) }

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
