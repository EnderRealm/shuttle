package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"
)

// Landing settlement (weft-service/v2 §7). The host never takes a landing
// step: it records what the worker intends, acknowledges it once the intent
// is durable, and records what the worker reports.
const (
	// WeftPhaseSettling is a run whose finished grade is landing, or whose
	// landing a reconcile is establishing.
	WeftPhaseSettling = "settling"

	WeftSettlementPending     = "pending"
	WeftSettlementInProgress  = "in_progress"
	WeftSettlementLanded      = "landed"
	WeftSettlementNotLanded   = "not_landed"
	WeftSettlementReconciling = "reconciling"

	WeftLandRefused      = "refused"
	WeftLandFailed       = "failed"
	WeftLandStopped      = "stopped"
	WeftLandUnreconciled = "unreconciled"

	WeftStepFastForward = "fast_forward"
	WeftStepPush        = "push"
	WeftStepMarkDone    = "mark_done"

	WeftActionLandingUnsettled = "landing_unsettled"

	// The segments that launch no model and hold no slot (§6.1, §11.1).
	WeftSegmentReconcile = "reconcile"
	WeftSegmentCleanup   = "cleanup"
	WeftSegmentArchive   = "archive"

	// weftPipelineKinds are the segments that run the pipeline.
	weftPipelineKinds = `('launch','answer','resume')`
)

// WeftLandSteps are the landing's external steps, in the only order they may
// be taken (§7.2).
var WeftLandSteps = []string{WeftStepFastForward, WeftStepPush, WeftStepMarkDone}

// WeftLandStep is one external step as the host recorded it: its intent, the
// acknowledgement, the worker's result and what a reconcile observed.
type WeftLandStep struct {
	Step, IntentAt                              string
	AckedAt, Result, ResultAt, Observed, Detail sql.NullString
	// ResultSegment and ObservedSegment are the segments that reported the
	// result and made the observation.
	ResultSegment, ObservedSegment sql.NullInt64
}

// Done is what the record says of the step's effect: a reconcile's
// observation supersedes a result an earlier segment reported, and a result
// the observing reconcile then reported supersedes its observation. It is
// history — what a landing is acknowledged on is landEvidenceTx.
func (s *WeftLandStep) Done() bool {
	if s == nil {
		return false
	}
	if s.Observed.Valid && (!s.Result.Valid || s.ObservedSegment.Int64 > s.ResultSegment.Int64) {
		return s.Observed.String == "done"
	}
	return s.Result.String == "done" || s.Observed.String == "done"
}

// WeftLanding is a landing as the worker reports it on ended or reconciled.
type WeftLanding struct {
	State, Step, Reason, Detail, Branch, Commit string
}

func loadLandSteps(q querier, runID string) ([]WeftLandStep, error) {
	rows, err := q.Query(`SELECT step,intent_at,acked_at,result,result_at,observed,detail,result_segment,observed_segment
		FROM weft_land_steps
		WHERE run_id=? ORDER BY CASE step WHEN 'fast_forward' THEN 1 WHEN 'push' THEN 2 ELSE 3 END`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WeftLandStep
	for rows.Next() {
		var st WeftLandStep
		if err := rows.Scan(&st.Step, &st.IntentAt, &st.AckedAt, &st.Result, &st.ResultAt, &st.Observed, &st.Detail,
			&st.ResultSegment, &st.ObservedSegment); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func stepOf(steps []WeftLandStep, step string) *WeftLandStep {
	for i := range steps {
		if steps[i].Step == step {
			return &steps[i]
		}
	}
	return nil
}

// firstUndone is the first step whose effect the host has no record of.
func firstUndone(steps []WeftLandStep) string {
	for _, step := range WeftLandSteps {
		if !stepOf(steps, step).Done() {
			return step
		}
	}
	return WeftStepMarkDone
}

// Settlement is the run's landing as every read and settlement event
// carries it (§7.1).
func (r *WeftRun) Settlement() map[string]any {
	steps := make([]map[string]any, 0, len(r.Steps))
	for _, st := range r.Steps {
		steps = append(steps, map[string]any{
			"step": st.Step, "intent_at": st.IntentAt, "acked_at": nullable(st.AckedAt),
			"result": nullable(st.Result), "result_at": nullable(st.ResultAt), "observed": nullable(st.Observed),
		})
	}
	return map[string]any{
		"state": r.SettlementState, "step": nullable(r.SettlementStep), "reason": nullable(r.SettlementReason),
		"detail": nullable(r.SettlementDetail), "branch": nullable(r.SettlementBranch),
		"commit": nullable(r.SettlementCommit), "steps": steps,
	}
}

type settlementEventPayload struct {
	eventBase
	Settlement map[string]any `json:"settlement"`
}

func appendSettlementEventTx(tx *sql.Tx, runID, stamp string) error {
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return err
	}
	if run.Steps, err = loadLandSteps(tx, runID); err != nil {
		return err
	}
	return appendWeftEventTx(tx, runID, "settlement", stamp, func(seq int64) any {
		return settlementEventPayload{eventBase: newEventBase(seq, stamp, runID), Settlement: run.Settlement()}
	})
}

// segmentState is what settling a reaped segment depends on.
type segmentState struct {
	segment                    int
	ledgerID, kind             string
	land, observed             bool
	workerRunID, terminal, dir sql.NullString
	observedOutcome            sql.NullString
	// observedSteps is a reconcile's reading of each step, in WeftLandSteps'
	// order.
	observedSteps [3]sql.NullString
}

func segmentStateTx(tx *sql.Tx, runID string, segment int) (*segmentState, error) {
	seg := &segmentState{segment: segment}
	err := tx.QueryRow(`SELECT s.ledger_run_id,s.kind,s.land,s.worker_run_id,s.terminal,r.run_dir,s.observed,s.observed_outcome,
		s.observed_fast_forward,s.observed_push,s.observed_mark_done
		FROM weft_segments s JOIN runs r ON r.id=s.ledger_run_id WHERE s.run_id=? AND s.segment=?`, runID, segment).
		Scan(&seg.ledgerID, &seg.kind, &seg.land, &seg.workerRunID, &seg.terminal, &seg.dir, &seg.observed, &seg.observedOutcome,
			&seg.observedSteps[0], &seg.observedSteps[1], &seg.observedSteps[2])
	return seg, err
}

// RecordWeftGraded records a landing segment's grade (§11.3). A finished
// grade settles: the run waits on its landing, which only the worker's steps
// move — and none at all when the operator stopped the run before it was
// graded. Any other grade lands nothing and the run is over.
func (s *Store) RecordWeftGraded(runID string, segment int, outcome string, stopped bool, haltStage, haltReason, haltDetail, detail, head string, summary json.RawMessage) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET graded=1 WHERE run_id=? AND segment=?`, runID, segment); err != nil {
			return err
		}
		phase, ended := WeftPhaseSettling, sql.NullString{}
		if outcome != "finished" {
			phase, ended = WeftPhaseEnded, nullString(stamp)
		}
		// A grade never clears an operator's stop: one accepted before it,
		// while the pipeline ran, lasts the attempt, and so stops the landing
		// a finished grade would start.
		if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,outcome=?,halt_stage=?,halt_reason=?,halt_detail=?,
			head=?,stopped=MAX(stopped,?),summary=?,updated_at=?,ended_at=?,revision=revision+1 WHERE id=?`,
			phase, outcome, nullString(haltStage), nullString(haltReason), nullString(firstNonempty(haltDetail, detail)),
			nullString(head), stopped, string(summary), stamp, ended, runID); err != nil {
			return err
		}
		if outcome != "finished" {
			if _, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_step=NULL,settlement_reason=NULL,
				settlement_detail=?,settlement_branch=NULL,settlement_commit=NULL WHERE id=?`, WeftSettlementNone,
				fmt.Sprintf("the run was graded %s; only a finished run lands", outcome), runID); err != nil {
				return err
			}
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// landEvidenceTx is what one segment itself established about each step:
// a result of done it reported, or — a reconcile — an observation of done it
// made. Another segment's record is history and never evidence: a reconcile's
// not_done or unknown leaves a step not done whatever was reported before.
func landEvidenceTx(tx *sql.Tx, runID string, segment int) (map[string]bool, error) {
	rows, err := tx.Query(`SELECT step,(result='done' AND result_segment=?2) OR (observed='done' AND observed_segment=?2)
		FROM weft_land_steps WHERE run_id=?1`, runID, segment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	done := map[string]bool{}
	for rows.Next() {
		var step string
		var yes sql.NullBool
		if err := rows.Scan(&step, &yes); err != nil {
			return nil, err
		}
		done[step] = yes.Bool
	}
	return done, rows.Err()
}

// RecordWeftLandIntent makes a step's intent durable before the host
// acknowledges it (§7.2), and is the one place that decides whether it is
// acknowledged. Every step is of the head the host recorded graded finished,
// on the run's base: a run with no such head — a reconcile of a landing lost
// before the host recorded its grade — takes no step. A landing segment's
// step follows its run's finished grade; a reconcile's follows its own
// observation of a finished grade. Either way every step before it must be
// done on the segment's own evidence (landEvidenceTx), and a step that
// evidence shows done is never intended again; an intent that breaks this is
// refused, and the segment fails. An intent that keeps it is acknowledged
// unless an operator stopped the landing (§6.6); acked is what was recorded.
func (s *Store) RecordWeftLandIntent(runID string, segment int, step, branch, commit, base, intentAt string) (acked bool, err error) {
	err = s.withTx(func(tx *sql.Tx) error {
		run, err := getWeftRunTx(tx, runID)
		if err != nil {
			return err
		}
		seg, err := segmentStateTx(tx, runID, segment)
		if err != nil {
			return err
		}
		switch {
		case run.Outcome.String != "finished" || run.Head.String == "" || run.Base.String == "":
			return fmt.Errorf("land_intent for a run with no finished grade recorded with its head and base")
		case commit != run.Head.String || base != run.Base.String:
			return fmt.Errorf("land_intent names a commit or base other than the recorded graded head on the run's base")
		case !slices.Contains([]string{WeftSettlementPending, WeftSettlementInProgress, WeftSettlementReconciling}, run.SettlementState):
			return fmt.Errorf("land_intent while the run's settlement is %s", run.SettlementState)
		case seg.kind == WeftSegmentReconcile && !seg.observed:
			return fmt.Errorf("land_intent before the reconcile observed what was done")
		case seg.kind == WeftSegmentReconcile && seg.observedOutcome.String != "finished":
			return fmt.Errorf("land_intent from a reconcile that observed no finished grade")
		case seg.kind != WeftSegmentReconcile && (!seg.land || run.Outcome.String != "finished"):
			return fmt.Errorf("land_intent before a finished grade")
		}
		done, err := landEvidenceTx(tx, runID, segment)
		if err != nil {
			return err
		}
		i := slices.Index(WeftLandSteps, step)
		if i < 0 {
			return fmt.Errorf("land_intent names no landing step")
		}
		for _, earlier := range WeftLandSteps[:i] {
			if !done[earlier] {
				return fmt.Errorf("land_intent %s before %s was done", step, earlier)
			}
		}
		switch {
		case done[step]:
			return fmt.Errorf("land_intent %s, which is already done", step)
		case seg.kind == WeftSegmentReconcile && seg.observedSteps[i].String != "not_done":
			// A reconcile takes only a step whose effect it observed absent:
			// one it could not observe is unreconciled, never retaken.
			return fmt.Errorf("land_intent %s, which the reconcile observed %s, not not_done", step,
				firstNonempty(seg.observedSteps[i].String, "nothing of"))
		}
		acked = !run.Stopped
		stamp := weftTimestamp(s.now())
		ackedAt := sql.NullString{}
		if acked {
			ackedAt = nullString(stamp)
		}
		if _, err := tx.Exec(`INSERT INTO weft_land_steps(run_id,step,intent_at,acked_at) VALUES(?,?,?,?)
			ON CONFLICT(run_id,step) DO UPDATE SET intent_at=excluded.intent_at,acked_at=excluded.acked_at,
			result=NULL,result_at=NULL,result_segment=NULL,detail=NULL`, runID, step, intentAt, ackedAt); err != nil {
			return err
		}
		state := WeftSettlementInProgress
		if run.SettlementState == WeftSettlementReconciling {
			state = WeftSettlementReconciling
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_step=?,settlement_reason=NULL,
			settlement_branch=?,settlement_commit=?,updated_at=?,revision=revision+1 WHERE id=?`,
			state, step, branch, commit, stamp, runID); err != nil {
			return err
		}
		return appendSettlementEventTx(tx, runID, stamp)
	})
	return acked, err
}

// RecordWeftLandResult records the worker's result for the step it intended.
func (s *Store) RecordWeftLandResult(runID string, segment int, step, result, detail, resultAt string) error {
	return s.withTx(func(tx *sql.Tx) error {
		var prior sql.NullString
		err := tx.QueryRow(`SELECT result FROM weft_land_steps WHERE run_id=? AND step=?`, runID, step).Scan(&prior)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("land_step_result for %s, which was never intended", step)
		case err != nil:
			return err
		case prior.Valid:
			return fmt.Errorf("land_step_result for %s, which already has its result", step)
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_land_steps SET result=?,result_at=?,result_segment=?,detail=? WHERE run_id=? AND step=?`,
			result, resultAt, segment, nullString(detail), runID, step); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET updated_at=?,revision=revision+1 WHERE id=?`, stamp, runID); err != nil {
			return err
		}
		return appendSettlementEventTx(tx, runID, stamp)
	})
}

// checkLandingTx holds a landing the segment reports to the host's record:
// a landing names every step done on the segment's own evidence, one that
// did not land names its step and reason, and nothing intended reads as not
// applicable — never after a finished grade, which ends only landed or not
// landed (rows 11n, 21n).
func checkLandingTx(tx *sql.Tx, runID string, segment int, l WeftLanding) error {
	steps, err := loadLandSteps(tx, runID)
	if err != nil {
		return err
	}
	var outcome, head sql.NullString
	if err := tx.QueryRow(`SELECT outcome,head FROM weft_runs WHERE id=?`, runID).Scan(&outcome, &head); err != nil {
		return err
	}
	if outcome.String == "finished" && head.Valid && l.Commit != "" && l.Commit != head.String {
		return fmt.Errorf("landing names a commit other than the reviewed head the host recorded")
	}
	done, err := landEvidenceTx(tx, runID, segment)
	if err != nil {
		return err
	}
	switch l.State {
	case WeftSettlementLanded:
		for _, step := range WeftLandSteps {
			if !done[step] {
				return fmt.Errorf("landing reported landed with %s not done", step)
			}
		}
		if l.Branch == "" || l.Commit == "" || l.Step != "" || l.Reason != "" {
			return fmt.Errorf("a landed landing names its branch and commit and no step or reason")
		}
	case WeftSettlementNotLanded:
		if !slices.Contains(WeftLandSteps, l.Step) ||
			!slices.Contains([]string{WeftLandRefused, WeftLandFailed, WeftLandStopped, WeftLandUnreconciled}, l.Reason) {
			return fmt.Errorf("a landing that did not land names its step and reason")
		}
	case WeftSettlementNone:
		if outcome.String == "finished" {
			return fmt.Errorf("landing reported not_applicable after a finished grade")
		}
		if len(steps) > 0 || l.Step != "" || l.Reason != "" {
			return fmt.Errorf("landing reported not_applicable after a landing step was recorded")
		}
	default:
		return fmt.Errorf("landing state is not landed, not_landed or not_applicable")
	}
	return nil
}

func settlementOf(l WeftLanding) (state string, step, reason, detail, branch, commit sql.NullString) {
	detail = nullString(l.Detail)
	if l.State == WeftSettlementNone && !detail.Valid {
		detail = nullString("the worker landed nothing")
	}
	return l.State, nullString(l.Step), nullString(l.Reason), detail, nullString(l.Branch), nullString(l.Commit)
}

// RecordWeftLanding records a landing segment's terminal event (§7.3): the
// landing as the worker saw it becomes the run's settlement, and a settling
// run is over. What it frees waits for reap.
func (s *Store) RecordWeftLanding(runID string, segment int, l WeftLanding) error {
	return s.withTx(func(tx *sql.Tx) error {
		prior, err := getWeftRunTx(tx, runID)
		if err != nil {
			return err
		}
		// Only a finished grade lands, so only its settlement can end landed
		// or not landed: a report of either on any other is the worker's
		// word against the host's record.
		if l.State != WeftSettlementNone && prior.SettlementState != WeftSettlementPending &&
			prior.SettlementState != WeftSettlementInProgress {
			return fmt.Errorf("ended reports a landing %s, but the run's settlement is %s", l.State, prior.SettlementState)
		}
		if err := checkLandingTx(tx, runID, segment, l); err != nil {
			return err
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='ended' WHERE run_id=? AND segment=?`,
			runID, segment); err != nil {
			return err
		}
		run, err := getWeftRunTx(tx, runID)
		if err != nil {
			return err
		}
		if l.State == WeftSettlementNone && run.SettlementState == WeftSettlementNone {
			// The grade already said why nothing lands.
			return nil
		}
		state, step, reason, detail, branch, commit := settlementOf(l)
		if _, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_step=?,settlement_reason=?,
			settlement_detail=?,settlement_branch=?,settlement_commit=?,
			phase=CASE WHEN phase=? THEN ? ELSE phase END,ended_at=COALESCE(ended_at,?),updated_at=?,
			revision=revision+1 WHERE id=?`, state, step, reason, detail, branch, commit,
			WeftPhaseSettling, WeftPhaseEnded, stamp, stamp, runID); err != nil {
			return err
		}
		if err := reviewedCommitTx(tx, runID); err != nil {
			return err
		}
		if err := appendSettlementEventTx(tx, runID, stamp); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// WeftObserved is a reconcile's reading of each step (§7.4).
type WeftObserved struct {
	FastForward, Push, MarkDone string
}

// RecordWeftObserved records a reconcile's reading — the outcome it read,
// "" for none — on its segment, and, when it read a finished grade, what it
// observed against each step the host recorded. Only a finished run lands,
// so only its observation becomes a land-step row (I-rows): a step observed
// done that the host never recorded an intent for is recorded at the
// observation, so the settlement shows every step whose effect exists. A
// reading of any other outcome for a run the host recorded finished is
// refused: the recorded grade stands against it (row 15g).
func (s *Store) RecordWeftObserved(runID string, segment int, outcome string, o WeftObserved, observedAt string) error {
	return s.withTx(func(tx *sql.Tx) error {
		if err := refuseUnfinishedReadingTx(tx, runID, outcome); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_segments SET observed=1,observed_outcome=?,observed_fast_forward=?,observed_push=?,
			observed_mark_done=? WHERE run_id=? AND segment=?`,
			nullString(outcome), o.FastForward, o.Push, o.MarkDone, runID, segment); err != nil {
			return err
		}
		if outcome == "finished" {
			for i, value := range []string{o.FastForward, o.Push, o.MarkDone} {
				res, err := tx.Exec(`UPDATE weft_land_steps SET observed=?,observed_segment=? WHERE run_id=? AND step=?`,
					value, segment, runID, WeftLandSteps[i])
				if err != nil {
					return err
				}
				if n, _ := res.RowsAffected(); n == 0 && value == "done" {
					if _, err := tx.Exec(`INSERT INTO weft_land_steps(run_id,step,intent_at,observed,observed_segment) VALUES(?,?,?,?,?)`,
						runID, WeftLandSteps[i], observedAt, value, segment); err != nil {
						return err
					}
				}
			}
		}
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_runs SET updated_at=?,revision=revision+1 WHERE id=?`, stamp, runID); err != nil {
			return err
		}
		return appendSettlementEventTx(tx, runID, stamp)
	})
}

// RecordWeftReconciled records a reconcile's terminal event (§7.4). outcome
// is what the reconcile read from runs.jsonl: none means the run was never
// graded and is closed as v1's interrupted segment, unknown and resumable;
// one not finished is the run's outcome and lands nothing; finished takes the
// landing the reconcile reports. A run the host recorded finished is never
// closed on another reading, and no reading closes a run as landing nothing
// over land-step rows it has (I-rows).
func (s *Store) RecordWeftReconciled(runID string, segment int, outcome string, l WeftLanding) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if err := refuseUnfinishedReadingTx(tx, runID, outcome); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='reconciled' WHERE run_id=? AND segment=?`, runID, segment); err != nil {
			return err
		}
		if outcome != "finished" && l.State != WeftSettlementNone {
			return fmt.Errorf("reconciled reports a landing for a run the reconcile found not finished")
		}
		switch outcome {
		case "":
			if err := closeUngradedTx(tx, runID, "the landing segment ended before it was graded: reconcile found no ended line"); err != nil {
				return err
			}
			l = WeftLanding{State: WeftSettlementNone, Detail: "the run was never graded, so nothing lands"}
		case "finished":
		default:
			l = WeftLanding{State: WeftSettlementNone, Detail: fmt.Sprintf("the run was graded %s; only a finished run lands", outcome)}
		}
		// The outcome is written first so the check holds the landing to
		// the grade the run now reads, the reconcile's reading included.
		if outcome != "" {
			if err := adoptReadingTx(tx, runID, outcome); err != nil {
				return err
			}
		}
		if err := checkLandingTx(tx, runID, segment, l); err != nil {
			return err
		}
		return endSettlementTx(tx, runID, l, stamp)
	})
}

// endSettlementTx ends the run with the landing as its settlement.
func endSettlementTx(tx *sql.Tx, runID string, l WeftLanding, stamp string) error {
	state, step, reason, detail, branch, commit := settlementOf(l)
	if _, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_step=?,settlement_reason=?,
		settlement_detail=?,settlement_branch=?,settlement_commit=?,phase=?,ended_at=COALESCE(ended_at,?),
		updated_at=?,revision=revision+1 WHERE id=?`, state, step, reason, detail, branch, commit,
		WeftPhaseEnded, stamp, stamp, runID); err != nil {
		return err
	}
	if err := reviewedCommitTx(tx, runID); err != nil {
		return err
	}
	if err := appendSettlementEventTx(tx, runID, stamp); err != nil {
		return err
	}
	return appendRunEventTx(tx, runID, stamp)
}

// reviewedCommitTx holds a not_landed settlement's commit to the host's
// record, never the worker's report: the head the host recorded graded
// finished, or none for a run it recorded no finished grade with a head for
// (T2u).
func reviewedCommitTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE weft_runs SET settlement_commit=CASE WHEN outcome='finished' THEN head END
		WHERE id=? AND settlement_state=?`, runID, WeftSettlementNotLanded)
	return err
}

// adoptReadingTx records a reconcile's reading of the run's grade as the
// run's outcome and as the lost pipeline segment's grade (rows 19, 22u), so a
// failed one is never resumable (§4).
func adoptReadingTx(tx *sql.Tx, runID, outcome string) error {
	if _, err := tx.Exec(`UPDATE weft_runs SET outcome=? WHERE id=?`, outcome, runID); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE weft_segments SET graded=1 WHERE run_id=? AND segment=(SELECT MAX(segment)
		FROM weft_segments WHERE run_id=? AND kind IN `+weftPipelineKinds+`)`, runID, runID)
	return err
}

// refuseUnfinishedReadingTx refuses a reconcile's reading of an outcome other
// than finished — none included — for a run the host recorded finished: the
// worker's reading never overrides the recorded grade (row 15g).
func refuseUnfinishedReadingTx(tx *sql.Tx, runID, outcome string) error {
	var recorded sql.NullString
	if err := tx.QueryRow(`SELECT outcome FROM weft_runs WHERE id=?`, runID).Scan(&recorded); err != nil {
		return err
	}
	if recorded.String == "finished" && outcome != "finished" {
		return fmt.Errorf("reconcile read the run as other than the finished grade the host recorded")
	}
	return nil
}

// settleReapedTx frees what a reaped segment held once its terminal event is
// recorded (§6.1, §6.2): a pipeline segment its run's reservation and slot, a
// reconcile or cleanup segment its settlement or maintenance row. A landing
// that did not land pauses its repository here, after reap.
func settleReapedTx(tx *sql.Tx, runID string, seg *segmentState, stamp string) error {
	switch seg.kind {
	case WeftSegmentReconcile:
		if err := settleUnresolvedTx(tx, runID, stamp); err != nil {
			return err
		}
		return releaseMaintenanceTx(tx, runID, stamp)
	case WeftSegmentCleanup:
		return releaseMaintenanceTx(tx, runID, stamp)
	}
	if seg.land {
		if err := settleUnresolvedTx(tx, runID, stamp); err != nil {
			return err
		}
	}
	if err := releaseRunTx(tx, runID, stamp); err != nil {
		return err
	}
	return capacityStepTx(tx, stamp)
}

// settleLostMaintenanceTx settles a reconcile or cleanup segment the host
// lost: one gone without its terminal event is unreconciled, or its cleanup
// refused, and either way its row is released.
func settleLostMaintenanceTx(tx *sql.Tx, runID string, seg *segmentState, detail, stamp string) error {
	if !seg.terminal.Valid {
		switch seg.kind {
		case WeftSegmentReconcile:
			if err := lostReconcileTx(tx, runID, seg.observedOutcome, detail, stamp); err != nil {
				return err
			}
		case WeftSegmentCleanup:
			if err := refuseCleanupTx(tx, runID, detail, stamp); err != nil {
				return err
			}
		}
	}
	return settleReapedTx(tx, runID, seg, stamp)
}

// settleUnresolvedTx is §7.5 at reap: a landing that did not land holds the
// run for the operator and pauses its repository — that repository only; a
// landing that a reconcile completed releases the run from that hold, and the
// pause stands until continue.
func settleUnresolvedTx(tx *sql.Tx, runID, stamp string) error {
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return err
	}
	switch {
	case run.SettlementState == WeftSettlementNotLanded:
		if _, err := tx.Exec(`UPDATE weft_runs SET action_required=?,action_detail=?,updated_at=?,revision=revision+1
			WHERE id=?`, WeftActionLandingUnsettled,
			fmt.Sprintf("dispatch to %s is paused until this landing is resolved and dispatch is continued", run.Repo),
			stamp, runID); err != nil {
			return err
		}
		if err := appendRunEventTx(tx, runID, stamp); err != nil {
			return err
		}
		return pauseRepositoryTx(tx, run, stamp)
	case run.ActionRequired.String == WeftActionLandingUnsettled:
		if _, err := tx.Exec(`UPDATE weft_runs SET action_required=NULL,action_detail=NULL,updated_at=?,revision=revision+1
			WHERE id=?`, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	}
	return nil
}

// releaseMaintenanceTx releases the settlement row a reconcile ran under, or
// the maintenance row a reconcile or cleanup entry of the run took.
func releaseMaintenanceTx(tx *sql.Tx, runID, stamp string) error {
	return releaseRepositoriesTx(tx, stamp, `(holder_kind=? AND holder_id=?) OR (holder_kind=? AND holder_id IN
		(SELECT id FROM weft_entries WHERE run_id=? AND kind IN (?,?)))`,
		WeftHolderSettlement, runID, WeftHolderMaintenance, runID, WeftEntryReconcile, WeftEntryCleanup)
}

// loseLandingTx is §7.4 for a landing segment gone without its terminal
// event: never replayed, never announced. The run goes reconciling, its
// reservation row passes in place to a settlement holder, and a reconcile
// entry is admitted and dispatched under that row in this transaction — it
// waits on nothing, because the row it needs is its run's. Its segment runs
// in a directory beside the lost one's. A run that has claimed all its
// segments can claim no reconcile: its landing is held unreconciled at once,
// as a lost reconcile's is, and paused here at reap. It answers whether a
// reconcile was claimed.
func loseLandingTx(tx *sql.Tx, runID string, seg *segmentState, now time.Time) (bool, error) {
	stamp := weftTimestamp(now)
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return false, err
	}
	next, err := nextSegmentTx(tx, runID)
	if err != nil {
		return false, err
	}
	if next > WeftSegmentLimit {
		detail := fmt.Sprintf("segment %d ended without its terminal event, and run %s has claimed all %d segments, "+
			"so no reconcile can read what it did", seg.segment, runID, WeftSegmentLimit)
		if err := lostReconcileTx(tx, runID, sql.NullString{}, detail, stamp); err != nil {
			return false, err
		}
		return false, settleReapedTx(tx, runID, seg, stamp)
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,settlement_state=?,settlement_reason=NULL,settlement_detail=?,
		updated_at=?,revision=revision+1 WHERE id=?`, WeftPhaseSettling, WeftSettlementReconciling,
		fmt.Sprintf("segment %d ended without its terminal event; a reconcile is reading what it did", seg.segment),
		stamp, runID); err != nil {
		return false, err
	}
	res, err := tx.Exec(`UPDATE weft_reservations SET holder_kind=? WHERE holder_kind=? AND holder_id=?`,
		WeftHolderSettlement, WeftHolderRun, runID)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		err = repositoryStepTx(tx, run.RepositoryID, run.Repo, stamp)
	} else {
		err = acquireRepository(tx, run.RepositoryID, run.Repo, WeftHolderSettlement, runID, stamp)
	}
	if err != nil {
		return false, err
	}
	entryID, err := insertEntryTx(tx, WeftAdmission{
		Kind: WeftEntryReconcile, Ticket: run.Ticket, Project: run.Project, RepositoryID: run.RepositoryID,
		Repository: run.Repo, RunID: runID,
	}, WeftEntryDispatched, now)
	if err != nil {
		return false, err
	}
	if _, err := claimMaintenanceSegmentTx(tx, runID, WeftSegmentReconcile, filepath.Dir(seg.dir.String), now); err != nil {
		return false, err
	}
	if err := queueStepTx(tx, stamp, true, entryID); err != nil {
		return false, err
	}
	if err := appendSettlementEventTx(tx, runID, stamp); err != nil {
		return false, err
	}
	return true, appendRunEventTx(tx, runID, stamp)
}

// landingOpenTx is whether the run's landing is still to be settled (A, A′,
// B): only then is a landing segment lost without its terminal event
// reconciled. A grade other than finished already closed it (T3).
func landingOpenTx(tx *sql.Tx, runID string) (bool, error) {
	var state string
	if err := tx.QueryRow(`SELECT settlement_state FROM weft_runs WHERE id=?`, runID).Scan(&state); err != nil {
		return false, err
	}
	return state == WeftSettlementPending || state == WeftSettlementInProgress, nil
}

// closeUngradedTx gives a run whose landing segment was lost before the host
// recorded its grade the outcome of the segment as it was lost: an
// operator's stop stands, and the class the host recorded on the lost
// pipeline segment is kept — unknown only where none was, as after a restart.
func closeUngradedTx(tx *sql.Tx, runID, haltDetail string) error {
	if _, err := tx.Exec(`UPDATE weft_runs SET outcome=CASE WHEN stopped THEN 'stopped' ELSE 'failed' END,
		summary=CASE WHEN stopped THEN ? ELSE summary END,halt_detail=? WHERE id=?`, weftStoppedSummary,
		haltDetail, runID); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE weft_segments SET failure=COALESCE(failure,?) WHERE run_id=? AND segment=(SELECT MAX(segment)
		FROM weft_segments WHERE run_id=? AND kind IN `+weftPipelineKinds+`)`, ClassUnknown, runID, runID)
	return err
}

// lostReconcileTx settles a reconcile gone without reconciled (rows 22,
// 22u). A grade it read before it was lost is adopted as reconciled adopts
// it: one other than finished lands nothing, and the run settles as row 19
// does (T3). Otherwise the landing is unreconciled at the first step the host
// has no effect recorded for. A run the host recorded no grade for and the
// reconcile read none of is closed as its landing segment was lost (T2u):
// whether that segment graded is unknown, so the run is held unreconciled,
// not resumable, until a retry reconciles it.
func lostReconcileTx(tx *sql.Tx, runID string, observed sql.NullString, detail, stamp string) error {
	run, err := getWeftRunTx(tx, runID)
	if err != nil {
		return err
	}
	switch {
	case observed.Valid && observed.String != "finished":
		if err := adoptReadingTx(tx, runID, observed.String); err != nil {
			return err
		}
		return endSettlementTx(tx, runID, WeftLanding{State: WeftSettlementNone,
			Detail: fmt.Sprintf("the run was graded %s; only a finished run lands", observed.String)}, stamp)
	case observed.Valid:
		if err := adoptReadingTx(tx, runID, observed.String); err != nil {
			return err
		}
	case !run.Outcome.Valid:
		if err := closeUngradedTx(tx, runID, "the landing segment ended before it was graded, and its reconcile ended without reconciling it"); err != nil {
			return err
		}
	}
	steps, err := loadLandSteps(tx, runID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET phase=?,settlement_state=?,settlement_step=?,settlement_reason=?,
		settlement_detail=?,ended_at=COALESCE(ended_at,?),updated_at=?,revision=revision+1 WHERE id=?`,
		WeftPhaseEnded, WeftSettlementNotLanded, firstUndone(steps), WeftLandUnreconciled, detail, stamp, stamp, runID); err != nil {
		return err
	}
	if err := reviewedCommitTx(tx, runID); err != nil {
		return err
	}
	if err := appendSettlementEventTx(tx, runID, stamp); err != nil {
		return err
	}
	return appendRunEventTx(tx, runID, stamp)
}

// weftUngradedDetail is why a run that ended before it was graded lands
// nothing.
const weftUngradedDetail = "the run ended before it was graded, so nothing lands"

// settleUngradedTx is the settlement of a run that ends without a grade:
// one still pending its landing will not land. It reports whether it
// changed the run, whose run event the caller writes.
func settleUngradedTx(tx *sql.Tx, runID string) (bool, error) {
	res, err := tx.Exec(`UPDATE weft_runs SET settlement_state=?,settlement_step=NULL,settlement_reason=NULL,
		settlement_detail=?,settlement_branch=NULL,settlement_commit=NULL WHERE id=? AND settlement_state=?`,
		WeftSettlementNone, weftUngradedDetail, runID, WeftSettlementPending)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// claimMaintenanceSegmentTx claims the run's next segment of a kind that
// launches no model, with its phase-1 ledger row, within the run's segment
// limit: ErrWeftSegmentLimit claims nothing. The run's phase and grade are
// its own and are left as they are.
func claimMaintenanceSegmentTx(tx *sql.Tx, runID, kind, runDirRoot string, now time.Time) (int, error) {
	segment, err := nextSegmentTx(tx, runID)
	if err != nil {
		return 0, err
	}
	if segment > WeftSegmentLimit {
		return 0, ErrWeftSegmentLimit
	}
	_, err = insertWeftSegmentTx(tx, runID, segment, kind, "", runDirRoot, now)
	return segment, err
}
