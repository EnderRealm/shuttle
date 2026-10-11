package weftremote

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"shuttle/internal/ledger"
)

// runsPage is §4.3's history page size.
const runsPage = 100

// actionOptions are the remedies a run's action_required offers (§6.7, §7.5).
var actionOptions = map[string][]string{
	"stale_base":        {"fresh", "terminal", "dismiss"},
	"landing_unsettled": {"reconcile", "terminal", "continue"},
	"branch_exists":     {"fresh"},
}

// v2RunRef is the run a catalog entry shows.
func v2RunRef(run *ledger.WeftRun) map[string]any {
	return map[string]any{
		"run_id": run.ID, "state": runState(run), "phase": ledger.WeftV2Phase(run.Phase),
		"outcome": nullString(run.Outcome), "action_required": nullString(run.ActionRequired),
		"landing": run.SettlementState,
	}
}

// v2RunHeader is one history row (runs.json).
func v2RunHeader(run *ledger.WeftRun) map[string]any {
	segment, questionID := 0, any(nil)
	if len(run.Segments) > 0 {
		segment = run.Segments[len(run.Segments)-1].Segment
	}
	if run.Question != nil {
		questionID = run.Question.ID
	}
	return map[string]any{
		"run_id": run.ID, "ticket": run.Ticket, "project": run.Project,
		"repository_id": run.RepositoryID, "origin": run.Origin, "recorded_under": run.RecordedUnder,
		"archive_id": nullString(run.ArchiveID), "state": runState(run),
		"phase": ledger.WeftV2Phase(run.Phase), "outcome": nullString(run.Outcome),
		"failed": runFailed(run), "stopped": run.Stopped,
		"action_required": nullString(run.ActionRequired), "landing": run.SettlementState,
		"disposition": run.Disposition, "segment": segment, "question_id": questionID,
		"created_at": run.CreatedAt, "updated_at": run.UpdatedAt, "ended_at": nullString(run.EndedAt),
	}
}

// v2Settlement is the run's landing as the host records it (§7.1). The
// intent steps are the landing capability's, which this build does not serve.
func v2Settlement(run *ledger.WeftRun) map[string]any {
	return map[string]any{
		"state": run.SettlementState, "step": nullString(run.SettlementStep),
		"reason": nullString(run.SettlementReason), "detail": nullString(run.SettlementDetail),
		"branch": nullString(run.SettlementBranch), "commit": nullString(run.SettlementCommit),
		"steps": []any{},
	}
}

func v2Halt(run *ledger.WeftRun) any {
	if !run.HaltStage.Valid || !run.HaltReason.Valid {
		return nil
	}
	return map[string]any{"stage": run.HaltStage.String, "reason": run.HaltReason.String, "detail": run.HaltDetail.String}
}

// v2Run is the full run (run.json).
func v2Run(run *ledger.WeftRun, store *ledger.Store) (map[string]any, error) {
	var action, failure any
	if run.ActionRequired.Valid {
		options := actionOptions[run.ActionRequired.String]
		if options == nil {
			options = []string{}
		}
		action = map[string]any{"reason": run.ActionRequired.String, "detail": run.ActionDetail.String, "options": options}
	}
	if run.Phase == ledger.WeftPhaseEnded && len(run.Segments) > 0 {
		failure = nullString(run.Segments[len(run.Segments)-1].Failure)
	}
	segments := make([]map[string]any, 0, len(run.Segments))
	for _, seg := range run.Segments {
		started := seg.ClaimedAt
		if seg.StartedAt.Valid {
			started = seg.StartedAt.String
		}
		var answer any
		if seg.AnswerID.Valid {
			a, err := store.WeftAnswer(seg.AnswerID.String)
			if err != nil {
				return nil, err
			}
			answer = map[string]any{"question_id": a.QuestionID, "answer_id": a.ID}
		}
		// Nothing launches segments in this build until v2 dispatch wires
		// the supervisor back in; every recorded segment was claimed under
		// weft-remote/v1.
		segments = append(segments, map[string]any{
			"segment": seg.Segment, "kind": seg.Kind, "worker_contract": ContractVersion,
			"ledger_run_id": seg.LedgerRunID, "worker_run_id": nullString(seg.WorkerRunID),
			"run_dir": nullString(seg.RunDir), "answer": answer, "started_at": started,
			"ended_at": nullString(seg.EndedAt), "terminal": nullString(seg.Terminal),
			"failure": nullString(seg.Failure),
		})
	}
	stages := make([]map[string]any, 0, len(run.Stages))
	for _, stage := range run.Stages {
		stages = append(stages, stageResource(stage))
	}
	var question any
	q, err := store.LatestWeftQuestion(run.ID)
	switch {
	case err == nil:
		question = v2Question(q)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	return map[string]any{
		"run_id": run.ID, "ticket": run.Ticket, "project": run.Project,
		"repository_id": run.RepositoryID, "repo": run.Repo, "branch": run.Branch,
		"worktree": run.Worktree, "base": nullString(run.Base), "head": nullString(run.Head),
		"origin": run.Origin, "recorded_under": run.RecordedUnder, "entry_id": nullString(run.EntryID),
		"state": runState(run), "phase": ledger.WeftV2Phase(run.Phase), "outcome": nullString(run.Outcome),
		"failed": runFailed(run), "stopped": run.Stopped, "halt": v2Halt(run), "failure": failure,
		"action_required": action, "settlement": v2Settlement(run), "disposition": run.Disposition,
		"archive_ref": nullString(run.ArchiveRef), "supersedes": nullString(run.Supersedes),
		"superseded_by": nullString(run.SupersededBy), "revision": run.Revision,
		"created_at": run.CreatedAt, "updated_at": run.UpdatedAt, "ended_at": nullString(run.EndedAt),
		"liveness": nil, "summary": rawJSON(run.Summary), "segments": segments, "stages": stages,
		"question": question,
	}, nil
}

// v2Result is the result alone (run-result.json).
func v2Result(run *ledger.WeftRun) map[string]any {
	ids := []string{}
	for _, seg := range run.Segments {
		if seg.WorkerRunID.Valid {
			ids = append(ids, seg.WorkerRunID.String)
		}
	}
	return map[string]any{
		"run_id": run.ID, "ticket": run.Ticket, "state": runState(run), "outcome": nullString(run.Outcome),
		"failed": runFailed(run), "stopped": run.Stopped, "ended_at": nullString(run.EndedAt),
		"repo": run.Repo, "branch": run.Branch, "worktree": run.Worktree, "base": nullString(run.Base),
		"head": nullString(run.Head), "halt": v2Halt(run), "segments": len(run.Segments),
		"worker_run_ids": ids, "settlement": v2Settlement(run), "disposition": run.Disposition,
		"archive_ref": nullString(run.ArchiveRef), "summary": rawJSON(run.Summary),
	}
}

func v2Question(q *ledger.WeftQuestion) map[string]any {
	var answer any
	if a := q.Answer; a != nil {
		answer = map[string]any{
			"answer_id": a.ID, "text": a.Text, "submitted_at": a.SubmittedAt,
			"entry_id": nullString(a.EntryID), "consumed_segment": nullInt(a.ConsumedSegment),
		}
	}
	return map[string]any{
		"question_id": q.ID, "run_id": q.RunID, "ticket": q.Ticket, "segment": q.Segment,
		"stage": q.Stage, "occurrence": q.Occurrence, "attempt": q.Attempt, "text": q.Text,
		"asked_at": q.AskedAt, "state": q.State, "answer": answer,
	}
}

func (s *Service) handleV2Runs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	q := ledger.WeftRunQuery{
		Ticket: query.Get("ticket"), State: query.Get("state"), Origin: query.Get("origin"),
		Before: query.Get("before"), Limit: runsPage + 1,
	}
	if q.State != "" && !oneOf(q.State, "active", "blocked", "over") {
		s.v2Error(w, http.StatusBadRequest, "malformed", "state must be active, blocked or over")
		return
	}
	switch q.Origin {
	case "", ledger.WeftOriginHosted:
	case "archive":
		s.capabilityNotServed(w, "archive")
		return
	default:
		s.v2Error(w, http.StatusBadRequest, "malformed", "origin must be hosted or archive")
		return
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if q.Before != "" {
		if _, err := s.store.GetWeftRun(q.Before); err != nil {
			s.v2Error(w, http.StatusBadRequest, "malformed", "before names "+q.Before+", which is not a run on host "+s.hostID)
			return
		}
	}
	runs, err := s.store.QueryWeftRuns(q)
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	var next any
	if len(runs) > runsPage {
		runs = runs[:runsPage]
		next = runs[runsPage-1].ID
	}
	items := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		items = append(items, v2RunHeader(run))
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"runs": items, "page": map[string]any{"next_before": next}})
}

func (s *Service) handleV2RunPath(w http.ResponseWriter, rest string) {
	runID, sub, _ := strings.Cut(rest, "/")
	if sub != "" && sub != "result" {
		s.v2Error(w, http.StatusNotFound, "malformed", "GET /v2/runs/"+rest+" is not a weft-service/v2 route on host "+s.hostID)
		return
	}
	if !validProtocolID(runID) {
		s.v2Error(w, http.StatusNotFound, "run_not_found", runID+" is not a run on host "+s.hostID)
		return
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	run, err := s.store.GetWeftRun(runID)
	if err != nil {
		s.v2Error(w, http.StatusNotFound, "run_not_found", runID+" is not a run on host "+s.hostID)
		return
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	if sub == "result" {
		if run.Phase != ledger.WeftPhaseEnded {
			s.v2Error(w, http.StatusConflict, "run_not_over", runID+" is not over")
			return
		}
		s.read(w, asOf, map[string]any{"result": v2Result(run)})
		return
	}
	resource, err := v2Run(run, s.store)
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"run": resource})
}

func (s *Service) handleV2Questions(w http.ResponseWriter) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	questions, err := s.store.PendingWeftQuestions()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	items := make([]map[string]any, 0, len(questions))
	for _, q := range questions {
		items = append(items, v2Question(q))
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"questions": items})
}

func (s *Service) handleV2Question(w http.ResponseWriter, id string) {
	if !validProtocolID(id) {
		s.v2Error(w, http.StatusNotFound, "question_not_found", id+" is not a question on host "+s.hostID)
		return
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	q, err := s.store.WeftQuestion(id)
	if err != nil {
		s.v2Error(w, http.StatusNotFound, "question_not_found", id+" is not a question on host "+s.hostID)
		return
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"question": v2Question(q)})
}
