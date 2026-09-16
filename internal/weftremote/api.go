package weftremote

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

const ContractVersion = "weft-remote/v1"

func ticketListItem(t *ticket.Ticket, run *ledger.WeftRun) map[string]any {
	item := map[string]any{
		"id": t.ID, "project": projectOf(t.ID), "title": t.Title,
		"status": string(t.Status), "type": string(t.Type), "priority": t.Priority,
		"updated": formatTime(t.Updated), "branch": nil, "question": nil, "run": nil,
	}
	if t.Branch != "" {
		item["branch"] = t.Branch
	}
	if t.Extra != nil && t.Extra["question"] != "" {
		item["question"] = t.Extra["question"]
	}
	if run != nil {
		item["run"] = map[string]any{
			"run_id": run.ID, "state": runState(run), "phase": run.Phase,
			"outcome": nullString(run.Outcome),
		}
	}
	return item
}

func ticketDetail(t *ticket.Ticket, byID map[string]*ticket.Ticket, run *ledger.WeftRun) map[string]any {
	item := ticketListItem(t, run)
	description, _, acceptance, _ := ticket.BodySections(t.Body)
	item["description"] = description
	item["acceptance"] = acceptance
	item["notes_total"] = len(t.Notes)
	deps := make([]map[string]any, 0, len(t.Deps))
	for _, raw := range t.Deps {
		id := qualifyDep(t.ID, raw)
		status := ""
		if dep := byID[id]; dep != nil {
			status = string(dep.Status)
		}
		deps = append(deps, map[string]any{"id": id, "status": status})
	}
	item["deps"] = deps
	return item
}

func runListItem(run *ledger.WeftRun) map[string]any {
	segment, questionID := 0, any(nil)
	if len(run.Segments) > 0 {
		segment = run.Segments[len(run.Segments)-1].Segment
	}
	if run.Question != nil {
		questionID = run.Question.ID
	}
	return map[string]any{
		"run_id": run.ID, "ticket": run.Ticket, "project": run.Project,
		"repo": run.Repo, "branch": run.Branch, "worktree": run.Worktree,
		"base": nullString(run.Base), "state": runState(run), "phase": run.Phase,
		"outcome": nullString(run.Outcome), "failed": runFailed(run), "stopped": run.Stopped,
		"segment": segment, "question_id": questionID, "created_at": run.CreatedAt,
		"updated_at": run.UpdatedAt, "ended_at": nullString(run.EndedAt),
	}
}

func runResource(run *ledger.WeftRun, store *ledger.Store) (map[string]any, error) {
	resource := runListItem(run)
	delete(resource, "segment")
	delete(resource, "question_id")
	resource["summary"] = rawJSON(run.Summary)
	segments := make([]map[string]any, 0, len(run.Segments))
	for _, seg := range run.Segments {
		started := any(seg.ClaimedAt)
		if seg.StartedAt.Valid {
			started = seg.StartedAt.String
		}
		var answer any
		if seg.AnswerID.Valid {
			a, err := store.WeftAnswer(seg.AnswerID.String)
			if err != nil {
				return nil, fmt.Errorf("read segment %d answer: %w", seg.Segment, err)
			}
			answer = map[string]any{"question_id": a.QuestionID, "answer_id": a.ID}
		}
		segments = append(segments, map[string]any{
			"segment": seg.Segment, "kind": seg.Kind, "ledger_run_id": seg.LedgerRunID,
			"worker_run_id": nullString(seg.WorkerRunID), "run_dir": nullString(seg.RunDir),
			"answer": answer, "started_at": started, "ended_at": nullString(seg.EndedAt),
			"terminal": nullString(seg.Terminal), "failure": nullString(seg.Failure),
		})
	}
	resource["segments"] = segments
	stages := make([]map[string]any, 0, len(run.Stages))
	for _, stage := range run.Stages {
		stages = append(stages, stageResource(stage))
	}
	resource["stages"] = stages
	if run.Question == nil {
		resource["question"] = nil
	} else {
		resource["question"] = questionResource(run.Question)
	}
	if run.Phase == ledger.WeftPhaseEnded {
		resource["result"] = resultResource(run)
	} else {
		resource["result"] = nil
	}
	return resource, nil
}

func stageResource(s ledger.WeftStage) map[string]any {
	return map[string]any{
		"segment": s.Segment, "stage": s.Stage, "backend": nullString(s.Backend),
		"occurrence": s.Occurrence, "attempt": s.Attempt, "state": s.State,
		"session": nullString(s.Session), "detail": nullString(s.Detail),
		"exit_code": nullInt(s.ExitCode), "turns": nullInt(s.Turns),
		"spend": rawJSON(s.Spend), "processing_ms": nullInt(s.Processing),
		"tokens": rawJSON(s.Tokens), "at": s.At,
	}
}

func questionResource(q *ledger.WeftQuestion) map[string]any {
	var answer any
	if q.Answer != nil {
		answer = map[string]any{
			"answer_id": q.Answer.ID, "text": q.Answer.Text,
			"submitted_at": q.Answer.SubmittedAt, "segment": q.Answer.Segment,
		}
	}
	return map[string]any{
		"question_id": q.ID, "run_id": q.RunID, "ticket": q.Ticket,
		"segment": q.Segment, "stage": q.Stage, "occurrence": q.Occurrence,
		"attempt": q.Attempt, "text": q.Text, "asked_at": q.AskedAt,
		"state": q.State, "answer": answer,
	}
}

func resultResource(run *ledger.WeftRun) map[string]any {
	ids := make([]string, 0, len(run.Segments))
	for _, seg := range run.Segments {
		if seg.WorkerRunID.Valid {
			ids = append(ids, seg.WorkerRunID.String)
		}
	}
	var halt any
	if run.HaltStage.Valid || run.HaltReason.Valid {
		halt = map[string]any{
			"stage": nullString(run.HaltStage), "reason": nullString(run.HaltReason),
			"detail": nullString(run.HaltDetail),
		}
	}
	result := map[string]any{
		"run_id": run.ID, "ticket": run.Ticket, "state": runState(run),
		"outcome": nullString(run.Outcome), "failed": runFailed(run), "stopped": run.Stopped,
		"ended_at": nullString(run.EndedAt), "repo": run.Repo, "branch": run.Branch,
		"worktree": run.Worktree, "base": nullString(run.Base), "head": nullString(run.Head),
		"halt": halt, "segments": len(run.Segments), "worker_run_ids": ids,
		"summary": rawJSON(run.Summary),
	}
	if halt == nil && run.HaltDetail.Valid {
		result["detail"] = run.HaltDetail.String
	}
	return result
}

func runState(run *ledger.WeftRun) string {
	switch run.Phase {
	case ledger.WeftPhaseParked:
		return "blocked"
	case ledger.WeftPhaseEnded:
		return "over"
	default:
		return "active"
	}
}

func runFailed(run *ledger.WeftRun) bool {
	return run.Phase == ledger.WeftPhaseEnded && run.Outcome.String == "failed"
}

func nullString(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}

func nullInt(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func rawJSON(v json.RawMessage) any {
	if len(v) == 0 || string(v) == "null" {
		return nil
	}
	return v
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00.000Z"
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
