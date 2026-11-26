package runner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/failure"
	"shuttle/internal/ledger"
	observercontract "shuttle/internal/observer"
)

func (p *Pipeline) runObserver(
	ctx context.Context,
	preflight Preflight,
	def *agentdef.Definition,
	row *ledger.Run,
	run *preparedRun,
	opts Options,
	record resultRecord,
	slot time.Time,
) (*RunResult, error) {
	state, err := p.collectObserver(ctx, def, run, slot)
	if err != nil {
		collection := "failed"
		var collectionErr *observerCollectionError
		if errors.As(err, &collectionErr) {
			collection = collectionErr.Status
		}
		_ = writeObserverResult(run.Dir, observerResultRecord{
			Collection: collection, AssessmentStatus: "not-run", NotebookStatus: "not-run",
			Detail: err.Error(),
		})
		return nil, p.fail(run, row.ID, failure.Provider, err, ledger.Result{}, record)
	}
	if err := run.renderObserver(def, state); err != nil {
		_ = promoteObserverEvidence(state.StagingDir, run.Dir)
		return nil, p.fail(run, row.ID, failure.Preflight, err, ledger.Result{}, record)
	}
	_, result, record, class, err := p.executeRuntime(ctx, preflight, def, row, run, opts, record)
	retainErr := promoteObserverEvidence(state.StagingDir, run.Dir)
	if retainErr != nil {
		p.pushObserverNotebook(ctx, state)
		return nil, p.fail(run, row.ID, failure.Output, fmt.Errorf("retain observer evidence: %w", retainErr), result, record)
	}
	if err != nil {
		p.pushObserverNotebook(ctx, state)
		_ = writeObserverResult(run.Dir, observerResultRecord{
			Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
			AssessmentStatus: "not-run", NotebookStatus: "not-run",
			NotebookPush: state.Push.Status, NotebookPushError: state.Push.Error, Detail: err.Error(),
		})
		return nil, p.fail(run, row.ID, class, err, result, record)
	}

	validated, err := validateObserverOutput(filepath.Join(run.Dir, "out"), def, state)
	if err != nil {
		p.pushObserverNotebook(ctx, state)
		_ = writeObserverResult(run.Dir, observerResultRecord{
			Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
			AssessmentStatus: "invalid", NotebookStatus: "not-run",
			NotebookPush: state.Push.Status, NotebookPushError: state.Push.Error, Detail: err.Error(),
		})
		return nil, p.fail(run, row.ID, failure.Output, err, result, record)
	}
	applied, err := p.applyObserverAssessments(ctx, row.ID, run.Dir, state, validated)
	if err == nil {
		err = p.applyObserverActions(ctx, row.ID, run.Dir, state, validated, applied)
	}
	p.pushObserverNotebook(ctx, state)
	if err != nil {
		_ = writeObserverResult(run.Dir, observerOutcome(state, validated, applied, "conflict-or-failure", err.Error()))
		return nil, p.fail(run, row.ID, failure.Output, err, result, record)
	}
	if err := p.prepareObserverReport(def, row, run, opts, validated, applied); err != nil {
		return nil, p.fail(run, row.ID, failure.Output, err, result, record)
	}
	if err := writeObserverResult(run.Dir, observerOutcome(state, validated, applied, "applied", "")); err != nil {
		return nil, p.fail(run, row.ID, failure.Output, err, result, record)
	}

	result.Silent = validated.Silent
	record.Silent = boolPointer(validated.Silent)
	record.Status = ledger.StatusCompleted
	record.EndedAt = stamp(p.now())
	if err := writeResult(run.Dir, record); err != nil {
		return nil, p.fail(run, row.ID, failure.Output, err, result, record)
	}
	if err := p.Ledger.Complete(row.ID, result); err != nil {
		return nil, err
	}
	runResult := &RunResult{RunID: row.ID, RunDir: run.Dir, Result: result}
	if err := p.deliverObserverReport(ctx, def, row, run, validated, applied); err != nil {
		return runResult, fmt.Errorf("observer delivery: %w", err)
	}
	return runResult, nil
}

func observerOutcome(state *observerRunState, out *validatedObserverOutput, applied *observerApplied, notebookStatus, detail string) observerResultRecord {
	result := observerResultRecord{
		Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
		AssessmentStatus: assessmentStatus(out), AssessmentCount: len(out.Assessments),
		ActionCount: len(out.Actions), NotebookStatus: notebookStatus,
		NotebookRevision: applied.NotebookRevision, NotebookPush: state.Push.Status,
		NotebookPushError: state.Push.Error, Silent: &out.Silent,
		Report: len(out.Report) > 0, Detail: detail,
	}
	result.ActionsApplied, result.ActionsRefused, result.ActionsFailed = observerActionCounts(applied.Actions)
	return result
}

// Push outcomes a run records beyond the ones Held.Push reports.
const (
	notebookPushLocalOnly = "local-only"
	notebookPushFailed    = observercontract.HeldPushFailed
)

type notebookPush struct {
	Status string
	Error  string
}

// pushObserverNotebook pushes a held notebook by the end of the run. A
// failed push is recorded and logged and does not fail the run: the commits
// are already durable here, and the next push carries them.
func (p *Pipeline) pushObserverNotebook(ctx context.Context, state *observerRunState) {
	held, ok := state.Notebook.(observercontract.Held)
	if !ok {
		return
	}
	status, err := pushHeld(ctx, state.Registration, held)
	if err != nil {
		state.Push = notebookPush{Status: notebookPushFailed, Error: err.Error()}
		p.logger().Printf("observer notebook push: %v", err)
		return
	}
	state.Push = notebookPush{Status: status}
}

// PushHeldNotebook pushes the notebook Shuttle holds for identity, as the end
// of a run does, for a change written outside one. Its registration is
// resolved as HeldNotebook resolves it.
func PushHeldNotebook(ctx context.Context, home string, cfg *config.Config, identity observercontract.Identity) (string, error) {
	registration, err := heldRegistration(cfg, identity)
	if err != nil {
		return "", err
	}
	held, err := heldNotebook(home, identity)
	if err != nil {
		return "", err
	}
	return pushHeld(ctx, registration, held)
}

// pushHeld pushes held under its registration's data policy. A local-only
// registration's notebook never leaves the machine, so it is not pushed
// whatever remote the repository has.
func pushHeld(ctx context.Context, registration config.Observer, held observercontract.Held) (string, error) {
	if registration.DataPolicy == "local-only" {
		return notebookPushLocalOnly, nil
	}
	return held.Push(ctx)
}

func assessmentStatus(out *validatedObserverOutput) string {
	if len(out.Assessments) == 0 {
		return "none"
	}
	return "validated"
}
