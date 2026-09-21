package runner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
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
		return nil, p.fail(run, row.ID, ledger.ClassProvider, err, ledger.Result{}, record)
	}
	if err := run.renderObserver(def, state); err != nil {
		_ = promoteObserverEvidence(state.StagingDir, run.Dir)
		return nil, p.fail(run, row.ID, ledger.ClassPreflight, err, ledger.Result{}, record)
	}
	_, result, record, class, err := p.executeRuntime(ctx, preflight, def, row, run, opts, record)
	retainErr := promoteObserverEvidence(state.StagingDir, run.Dir)
	if retainErr != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, fmt.Errorf("retain observer evidence: %w", retainErr), result, record)
	}
	if err != nil {
		_ = writeObserverResult(run.Dir, observerResultRecord{
			Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
			AssessmentStatus: "not-run", NotebookStatus: "not-run", Detail: err.Error(),
		})
		return nil, p.fail(run, row.ID, class, err, result, record)
	}

	validated, err := validateObserverOutput(filepath.Join(run.Dir, "out"), def, state)
	if err != nil {
		_ = writeObserverResult(run.Dir, observerResultRecord{
			Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
			AssessmentStatus: "invalid", NotebookStatus: "not-run", Detail: err.Error(),
		})
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	applied, err := p.applyObserverAssessments(ctx, row.ID, run.Dir, state, validated)
	if err != nil {
		_ = writeObserverResult(run.Dir, observerResultRecord{
			Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
			AssessmentStatus: "validated", AssessmentCount: len(validated.Assessments),
			ActionCount: len(validated.Actions), NotebookStatus: "conflict-or-failure",
			NotebookRevision: applied.NotebookRevision, Silent: &validated.Silent,
			Report: len(validated.Report) > 0, Detail: err.Error(),
		})
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	if err := p.prepareObserverReport(def, row, run, opts, validated, applied); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	if err := writeObserverResult(run.Dir, observerResultRecord{
		Collection: state.Collection.Status, EvidenceCount: len(state.Collection.Evidence),
		AssessmentStatus: assessmentStatus(validated), AssessmentCount: len(validated.Assessments),
		ActionCount: len(validated.Actions), NotebookStatus: "applied",
		NotebookRevision: applied.NotebookRevision, Silent: &validated.Silent,
		Report: len(validated.Report) > 0,
	}); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}

	result.Silent = validated.Silent
	record.Silent = boolPointer(validated.Silent)
	record.Status = ledger.StatusCompleted
	record.EndedAt = stamp(p.now())
	if err := writeResult(run.Dir, record); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
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

func assessmentStatus(out *validatedObserverOutput) string {
	if len(out.Assessments) == 0 {
		return "none"
	}
	return "validated"
}
