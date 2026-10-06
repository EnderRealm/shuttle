package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
	observercontract "shuttle/internal/observer"
	"shuttle/internal/output"
)

const (
	observerOutputFile      = "observer.json"
	maxObserverOutput       = 4 << 20
	maxObserverReport       = 200 << 10
	minObserverReport       = 200
	maxObserverSubjectRunes = 120
)

type observerOutputManifest struct {
	Silent      *bool              `json:"silent"`
	Subject     string             `json:"subject,omitempty"`
	Assessments *[]json.RawMessage `json:"assessments"`
	Actions     *[]json.RawMessage `json:"actions"`
}

type validatedObserverOutput struct {
	Silent      bool
	Subject     string
	Report      []byte
	Assessments []*observercontract.Assessment
	Actions     []*observercontract.Action
}

type persistedObserverOutput struct {
	Silent      bool                           `json:"silent"`
	Subject     string                         `json:"subject,omitempty"`
	Assessments []*observercontract.Assessment `json:"assessments"`
	Actions     []*observercontract.Action     `json:"actions"`
}

type observerObservationReceipt struct {
	RequestID      string `json:"request_id"`
	EntryID        string `json:"entry_id"`
	RevisionBefore int64  `json:"revision_before"`
}

type observerApplied struct {
	NotebookRevision int64                        `json:"notebook_revision"`
	Observations     []observerObservationReceipt `json:"observations"`
	Actions          []observerActionRecord       `json:"actions"`
	Report           *output.Applied              `json:"report,omitempty"`
	DeliveryStatus   string                       `json:"delivery_status"`
	DeliveryError    string                       `json:"delivery_error,omitempty"`
}

type observerResultRecord struct {
	Collection       string `json:"collection"`
	EvidenceCount    int    `json:"evidence_count"`
	AssessmentStatus string `json:"assessment_status"`
	AssessmentCount  int    `json:"assessment_count"`
	ActionCount      int    `json:"action_count"`
	ActionsApplied   int    `json:"actions_applied"`
	ActionsRefused   int    `json:"actions_refused"`
	ActionsFailed    int    `json:"actions_failed"`
	NotebookStatus   string `json:"notebook_status"`
	NotebookRevision int64  `json:"notebook_revision"`
	Silent           *bool  `json:"silent,omitempty"`
	Report           bool   `json:"report"`
	Detail           string `json:"detail,omitempty"`
}

func validateObserverOutput(outDir string, def *agentdef.Definition, state *observerRunState) (*validatedObserverOutput, error) {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return nil, err
	}
	present := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.Name() != observerOutputFile && entry.Name() != output.ReportFile {
			return nil, fmt.Errorf("output %s: observer out/ may contain only %s and optional %s", entry.Name(), observerOutputFile, output.ReportFile)
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("output %s: not a regular file", entry.Name())
		}
		present[entry.Name()] = true
	}
	if !present[observerOutputFile] {
		return nil, fmt.Errorf("output %s: missing", observerOutputFile)
	}
	raw, err := readBounded(filepath.Join(outDir, observerOutputFile), maxObserverOutput)
	if err != nil {
		return nil, err
	}
	var manifest observerOutputManifest
	if err := strictJSON(raw, &manifest); err != nil {
		return nil, fmt.Errorf("output %s: %w", observerOutputFile, err)
	}
	if manifest.Silent == nil {
		return nil, fmt.Errorf("output %s: silent is required", observerOutputFile)
	}
	if manifest.Assessments == nil {
		return nil, fmt.Errorf("output %s: assessments is required and must be an array", observerOutputFile)
	}
	if manifest.Actions == nil {
		return nil, fmt.Errorf("output %s: actions is required and must be an array", observerOutputFile)
	}
	validated := &validatedObserverOutput{
		Silent: *manifest.Silent, Subject: manifest.Subject,
		Assessments: make([]*observercontract.Assessment, 0, len(*manifest.Assessments)),
		Actions:     make([]*observercontract.Action, 0, len(*manifest.Actions)),
	}
	if present[output.ReportFile] {
		if validated.Silent {
			return nil, fmt.Errorf("output %s: present while silent is true", output.ReportFile)
		}
	}
	validated.Report, err = loadObserverReport(filepath.Join(outDir, output.ReportFile), validated.Silent)
	if err != nil {
		return nil, err
	}
	if !validated.Silent && def.Manifest.Outputs.Email != nil && def.Manifest.Outputs.Email.Max > 0 {
		if err := validateObserverSubject(validated.Subject); err != nil {
			return nil, err
		}
	}

	evidence := make(map[string]bool, len(state.Collection.Evidence))
	sources := make(map[string]bool)
	for _, bundle := range state.Collection.Evidence {
		evidence[canonicalJSON(bundle)] = true
		for _, source := range bundle.Sources {
			sources[source.Kind+"\x00"+source.Ref] = true
		}
	}
	entryIDs := make(map[string]bool, len(state.Snapshot.Entries)+len(*manifest.Assessments))
	for _, entry := range state.Snapshot.Entries {
		entryIDs[entry.ID] = true
	}
	for i, raw := range *manifest.Assessments {
		assessment, err := observercontract.ValidateAssessment(raw)
		if err != nil {
			return nil, fmt.Errorf("output %s assessments[%d]: %w", observerOutputFile, i, err)
		}
		if assessment.Identity != state.Definition.Identity {
			return nil, fmt.Errorf("output %s assessments[%d].identity does not match configured observer", observerOutputFile, i)
		}
		if assessment.Finding == nil && !entryIDs[assessment.QuestionID] {
			return nil, fmt.Errorf("output %s assessments[%d].question_id %q was not in the selected notebook snapshot", observerOutputFile, i, assessment.QuestionID)
		}
		if assessment.Finding != nil && entryIDs[assessment.Finding.ID] {
			return nil, fmt.Errorf("output %s assessments[%d].finding.id %q collides with a notebook entry or earlier finding", observerOutputFile, i, assessment.Finding.ID)
		}
		if !evidence[canonicalJSON(assessment.Evidence)] {
			return nil, fmt.Errorf("output %s assessments[%d].evidence was not returned by the collector", observerOutputFile, i)
		}
		validated.Assessments = append(validated.Assessments, assessment)
		entryIDs[assessment.EntryID()] = true
	}
	actionIDs := make(map[string]bool, len(*manifest.Actions))
	for i, raw := range *manifest.Actions {
		action, err := observercontract.ValidateAction(raw)
		if err != nil {
			return nil, fmt.Errorf("output %s actions[%d]: %w", observerOutputFile, i, err)
		}
		if action.Identity != state.Definition.Identity {
			return nil, fmt.Errorf("output %s actions[%d].identity does not match configured observer", observerOutputFile, i)
		}
		if actionIDs[action.ID] {
			return nil, fmt.Errorf("output %s actions[%d].id %q appears twice", observerOutputFile, i, action.ID)
		}
		actionIDs[action.ID] = true
		if !entryIDs[action.QuestionID] {
			return nil, fmt.Errorf("output %s actions[%d].question_id %q is not in the notebook or this run's findings", observerOutputFile, i, action.QuestionID)
		}
		for j, source := range action.Evidence {
			if !sources[source.Kind+"\x00"+source.Ref] {
				return nil, fmt.Errorf("output %s actions[%d].evidence[%d] was not returned by the collector", observerOutputFile, i, j)
			}
		}
		validated.Actions = append(validated.Actions, action)
	}
	if err := writeJSON(filepath.Join(stateRunDir(outDir), "observer", "validated.json"), persistedObserverOutput{
		Silent: validated.Silent, Subject: validated.Subject,
		Assessments: validated.Assessments, Actions: validated.Actions,
	}, 0o600); err != nil {
		return nil, err
	}
	return validated, nil
}

func validateObserverSubject(subject string) error {
	if subject == "" {
		return fmt.Errorf("output %s: subject is required when email delivery is enabled and silent is false", observerOutputFile)
	}
	if strings.ContainsAny(subject, "\r\n") {
		return fmt.Errorf("output %s: subject contains a newline", observerOutputFile)
	}
	if utf8.RuneCountInString(subject) > maxObserverSubjectRunes {
		return fmt.Errorf("output %s: subject is longer than %d characters", observerOutputFile, maxObserverSubjectRunes)
	}
	for _, r := range subject {
		if !unicode.IsPrint(r) {
			return fmt.Errorf("output %s: subject contains unprintable character %q", observerOutputFile, r)
		}
	}
	return nil
}

func canonicalJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func stateRunDir(outDir string) string { return filepath.Dir(outDir) }

func loadObserverReport(path string, silent bool) ([]byte, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && silent {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("output %s: %w", output.ReportFile, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("output %s: not a regular file", output.ReportFile)
	}
	if silent {
		return nil, fmt.Errorf("output %s: present while silent is true", output.ReportFile)
	}
	report, err := readBounded(path, maxObserverReport)
	if err != nil {
		return nil, fmt.Errorf("output %s: %w", output.ReportFile, err)
	}
	if !utf8.Valid(report) {
		return nil, fmt.Errorf("output %s: not valid UTF-8", output.ReportFile)
	}
	if len(report) < minObserverReport {
		return nil, fmt.Errorf("output %s: %d bytes, at least %d when silent is false", output.ReportFile, len(report), minObserverReport)
	}
	return report, nil
}

func (p *Pipeline) applyObserverAssessments(ctx context.Context, runID, runDir string, state *observerRunState, out *validatedObserverOutput) (*observerApplied, error) {
	applied := &observerApplied{
		NotebookRevision: state.Snapshot.Revision,
		Observations:     []observerObservationReceipt{},
		Actions:          newObserverActionRecords(out.Actions),
		DeliveryStatus:   ledger.DeliveryPending,
	}
	for i, assessment := range out.Assessments {
		if err := p.recordObserverAssessment(ctx, runID, runDir, state, applied, i, assessment); err != nil {
			return applied, err
		}
	}
	revision, err := state.Notebook.Revision(ctx)
	if err != nil {
		return applied, fmt.Errorf("read notebook revision after apply: %w", err)
	}
	applied.NotebookRevision = revision
	if err := writeObserverApplied(p, runID, runDir, applied); err != nil {
		return applied, err
	}
	return applied, nil
}

// assessmentRequestID is the request id assessment index of a run is recorded
// under, live or replayed by a reconciliation.
func assessmentRequestID(runID string, index int) string {
	return fmt.Sprintf("%s-assessment-%03d", runID, index+1)
}

// recordObserverAssessment records one assessment under its position's
// request id and revision, then appends the receipt an action authorized by
// it is admitted on. The live apply and a reconciliation replaying a record
// that never landed share this step, so a replay carries the same id and
// revision the run would have.
func (p *Pipeline) recordObserverAssessment(ctx context.Context, runID, runDir string, state *observerRunState, applied *observerApplied, index int, assessment *observercontract.Assessment) error {
	requestID := assessmentRequestID(runID, index)
	revision := state.Snapshot.Revision + int64(index)
	request := observercontract.Request{
		Identity: state.Definition.Identity, Revision: revision, RequestID: requestID,
	}
	_, err := state.Notebook.Record(ctx, request, assessment)
	if err != nil && !errors.Is(err, observercontract.ErrStaleRevision) &&
		!errors.Is(err, observercontract.ErrRetired) &&
		!errors.Is(err, observercontract.ErrRequestConflict) {
		// The adapter may have committed before its process failed. Replay
		// the identical request id and revision once; conforming adapters
		// return the original outcome without a duplicate observation.
		_, err = state.Notebook.Record(ctx, request, assessment)
	}
	if err != nil {
		_ = writeObserverApplied(p, runID, runDir, applied)
		return fmt.Errorf("record assessment %d with request %s: %w", index, requestID, err)
	}
	applied.Observations = append(applied.Observations, observerObservationReceipt{
		RequestID: requestID, EntryID: assessment.EntryID(), RevisionBefore: revision,
	})
	applied.NotebookRevision = revision + 1
	return writeObserverApplied(p, runID, runDir, applied)
}

// observationRecorded reports whether the assessment's record receipt is in
// the durable apply record: the notebook admitted it under the request id its
// position derives.
func observationRecorded(applied *observerApplied, requestID string) bool {
	for _, receipt := range applied.Observations {
		if receipt.RequestID == requestID {
			return true
		}
	}
	return false
}

func writeObserverApplied(p *Pipeline, runID, runDir string, applied *observerApplied) error {
	data, err := json.Marshal(applied)
	if err != nil {
		return err
	}
	if err := p.Ledger.SetOutputsApplied(runID, data); err != nil {
		return err
	}
	return writeJSON(filepath.Join(runDir, appliedFileName()), applied, 0o600)
}

func (p *Pipeline) prepareObserverReport(def *agentdef.Definition, row *ledger.Run, run *preparedRun, opts Options, out *validatedObserverOutput, applied *observerApplied) error {
	report := &output.Outputs{Silent: out.Silent, Subject: out.Subject, Report: out.Report}
	if len(out.Report) > 0 {
		if file := def.Manifest.Outputs.File; file != nil && file.Max > 0 {
			var err error
			applied.Report, err = p.apply(output.Target{
				ReportsDir: filepath.Join(p.Home, reportsDir), Agent: def.Name,
				Dir: file.Dir, Label: run.Window.Label, RunID: row.ID,
			}, report, p.logger())
			if err != nil {
				return err
			}
		} else {
			applied.Report = &output.Applied{Subject: out.Subject, DeliveryStatus: ledger.DeliveryPending}
		}
	}
	if !observerDeliveryEligible(def, out) || opts.NoDeliver {
		applied.DeliveryStatus = ledger.DeliverySkipped
		if applied.Report != nil {
			applied.Report.DeliveryStatus = ledger.DeliverySkipped
		}
		if err := p.Ledger.SetDelivery(row.ID, ledger.DeliverySkipped, ""); err != nil {
			return err
		}
		return writeObserverApplied(p, row.ID, run.Dir, applied)
	}
	applied.DeliveryStatus = ledger.DeliveryPending
	if err := p.Ledger.SetDelivery(row.ID, ledger.DeliveryPending, ""); err != nil {
		return err
	}
	return writeObserverApplied(p, row.ID, run.Dir, applied)
}

func (p *Pipeline) deliverObserverReport(ctx context.Context, def *agentdef.Definition, row *ledger.Run, run *preparedRun, out *validatedObserverOutput, applied *observerApplied) error {
	if !observerDeliveryEligible(def, out) {
		applied.DeliveryStatus = ledger.DeliverySkipped
		if applied.Report != nil {
			applied.Report.DeliveryStatus = ledger.DeliverySkipped
		}
		if err := p.Ledger.SetDelivery(row.ID, ledger.DeliverySkipped, ""); err != nil {
			return err
		}
		return writeObserverApplied(p, row.ID, run.Dir, applied)
	}
	if applied.DeliveryStatus != ledger.DeliveryPending || p.Sender == nil {
		return nil
	}
	html, err := output.Render(out.Report, output.AllowHosts(p.Config.Output.LinkHosts, p.Config.Paths.TKConfig))
	if err == nil {
		err = p.Sender.Send(ctx, Delivery{
			RunID: row.ID, Agent: def.Name, Subject: out.Subject, Text: out.Report, HTML: html,
		})
	}
	if err != nil {
		applied.DeliveryStatus = ledger.DeliveryFailed
		applied.DeliveryError = err.Error()
		if applied.Report != nil {
			applied.Report.DeliveryStatus = ledger.DeliveryFailed
			applied.Report.DeliveryError = err.Error()
		}
		if ledgerErr := p.Ledger.SetDelivery(row.ID, ledger.DeliveryFailed, err.Error()); ledgerErr != nil {
			return errors.Join(err, ledgerErr)
		}
		if recordErr := writeObserverApplied(p, row.ID, run.Dir, applied); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		if incidentErr := p.openDeliveryAuthIncident(row.ID, err); incidentErr != nil {
			return errors.Join(err, incidentErr)
		}
		return err
	}
	applied.DeliveryStatus = ledger.DeliveryDelivered
	applied.DeliveryError = ""
	if applied.Report != nil {
		applied.Report.DeliveryStatus = ledger.DeliveryDelivered
		applied.Report.DeliveryError = ""
	}
	if err := p.Ledger.SetDelivery(row.ID, ledger.DeliveryDelivered, ""); err != nil {
		return err
	}
	return writeObserverApplied(p, row.ID, run.Dir, applied)
}

func observerDeliveryEligible(def *agentdef.Definition, out *validatedObserverOutput) bool {
	return def != nil && out != nil && !out.Silent && len(out.Report) > 0 &&
		def.Manifest.Outputs.Email != nil && def.Manifest.Outputs.Email.Max > 0
}

func writeObserverResult(runDir string, result observerResultRecord) error {
	return writeJSON(filepath.Join(runDir, "observer-result.json"), result, 0o600)
}

func loadValidatedObserverOutput(runDir string) (*validatedObserverOutput, error) {
	data, err := readBounded(filepath.Join(runDir, "observer", "validated.json"), maxObserverOutput)
	if err != nil {
		return nil, err
	}
	var persisted persistedObserverOutput
	if err := strictJSON(data, &persisted); err != nil {
		return nil, err
	}
	out := &validatedObserverOutput{
		Silent: persisted.Silent, Subject: persisted.Subject,
		Assessments: persisted.Assessments, Actions: persisted.Actions,
	}
	reportPath := filepath.Join(runDir, "out", output.ReportFile)
	out.Report, err = loadObserverReport(reportPath, out.Silent)
	if err != nil {
		return nil, err
	}
	return out, nil
}
