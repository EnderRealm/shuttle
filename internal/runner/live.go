package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/output"
	"shuttle/internal/provider"
	"shuttle/internal/runtime/claude"
	"shuttle/internal/sandbox"
)

const (
	transcriptFile = "transcript.ndjson"
	stderrFile     = "stderr.log"
	resultFile     = "result.json"
)

// Delivery is the retained, validated message handed to the configured sender.
// The concrete Cloudflare transport is a separate package; keeping it behind
// this seam lets a delivery retry read the run directory without launching the
// model again.
type Delivery struct {
	RunID   string
	Agent   string
	Subject string
	Text    []byte
	HTML    []byte
}

type Sender interface {
	Send(context.Context, Delivery) error
}

type Options struct {
	UnsafeNoSandbox bool
	NoDeliver       bool
	Auth            *AuthTick
}

// Pipeline is the shared manual/scheduled execution path.
type Pipeline struct {
	Ledger  *ledger.Store
	Config  *config.Config
	Home    string
	HomeDir string
	PATH    string
	Logger  *log.Logger
	Auth    *AuthTick
	Sender  Sender
	Now     func() time.Time
	FindKey func(string) (string, error)
	applyMu sync.Mutex
}

type RunResult struct {
	RunID  string
	RunDir string
	Result ledger.Result
}

type resultRecord struct {
	Status       string   `json:"status"`
	FailureClass string   `json:"failure_class,omitempty"`
	Detail       string   `json:"detail,omitempty"`
	ExitCode     *int     `json:"exit_code"`
	SessionID    string   `json:"session_id,omitempty"`
	NumTurns     *int     `json:"num_turns"`
	CostUSD      *float64 `json:"cost_usd"`
	InputTokens  *int     `json:"input_tokens"`
	OutputTokens *int     `json:"output_tokens"`
	Silent       *bool    `json:"silent"`
	StartedAt    string   `json:"started_at"`
	EndedAt      string   `json:"ended_at"`
}

// Run executes an already claimed row and always asks the ledger for a
// terminal transition before returning from a stage failure.
func (p *Pipeline) Run(ctx context.Context, def *agentdef.Definition, row *ledger.Run, opts Options) (*RunResult, error) {
	if p == nil || p.Ledger == nil || p.Config == nil {
		return nil, errors.New("runner: ledger and config are required")
	}
	if def == nil || row == nil {
		return nil, errors.New("runner: definition and claimed run are required")
	}
	if row.Status != ledger.StatusClaimed {
		return nil, fmt.Errorf("run %s: expected claimed, got %s", row.ID, row.Status)
	}
	slot, err := time.Parse(time.RFC3339, row.ScheduledFor)
	if err != nil {
		return nil, p.fail(nil, row.ID, ledger.ClassPreflight, fmt.Errorf("scheduled_for %q: %w", row.ScheduledFor, err), ledger.Result{}, resultRecord{})
	}
	started := p.now()
	run, err := prepare(def, slot, row.ID, row.TriggerKind, false, opts.UnsafeNoSandbox, opts.NoDeliver, p.Config, p.Home)
	if err != nil {
		return nil, p.fail(nil, row.ID, ledger.ClassPreflight, err, ledger.Result{}, resultRecord{StartedAt: stamp(started)})
	}
	record := resultRecord{StartedAt: stamp(started)}
	if err := p.Ledger.SetRunDir(row.ID, run.Dir); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassPreflight, err, ledger.Result{}, record)
	}
	preflight := Preflight{
		Config: p.Config, Home: p.Home, HomeDir: p.HomeDir, PATH: p.PATH,
		UnsafeNoSandbox: opts.UnsafeNoSandbox, NoDeliver: opts.NoDeliver, findKey: p.FindKey,
	}
	auth := opts.Auth
	if auth == nil {
		auth = p.Auth
	}
	if auth == nil {
		auth = &AuthTick{}
	}
	if _, err := auth.Check(ctx, preflight, def, run.Dir); err != nil {
		class := ledger.ClassPreflight
		var check *CheckError
		if errors.As(err, &check) && check.Class != "" {
			class = check.Class
		}
		return nil, p.fail(run, row.ID, class, err, ledger.Result{}, record)
	}
	if def.Manifest.Observer != nil {
		return p.runObserver(ctx, preflight, def, row, run, opts, record, slot)
	}
	if err := run.collect(ctx, def, p.Config, p.Home, p.logger(), slot); err != nil {
		class := ledger.ClassProvider
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) {
			class = ledger.ClassPreflight
		}
		return nil, p.fail(run, row.ID, class, err, ledger.Result{}, record)
	}
	if err := run.render(def); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassPreflight, err, ledger.Result{}, record)
	}

	_, result, record, class, err := p.executeRuntime(ctx, preflight, def, row, run, opts, record)
	if err != nil {
		return nil, p.fail(run, row.ID, class, err, result, record)
	}

	validated, err := output.Validate(filepath.Join(run.Dir, "out"))
	if err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	result.Silent = validated.Silent
	record.Silent = boolPointer(validated.Silent)
	applied := &output.Applied{DeliveryStatus: ledger.DeliveryPending}
	if file := def.Manifest.Outputs.File; file != nil && file.Max > 0 {
		applied, err = p.apply(output.Target{
			ReportsDir: filepath.Join(p.Home, reportsDir), Agent: def.Name,
			Dir: file.Dir, Label: run.Window.Label, RunID: row.ID,
		}, validated, p.logger())
		if err != nil {
			return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
		}
	} else if !validated.Silent {
		applied.Subject = validated.Subject
	}
	if validated.Silent || def.Manifest.Outputs.Email == nil || def.Manifest.Outputs.Email.Max == 0 || opts.NoDeliver {
		applied.DeliveryStatus = ledger.DeliverySkipped
	}
	if err := p.Ledger.SetDelivery(row.ID, applied.DeliveryStatus, ""); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	if err := p.recordApplied(row.ID, run.Dir, applied); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	record.Status = ledger.StatusCompleted
	record.EndedAt = stamp(p.now())
	if err := writeResult(run.Dir, record); err != nil {
		return nil, p.fail(run, row.ID, ledger.ClassOutput, err, result, record)
	}
	if err := p.Ledger.Complete(row.ID, result); err != nil {
		return nil, err
	}
	runResult := &RunResult{RunID: row.ID, RunDir: run.Dir, Result: result}
	if applied.DeliveryStatus == ledger.DeliveryPending && p.Sender != nil {
		if err := p.deliver(ctx, def, row.ID, run.Dir, validated, applied); err != nil {
			return runResult, err
		}
	}
	return runResult, nil
}

func (p *Pipeline) executeRuntime(
	ctx context.Context,
	preflight Preflight,
	def *agentdef.Definition,
	row *ledger.Run,
	run *preparedRun,
	opts Options,
	record resultRecord,
) (*claude.Outcome, ledger.Result, resultRecord, string, error) {
	key, err := preflight.key(p.Config.Anthropic.KeychainItem)
	if err != nil {
		return nil, ledger.Result{}, record, ledger.ClassPreflight, err
	}
	var launcher []string
	if !opts.UnsafeNoSandbox {
		launcher, err = sandbox.Launcher(preflight.sandbox(def, run.Dir))
		if err != nil {
			return nil, ledger.Result{}, record, ledger.ClassSandbox, err
		}
	}
	runtimeOptions := claude.Options{
		Bin: p.Config.Paths.ClaudeBin, Launcher: launcher, RunDir: run.Dir,
		Auth: def.Manifest.Runtime.Auth, Model: def.Manifest.Runtime.Model,
		MaxTurns: def.Manifest.Runtime.MaxTurns, MaxBudgetUSD: def.Manifest.Runtime.MaxBudgetUSD,
		Tools: def.Manifest.Runtime.Tools, TimeoutSeconds: def.Manifest.Runtime.TimeoutSeconds,
		APIKey: key, Prompt: run.Prompt, PATH: p.PATH, HomeDir: p.HomeDir,
		Timezone: run.Loc.String(),
	}
	if def.Manifest.Observer != nil {
		runtimeOptions.ReadRoots = []string{filepath.Join(run.Dir, "inputs")}
	}
	run.record.Argv, err = claude.Argv(runtimeOptions)
	if err != nil {
		return nil, ledger.Result{}, record, ledger.ClassPreflight, err
	}
	if err := run.writeRecord(); err != nil {
		return nil, ledger.Result{}, record, ledger.ClassPreflight, err
	}

	transcript, err := os.OpenFile(filepath.Join(run.Dir, transcriptFile), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, ledger.Result{}, record, ledger.ClassPreflight, err
	}
	stderr, err := os.OpenFile(filepath.Join(run.Dir, stderrFile), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		_ = transcript.Close()
		return nil, ledger.Result{}, record, ledger.ClassPreflight, err
	}
	runtimeOptions.Started = func(pid int) error {
		run.record.PID = pid
		if err := run.writeRecord(); err != nil {
			return err
		}
		return p.Ledger.MarkRunning(row.ID, pid, run.Dir)
	}
	outcome, launchErr := claude.Run(ctx, runtimeOptions, transcript, stderr)
	transcriptCloseErr := transcript.Close()
	stderrCloseErr := stderr.Close()
	if launchErr != nil {
		return nil, ledger.Result{}, record, ledger.ClassDied, launchErr
	}
	result := ledgerResult(outcome)
	record = recordFromOutcome(record, outcome)
	for _, evidenceErr := range []error{outcome.TranscriptErr, outcome.StderrErr, transcriptCloseErr, stderrCloseErr} {
		if evidenceErr != nil {
			return outcome, result, record, ledger.ClassDied, fmt.Errorf("retain runtime evidence: %w", evidenceErr)
		}
	}
	if outcome.Class != "" {
		return outcome, result, record, outcome.Class, errors.New(outcome.Detail)
	}
	return outcome, result, record, "", nil
}

func (p *Pipeline) apply(target output.Target, out *output.Outputs, logger *log.Logger) (*output.Applied, error) {
	// Every agent archives into one reports repository. Its Git index is a
	// process-wide mutable resource, so daemon workers may prepare/model in
	// parallel but must apply and commit one report at a time.
	p.applyMu.Lock()
	defer p.applyMu.Unlock()
	return output.Apply(target, out, logger)
}

// RetryDelivery validates and renders retained output from a completed run. It
// has no runtime dependency and therefore cannot spend model tokens.
func (p *Pipeline) RetryDelivery(ctx context.Context, def *agentdef.Definition, row *ledger.Run) error {
	if p.Sender == nil {
		return errors.New("delivery sender is not configured")
	}
	if row.Status != ledger.StatusCompleted || !row.RunDir.Valid {
		return fmt.Errorf("run %s: retained output is not available from a completed run", row.ID)
	}
	recordData, err := os.ReadFile(filepath.Join(row.RunDir.String, "run.json"))
	if err != nil {
		return err
	}
	var retained runRecord
	if err := json.Unmarshal(recordData, &retained); err != nil {
		return fmt.Errorf("retained run.json: %w", err)
	}
	if retained.Observer != nil {
		// An observer retry delivers under the capabilities the run retained,
		// not whatever the definition says now.
		retainedDef := *def
		retainedDef.Manifest.Outputs = retained.Outputs
		retainedDef.Manifest.Observer = &agentdef.Observer{Config: retained.Observer.Config}
		validated, err := loadValidatedObserverOutput(row.RunDir.String)
		if err != nil {
			return err
		}
		eligible := observerDeliveryEligible(&retainedDef, validated)
		if eligible {
			if err := validateObserverSubject(validated.Subject); err != nil {
				return err
			}
		}
		data, err := os.ReadFile(filepath.Join(row.RunDir.String, appliedFileName()))
		if err != nil {
			return err
		}
		var applied observerApplied
		if err := json.Unmarshal(data, &applied); err != nil {
			return err
		}
		if eligible {
			applied.DeliveryStatus = ledger.DeliveryPending
		}
		return p.deliverObserverReport(ctx, &retainedDef, row, &preparedRun{Dir: row.RunDir.String}, validated, &applied)
	}
	validated, err := output.Validate(filepath.Join(row.RunDir.String, "out"))
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(row.RunDir.String, appliedFileName()))
	if err != nil {
		return err
	}
	var applied output.Applied
	if err := json.Unmarshal(data, &applied); err != nil {
		return err
	}
	return p.deliver(ctx, def, row.ID, row.RunDir.String, validated, &applied)
}

func (p *Pipeline) deliver(ctx context.Context, def *agentdef.Definition, runID, runDir string, out *output.Outputs, applied *output.Applied) error {
	if out.Silent || def.Manifest.Outputs.Email == nil || def.Manifest.Outputs.Email.Max == 0 {
		applied.DeliveryStatus = ledger.DeliverySkipped
		return p.recordApplied(runID, runDir, applied)
	}
	html, err := output.Render(out.Report, output.AllowHosts(p.Config.Output.LinkHosts, p.Config.Paths.TKConfig))
	if err == nil {
		err = p.Sender.Send(ctx, Delivery{
			RunID: runID, Agent: def.Name, Subject: out.Subject,
			Text: out.Report, HTML: html,
		})
	}
	if err != nil {
		applied.DeliveryStatus = ledger.DeliveryFailed
		applied.DeliveryError = err.Error()
		if recordErr := p.Ledger.SetDelivery(runID, ledger.DeliveryFailed, err.Error()); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		if recordErr := p.recordApplied(runID, runDir, applied); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		if incidentErr := p.openDeliveryAuthIncident(runID, err); incidentErr != nil {
			return errors.Join(err, incidentErr)
		}
		return err
	}
	applied.DeliveryStatus = ledger.DeliveryDelivered
	applied.DeliveryError = ""
	if err := p.Ledger.SetDelivery(runID, ledger.DeliveryDelivered, ""); err != nil {
		return err
	}
	return p.recordApplied(runID, runDir, applied)
}

func (p *Pipeline) recordApplied(runID, runDir string, applied *output.Applied) error {
	data, err := json.Marshal(applied)
	if err != nil {
		return err
	}
	if err := p.Ledger.SetOutputsApplied(runID, data); err != nil {
		return err
	}
	return output.WriteApplied(runDir, applied)
}

func (p *Pipeline) fail(run *preparedRun, id, class string, cause error, result ledger.Result, record resultRecord) error {
	record.Status = ledger.StatusFailed
	record.FailureClass = class
	record.Detail = cause.Error()
	record.EndedAt = stamp(p.now())
	var evidenceErr error
	if run != nil {
		evidenceErr = writeResult(run.Dir, record)
	}
	ledgerErr := p.Ledger.Fail(id, class, cause.Error(), result)
	return errors.Join(cause, evidenceErr, ledgerErr)
}

func ledgerResult(out *claude.Outcome) ledger.Result {
	res := ledger.Result{SessionID: out.SessionID}
	if out.Result == nil {
		return res
	}
	if res.SessionID == "" {
		res.SessionID = out.Result.SessionID
	}
	res.NumTurns = sql.NullInt64{Int64: int64(out.Result.NumTurns), Valid: true}
	res.InputTokens = sql.NullInt64{Int64: int64(out.Result.InputTokens), Valid: true}
	res.OutputTokens = sql.NullInt64{Int64: int64(out.Result.OutputTokens), Valid: true}
	if out.Result.CostReported {
		res.CostUSD = sql.NullFloat64{Float64: out.Result.TotalCostUSD, Valid: true}
	}
	return res
}

func recordFromOutcome(record resultRecord, out *claude.Outcome) resultRecord {
	record.ExitCode = intPointer(out.ExitCode)
	record.SessionID = out.SessionID
	if out.Result == nil {
		return record
	}
	if record.SessionID == "" {
		record.SessionID = out.Result.SessionID
	}
	record.NumTurns = intPointer(out.Result.NumTurns)
	record.InputTokens = intPointer(out.Result.InputTokens)
	record.OutputTokens = intPointer(out.Result.OutputTokens)
	if out.Result.CostReported {
		record.CostUSD = floatPointer(out.Result.TotalCostUSD)
	}
	return record
}

func writeResult(runDir string, result resultRecord) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	// Replaced by rename so a failed write leaves the previous result.json
	// whole: a reconciliation reads it back and cannot finish past a torn one.
	tmp, err := os.CreateTemp(runDir, ".result-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(runDir, resultFile))
}

func (p *Pipeline) logger() *log.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return log.Default()
}

func (p *Pipeline) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func stamp(t time.Time) string        { return t.UTC().Format(time.RFC3339) }
func intPointer(v int) *int           { return &v }
func floatPointer(v float64) *float64 { return &v }
func boolPointer(v bool) *bool        { return &v }

// Kept local to runner so output's on-disk filename need not become part of
// its public API solely for retry loading.
func appliedFileName() string { return "applied.json" }
