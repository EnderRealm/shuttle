package observer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const (
	maxCommandResponseBytes = 1 << 20
	maxCommandStderrBytes   = 8 << 10
)

// CommandNotebook is the JSON subprocess client described in
// docs/observer-contract.md §8. It never invokes a shell and receives an
// explicit directory and environment so its behavior does not depend on the
// shuttle process's cwd or project environment.
type CommandNotebook struct {
	Argv    []string
	Dir     string
	Env     []string
	Timeout time.Duration
}

func (c CommandNotebook) Revision(ctx context.Context) (int64, error) {
	var response struct {
		Revision *int64 `json:"revision"`
	}
	if err := c.call(ctx, map[string]any{"operation": "revision"}, &response); err != nil {
		return 0, err
	}
	if response.Revision == nil {
		return 0, errors.New("notebook response: revision is required")
	}
	if *response.Revision < 0 {
		return 0, fmt.Errorf("notebook revision %d is negative", *response.Revision)
	}
	return *response.Revision, nil
}

func (c CommandNotebook) Entry(ctx context.Context, id string) (*Entry, error) {
	var entry Entry
	if err := c.call(ctx, map[string]any{"operation": "entry", "entry_id": id}, &entry); err != nil {
		return nil, err
	}
	if err := validateEntry(&entry); err != nil {
		return nil, fmt.Errorf("notebook entry response: %w", err)
	}
	if entry.ID != id {
		return nil, fmt.Errorf("notebook entry response: id %q, requested %q", entry.ID, id)
	}
	return &entry, nil
}

func (c CommandNotebook) Register(ctx context.Context, req Request, q *Question) (*Entry, error) {
	if q == nil {
		return nil, fieldErr("payload", "question is null")
	}
	if err := q.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(q.Identity); err != nil {
		return nil, err
	}
	entry, err := c.entryMutation(ctx, "register", req, q)
	if err == nil && (entry.ID != q.ID || entry.Identity != q.Identity) {
		return nil, fmt.Errorf("notebook register response does not identify question %q", q.ID)
	}
	if err == nil && (entry.Question == nil || entry.Finding != nil || !reflect.DeepEqual(entry.Question, q) ||
		entry.State != StateObserving || entry.Experiment != nil || len(entry.Observations) != 0 ||
		len(entry.InFlight) != 0) {
		return nil, fmt.Errorf("notebook register response does not contain the submitted observing question")
	}
	return entry, err
}

func (c CommandNotebook) Activate(ctx context.Context, req Request, exp *Experiment) (*Entry, error) {
	if exp == nil {
		return nil, fieldErr("payload", "experiment is null")
	}
	if err := exp.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(exp.Identity); err != nil {
		return nil, err
	}
	entry, err := c.entryMutation(ctx, "activate", req, exp)
	if err == nil && (entry.ID != exp.QuestionID || entry.Identity != exp.Identity) {
		return nil, fmt.Errorf("notebook activate response does not identify question %q", exp.QuestionID)
	}
	if err == nil && (!reflect.DeepEqual(entry.Experiment, exp) || entry.ExperimentStatus != exp.Status()) {
		return nil, fmt.Errorf("notebook activate response does not contain the submitted experiment")
	}
	return entry, err
}

func (c CommandNotebook) Record(ctx context.Context, req Request, assessment *Assessment) (*Entry, error) {
	if assessment == nil {
		return nil, fieldErr("payload", "assessment is null")
	}
	if err := assessment.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(assessment.Identity); err != nil {
		return nil, err
	}
	before := &Entry{
		ID: assessment.EntryID(), Identity: assessment.Identity,
		Finding: assessment.Finding, State: StateObserving,
	}
	exactTransition := assessment.Finding != nil
	if assessment.Finding == nil {
		currentRevision, err := c.Revision(ctx)
		if err != nil {
			return nil, err
		}
		if currentRevision == req.Revision {
			before, err = c.Entry(ctx, assessment.QuestionID)
			if err != nil {
				return nil, err
			}
			exactTransition = true
		}
	}
	var expectedState string
	if exactTransition {
		if err := validateAssessmentAgainstEntry(before, assessment); err != nil {
			return nil, err
		}
		var err error
		expectedState, err = TransitionFrom(before.State, assessment.Outcome)
		if err != nil {
			return nil, err
		}
	}
	entry, err := c.entryMutation(ctx, "record", req, assessment)
	if err == nil && (entry.ID != assessment.EntryID() || entry.Identity != assessment.Identity) {
		return nil, fmt.Errorf("notebook record response does not identify entry %q", assessment.EntryID())
	}
	if err == nil {
		found := false
		submitted, _ := json.Marshal(assessment)
		for _, recorded := range entry.Observations {
			candidate, _ := json.Marshal(recorded)
			if bytes.Equal(submitted, candidate) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("notebook record response does not contain the submitted assessment")
		}
		if assessment.Finding != nil && len(entry.InFlight) != 0 {
			return nil, fmt.Errorf("notebook record response opens a finding with in-flight actions")
		}
		if assessment.Finding != nil && !reflect.DeepEqual(entry.Finding, assessment.Finding) {
			return nil, fmt.Errorf("notebook record response finding differs from the submitted finding")
		}
		if err := validateAssessmentAgainstEntry(entry, assessment); err != nil {
			return nil, fmt.Errorf("notebook record response: %w", err)
		}
		if exactTransition && entry.State != expectedState {
			return nil, fmt.Errorf("notebook record response state %q, transition from %q with %q requires %q",
				entry.State, before.State, assessment.Outcome, expectedState)
		}
		if !exactTransition && assessment.Outcome != OutcomeInsufficient && entry.State != StateAnswered {
			return nil, fmt.Errorf("notebook replay response state %q, conclusive assessment requires %q", entry.State, StateAnswered)
		}
		if !exactTransition && entry.State == StateRetired {
			return nil, fmt.Errorf("notebook replay response is retired; retired entries cannot accept observations")
		}
	}
	return entry, err
}

func validateAssessmentAgainstEntry(entry *Entry, assessment *Assessment) error {
	if assessment.Evidence.Subject != entry.subject() {
		return fieldErr("evidence.subject", "%s %s, the entry is about %s %s",
			assessment.Evidence.Subject.Kind, assessment.Evidence.Subject.Instance,
			entry.subject().Kind, entry.subject().Instance)
	}
	return checkRequirements(entry, assessment)
}

func (c CommandNotebook) Claim(ctx context.Context, req Request, action *Action) (*Receipt, error) {
	if action == nil {
		return nil, fieldErr("payload", "action is null")
	}
	if err := action.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(action.Identity); err != nil {
		return nil, err
	}
	var receipt Receipt
	if err := c.call(ctx, mutationRequest("claim", req, action), &receipt); err != nil {
		return nil, err
	}
	if err := receipt.validate(); err != nil {
		return nil, fmt.Errorf("notebook claim response: %w", err)
	}
	if receipt.Identity != action.Identity || receipt.ActionID != action.ID || receipt.Status != ReceiptInFlight {
		return nil, fmt.Errorf("notebook claim response does not identify in-flight action %q", action.ID)
	}
	return &receipt, nil
}

func (c CommandNotebook) Settle(ctx context.Context, req Request, settled *Receipt) (*Receipt, error) {
	if settled == nil {
		return nil, fieldErr("payload", "receipt is null")
	}
	if err := settled.validate(); err != nil {
		return nil, err
	}
	if settled.Status != ReceiptSettled {
		return nil, fieldErr("status", "%q, settling records the outcome of a claim", settled.Status)
	}
	if err := req.validate(settled.Identity); err != nil {
		return nil, err
	}
	var receipt Receipt
	if err := c.call(ctx, mutationRequest("settle", req, settled), &receipt); err != nil {
		return nil, err
	}
	if err := receipt.validate(); err != nil {
		return nil, fmt.Errorf("notebook settle response: %w", err)
	}
	if receipt.Identity != settled.Identity || receipt.ActionID != settled.ActionID || receipt.Status != ReceiptSettled {
		return nil, fmt.Errorf("notebook settle response does not identify settled action %q", settled.ActionID)
	}
	if !reflect.DeepEqual(&receipt, settled) {
		return nil, fmt.Errorf("notebook settle response differs from the submitted receipt")
	}
	return &receipt, nil
}

func (c CommandNotebook) Retire(ctx context.Context, req Request, entryID, reason string) (*Retirement, error) {
	if err := req.validate(req.Identity); err != nil {
		return nil, err
	}
	if err := checkQualified("entry_id", entryID, req.Identity); err != nil {
		return nil, err
	}
	if reason == "" {
		return nil, fieldErr("reason", "required")
	}
	var retirement Retirement
	if err := c.call(ctx, map[string]any{
		"operation": "retire", "request": req, "entry_id": entryID, "reason": reason,
	}, &retirement); err != nil {
		return nil, err
	}
	if err := validateRetirement(&retirement, req.Identity); err != nil {
		return nil, fmt.Errorf("notebook retire response: %w", err)
	}
	if retirement.EntryID != entryID {
		return nil, fmt.Errorf("notebook retire response: entry_id %q, requested %q", retirement.EntryID, entryID)
	}
	if retirement.Reason != reason {
		return nil, fmt.Errorf("notebook retire response reason %q, requested %q", retirement.Reason, reason)
	}
	if retirement.Revision != req.Revision+1 {
		return nil, fmt.Errorf("notebook retire response revision %d, request at %d", retirement.Revision, req.Revision)
	}
	return &retirement, nil
}

func (c CommandNotebook) Reopen(ctx context.Context, req Request, entryID, condition string) (*Entry, error) {
	if err := req.validate(req.Identity); err != nil {
		return nil, err
	}
	if err := checkQualified("entry_id", entryID, req.Identity); err != nil {
		return nil, err
	}
	switch condition {
	case ReopenReactivated, ReopenMethodRevised, ReopenContradicted:
	default:
		return nil, fieldErr("condition", "%q does not authorize a reopen", condition)
	}
	var entry Entry
	if err := c.call(ctx, map[string]any{
		"operation": "reopen", "request": req, "entry_id": entryID, "condition": condition,
	}, &entry); err != nil {
		return nil, err
	}
	if err := validateEntry(&entry); err != nil {
		return nil, fmt.Errorf("notebook reopen response: %w", err)
	}
	if entry.ID != entryID {
		return nil, fmt.Errorf("notebook reopen response: id %q, requested %q", entry.ID, entryID)
	}
	if entry.State != StateObserving {
		return nil, fmt.Errorf("notebook reopen response state %q, want %q", entry.State, StateObserving)
	}
	return &entry, nil
}

func (c CommandNotebook) entryMutation(ctx context.Context, operation string, req Request, payload any) (*Entry, error) {
	var entry Entry
	if err := c.call(ctx, mutationRequest(operation, req, payload), &entry); err != nil {
		return nil, err
	}
	if err := validateEntry(&entry); err != nil {
		return nil, fmt.Errorf("notebook %s response: %w", operation, err)
	}
	return &entry, nil
}

func mutationRequest(operation string, req Request, payload any) map[string]any {
	return map[string]any{"operation": operation, "request": req, "payload": payload}
}

func (c CommandNotebook) call(ctx context.Context, request any, response any) error {
	if len(c.Argv) == 0 || c.Argv[0] == "" {
		return errors.New("notebook command is empty")
	}
	if !filepath.IsAbs(c.Argv[0]) {
		return fmt.Errorf("notebook executable %q is not absolute", c.Argv[0])
	}
	if !filepath.IsAbs(c.Dir) {
		return fmt.Errorf("notebook working directory %q is not absolute", c.Dir)
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	bound, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	input, err := json.Marshal(request)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(bound, c.Argv[0], c.Argv[1:]...)
	cmd.Dir = c.Dir
	cmd.Env = append([]string{}, c.Env...)
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(append(input, '\n'))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedBuffer{Buffer: &stdout, N: maxCommandResponseBytes + 1}
	cmd.Stderr = &limitedBuffer{Buffer: &stderr, N: maxCommandStderrBytes}
	err = cmd.Run()
	if bound.Err() != nil {
		return fmt.Errorf("notebook command: %w", bound.Err())
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if sentinel := refusalForExit(exit.ExitCode()); sentinel != nil {
				if strings.TrimSpace(stdout.String()) != "" {
					return fmt.Errorf("notebook refusal exit %d wrote stdout", exit.ExitCode())
				}
				return sentinel
			}
		}
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return fmt.Errorf("notebook command: %w: %s", err, detail)
		}
		return fmt.Errorf("notebook command: %w", err)
	}
	if stdout.Len() > maxCommandResponseBytes {
		return fmt.Errorf("notebook response exceeds %d bytes", maxCommandResponseBytes)
	}
	return decodeCommandResponse(stdout.Bytes(), response)
}

func decodeCommandResponse(data []byte, response any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(response); err != nil {
		return fmt.Errorf("notebook response: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("notebook response contains multiple JSON values")
		}
		return fmt.Errorf("notebook response trailing content: %w", err)
	}
	return nil
}

func refusalForExit(code int) error {
	switch code {
	case 10:
		return ErrStaleRevision
	case 11:
		return ErrRetired
	case 12:
		return ErrNoEntry
	case 13:
		return ErrNotClaimed
	case 14:
		return ErrRequestConflict
	default:
		return nil
	}
}

type limitedBuffer struct {
	*bytes.Buffer
	N int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	if b.N > 0 {
		if len(p) > b.N {
			p = p[:b.N]
		}
		b.N -= len(p)
		_, _ = b.Buffer.Write(p)
	}
	return original, nil
}
