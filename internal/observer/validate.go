package observer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// maxPayloadBytes caps one payload. A bundle carries prose readings and a
// baseline, so 64 KiB is generous for the shape; the point of the cap is that
// payloads arrive from a domain adapter the harness does not control, and a
// bounded read is what keeps a malformed one an error rather than an
// out-of-memory in the harness.
const maxPayloadBytes = 64 << 10

// Error is a contract validation failure. Callers match it with errors.As, and
// the message always names the field at fault: a refusal that says only
// "invalid" leaves the producer guessing which of thirty fields to fix.
type Error struct {
	Field string
	Err   error
}

func (e *Error) Error() string { return fmt.Sprintf("observer %s: %v", e.Field, e.Err) }

func (e *Error) Unwrap() error { return e.Err }

func fieldErr(field, format string, args ...any) *Error {
	return &Error{Field: field, Err: fmt.Errorf(format, args...)}
}

// wrapField re-roots a nested refusal under the field that carried it, so an
// evidence bundle inside an assessment names evidence.window.start rather than
// window.start.
func wrapField(prefix string, err error) error {
	var ferr *Error
	if errors.As(err, &ferr) {
		return &Error{Field: prefix + "." + ferr.Field, Err: ferr.Err}
	}
	return err
}

// ValidateQuestion decodes and checks a standing question.
func ValidateQuestion(data []byte) (*Question, error) {
	var q Question
	if err := decode(data, &q); err != nil {
		return nil, err
	}
	if err := q.validate(); err != nil {
		return nil, err
	}
	return &q, nil
}

// ValidateExperiment decodes and checks an experiment registration. It returns
// the experiment whether it is live or pending: pending is a state, not a
// failure, and only a missing declaration is refused.
func ValidateExperiment(data []byte) (*Experiment, error) {
	var e Experiment
	if err := decode(data, &e); err != nil {
		return nil, err
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	return &e, nil
}

// ValidateEvidence decodes and checks an evidence bundle.
func ValidateEvidence(data []byte) (*Bundle, error) {
	var b Bundle
	if err := decode(data, &b); err != nil {
		return nil, err
	}
	if err := b.validate(); err != nil {
		return nil, err
	}
	return &b, nil
}

// ValidateAssessment decodes and checks an assessment and the bundle it cites.
func ValidateAssessment(data []byte) (*Assessment, error) {
	var a Assessment
	if err := decode(data, &a); err != nil {
		return nil, err
	}
	if err := a.validate(); err != nil {
		return nil, err
	}
	return &a, nil
}

// ValidateAction decodes and checks a proposed action.
func ValidateAction(data []byte) (*Action, error) {
	var a Action
	if err := decode(data, &a); err != nil {
		return nil, err
	}
	if err := a.validate(); err != nil {
		return nil, err
	}
	return &a, nil
}

// ValidateReceipt decodes and checks an action receipt.
func ValidateReceipt(data []byte) (*Receipt, error) {
	var r Receipt
	if err := decode(data, &r); err != nil {
		return nil, err
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	return &r, nil
}

// validateEntry checks a successful adapter response before the runner retains
// or prompts from it. Entries are notebook snapshots rather than model output,
// but they cross the same untrusted process boundary as every payload.
func validateEntry(e *Entry) error {
	if err := e.Identity.validate("identity"); err != nil {
		return err
	}
	if err := checkQualified("id", e.ID, e.Identity); err != nil {
		return err
	}
	if err := validateState("state", e.State); err != nil {
		return err
	}
	if (e.Question == nil) == (e.Finding == nil) {
		return fieldErr("question", "entry must carry exactly one of question or finding")
	}
	if e.Question != nil {
		if err := e.Question.validate(); err != nil {
			return wrapField("question", err)
		}
		if e.Question.ID != e.ID || e.Question.Identity != e.Identity {
			return fieldErr("question.id", "does not identify entry %q", e.ID)
		}
	} else {
		if err := checkQualified("finding.id", e.Finding.ID, e.Identity); err != nil {
			return err
		}
		if e.Finding.ID != e.ID {
			return fieldErr("finding.id", "%q does not identify entry %q", e.Finding.ID, e.ID)
		}
		if err := e.Finding.Subject.validate("finding.subject"); err != nil {
			return err
		}
		if e.Finding.Question == "" || e.Finding.Changes == "" {
			return fieldErr("finding", "question and changes are required")
		}
	}
	if e.Experiment != nil {
		if err := e.Experiment.validate(); err != nil {
			return wrapField("experiment", err)
		}
		if e.Experiment.QuestionID != e.ID || e.Experiment.Identity != e.Identity {
			return fieldErr("experiment.question_id", "does not identify entry %q", e.ID)
		}
		if e.ExperimentStatus != e.Experiment.Status() {
			return fieldErr("experiment_status", "%q does not match experiment status %q", e.ExperimentStatus, e.Experiment.Status())
		}
	} else if e.ExperimentStatus != "" {
		return fieldErr("experiment_status", "%q without an experiment", e.ExperimentStatus)
	}
	for i, a := range e.Observations {
		if a == nil {
			return fieldErr(fmt.Sprintf("observations[%d]", i), "null")
		}
		if err := a.validate(); err != nil {
			return wrapField(fmt.Sprintf("observations[%d]", i), err)
		}
		if a.Identity != e.Identity || a.EntryID() != e.ID {
			return fieldErr(fmt.Sprintf("observations[%d]", i), "does not belong to entry %q", e.ID)
		}
		if a.Evidence.Subject != e.subject() {
			return fieldErr(fmt.Sprintf("observations[%d].evidence.subject", i),
				"%s %s, the entry is about %s %s",
				a.Evidence.Subject.Kind, a.Evidence.Subject.Instance,
				e.subject().Kind, e.subject().Instance)
		}
	}
	inFlight := make(map[string]bool, len(e.InFlight))
	for i, id := range e.InFlight {
		if err := checkQualified(fmt.Sprintf("in_flight[%d]", i), id, e.Identity); err != nil {
			return err
		}
		if inFlight[id] {
			return fieldErr(fmt.Sprintf("in_flight[%d]", i), "%q appears twice", id)
		}
		inFlight[id] = true
	}
	if e.State == StateRetired {
		if e.Retirement == nil {
			return fieldErr("retirement", "required for a retired entry")
		}
		if err := validateRetirement(e.Retirement, e.Identity); err != nil {
			return err
		}
		if e.Retirement.EntryID != e.ID {
			return fieldErr("retirement.entry_id", "%q does not identify entry %q", e.Retirement.EntryID, e.ID)
		}
	} else if e.Retirement != nil {
		return fieldErr("retirement", "present while state is %q", e.State)
	}
	return nil
}

func validateRetirement(r *Retirement, identity Identity) error {
	if r == nil {
		return fieldErr("retirement", "null")
	}
	if err := checkQualified("retirement.entry_id", r.EntryID, identity); err != nil {
		return err
	}
	if r.Reason == "" {
		return fieldErr("retirement.reason", "required")
	}
	if err := checkTimestamp("retirement.at", r.At); err != nil {
		return err
	}
	if r.Revision <= 0 {
		return fieldErr("retirement.revision", "%d, must be positive", r.Revision)
	}
	seen := make(map[string]bool, len(r.InFlight))
	for i, id := range r.InFlight {
		if err := checkQualified(fmt.Sprintf("retirement.in_flight[%d]", i), id, identity); err != nil {
			return err
		}
		if seen[id] {
			return fieldErr(fmt.Sprintf("retirement.in_flight[%d]", i), "%q appears twice", id)
		}
		seen[id] = true
	}
	return nil
}

// decode reads one payload strictly: an unknown key is refused rather than
// ignored (config.Load's rule), so a field this binary does not understand can
// never be silently dropped from a record that is never rewritten.
func decode(data []byte, v any) error {
	if len(data) > maxPayloadBytes {
		return fieldErr("payload", "%d bytes, at most %d", len(data), maxPayloadBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return &Error{Field: "payload", Err: err}
	}
	// Decode reads one JSON value; anything after it would otherwise slip past
	// the strict decode above.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fieldErr("payload", "trailing content after the object")
	}
	return nil
}
