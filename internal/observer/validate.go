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
