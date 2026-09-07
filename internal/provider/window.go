package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	// WindowPreviousDay is the only kind phase 1 resolves. previous-week,
	// previous-month and previous-year are §16's roll-up seam.
	WindowPreviousDay = "previous-day"

	labelLayout = "2006-01-02"
)

// Window is the span a provider reports over: half-open, [From, To). Label is
// the day the report is about, which §7.3's contract and §9.2's archive file
// are both named after.
type Window struct {
	From  time.Time
	To    time.Time
	Label string
}

// Resolve derives the window from the slot (§8.1). now is deliberately not an
// input: a catch-up run at 23:00 for a missed 04:30 slot has to report the same
// day the slot would have.
func Resolve(kind string, slot time.Time, loc *time.Location) (Window, error) {
	if kind != WindowPreviousDay {
		return Window{}, fmt.Errorf("window %q: only %q resolves in phase 1", kind, WindowPreviousDay)
	}
	local := slot.In(loc)
	y, m, d := local.Date()
	// time.Date in the agent's zone rather than a 24-hour subtraction, so a DST
	// day is the 23 or 25 hours it actually was.
	to := time.Date(y, m, d, 0, 0, 0, 0, loc)
	from := time.Date(y, m, d-1, 0, 0, 0, 0, loc)
	return Window{From: from, To: to, Label: from.Format(labelLayout)}, nil
}

// Contains applies the half-open rule: an instant at To belongs to the next
// window, not this one.
func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.From) && t.Before(w.To)
}

// MarshalJSON renders the bounds in the agent's zone, so the document reads as
// the wall clock the day was lived in rather than as UTC.
func (w Window) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		From  string `json:"from"`
		To    string `json:"to"`
		Label string `json:"label"`
	}{w.From.Format(time.RFC3339), w.To.Format(time.RFC3339), w.Label})
}

// WindowArg reads the window kind from a provider's manifest args.
func WindowArg(args map[string]any) (string, error) {
	kind, ok := args["window"].(string)
	if !ok || kind == "" {
		return "", errors.New("args.window: a window kind is required")
	}
	return kind, nil
}
