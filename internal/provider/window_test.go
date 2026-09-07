package provider

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// The window is a function of the slot alone (§8.1), which is what makes a
// catch-up run report the day it was scheduled for rather than the day it ran.
func TestResolvePreviousDay(t *testing.T) {
	la := mustLoad(t, "America/Los_Angeles")
	utc := time.UTC
	cases := []struct {
		name     string
		slot     time.Time
		loc      *time.Location
		from, to string
		label    string
		hours    float64
	}{
		{
			name:  "the daily agent's slot",
			slot:  time.Date(2026, 9, 2, 4, 30, 0, 0, la),
			loc:   la,
			from:  "2026-09-01T00:00:00-07:00",
			to:    "2026-09-02T00:00:00-07:00",
			label: "2026-09-01",
			hours: 24,
		},
		{
			// The same slot instant, resolved as a catch-up run would at 23:00
			// on 2026-09-02: Resolve takes no clock, so the result is identical.
			name:  "the missed slot, caught up at 23:00",
			slot:  time.Date(2026, 9, 2, 4, 30, 0, 0, la),
			loc:   la,
			from:  "2026-09-01T00:00:00-07:00",
			to:    "2026-09-02T00:00:00-07:00",
			label: "2026-09-01",
			hours: 24,
		},
		{
			name:  "a slot at midnight belongs to its own day",
			slot:  time.Date(2026, 9, 2, 0, 0, 0, 0, la),
			loc:   la,
			from:  "2026-09-01T00:00:00-07:00",
			to:    "2026-09-02T00:00:00-07:00",
			label: "2026-09-01",
			hours: 24,
		},
		{
			name:  "the fall-back day is 25 hours",
			slot:  time.Date(2025, 11, 3, 4, 30, 0, 0, la),
			loc:   la,
			from:  "2025-11-02T00:00:00-07:00",
			to:    "2025-11-03T00:00:00-08:00",
			label: "2025-11-02",
			hours: 25,
		},
		{
			name:  "the day before spring forward is 24 hours",
			slot:  time.Date(2026, 3, 8, 4, 30, 0, 0, la),
			loc:   la,
			from:  "2026-03-07T00:00:00-08:00",
			to:    "2026-03-08T00:00:00-08:00",
			label: "2026-03-07",
			hours: 24,
		},
		{
			name:  "the spring-forward day is 23 hours",
			slot:  time.Date(2026, 3, 9, 4, 30, 0, 0, la),
			loc:   la,
			from:  "2026-03-08T00:00:00-08:00",
			to:    "2026-03-09T00:00:00-07:00",
			label: "2026-03-08",
			hours: 23,
		},
		{
			// A slot recorded in one zone resolves in the agent's, so the day
			// boundary is the agent's day boundary.
			name:  "a slot from another zone resolves in the agent's",
			slot:  time.Date(2026, 9, 2, 11, 30, 0, 0, utc),
			loc:   la,
			from:  "2026-09-01T00:00:00-07:00",
			to:    "2026-09-02T00:00:00-07:00",
			label: "2026-09-01",
			hours: 24,
		},
		{
			name:  "the first of the month reaches back into the last",
			slot:  time.Date(2026, 9, 1, 4, 30, 0, 0, la),
			loc:   la,
			from:  "2026-08-31T00:00:00-07:00",
			to:    "2026-09-01T00:00:00-07:00",
			label: "2026-08-31",
			hours: 24,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w, err := Resolve(WindowPreviousDay, c.slot, c.loc)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got := w.From.Format(time.RFC3339); got != c.from {
				t.Errorf("from = %s, want %s", got, c.from)
			}
			if got := w.To.Format(time.RFC3339); got != c.to {
				t.Errorf("to = %s, want %s", got, c.to)
			}
			if w.Label != c.label {
				t.Errorf("label = %s, want %s", w.Label, c.label)
			}
			if got := w.To.Sub(w.From).Hours(); got != c.hours {
				t.Errorf("span = %vh, want %vh", got, c.hours)
			}
		})
	}
}

func TestResolveRejectsAnUnknownKind(t *testing.T) {
	la := mustLoad(t, "America/Los_Angeles")
	// The roll-up windows are §16's seam: named in the error rather than
	// silently resolved as a day.
	_, err := Resolve("previous-week", time.Date(2026, 9, 2, 4, 30, 0, 0, la), la)
	if err == nil || !strings.Contains(err.Error(), "previous-week") {
		t.Fatalf("err = %v, want one naming the kind", err)
	}
}

func TestWindowContainsIsHalfOpen(t *testing.T) {
	la := mustLoad(t, "America/Los_Angeles")
	w, err := Resolve(WindowPreviousDay, time.Date(2026, 9, 2, 4, 30, 0, 0, la), la)
	if err != nil {
		t.Fatal(err)
	}
	if !w.Contains(w.From) {
		t.Error("From is outside the window")
	}
	if w.Contains(w.To) {
		t.Error("To is inside the window")
	}
	if w.Contains(w.From.Add(-time.Nanosecond)) {
		t.Error("an instant before From is inside the window")
	}
}

func TestWindowMarshalsInTheAgentsZone(t *testing.T) {
	la := mustLoad(t, "America/Los_Angeles")
	w, err := Resolve(WindowPreviousDay, time.Date(2026, 9, 2, 4, 30, 0, 0, la), la)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"from":"2026-09-01T00:00:00-07:00","to":"2026-09-02T00:00:00-07:00","label":"2026-09-01"}`
	if string(data) != want {
		t.Fatalf("json = %s, want %s", data, want)
	}
}

func TestWindowArg(t *testing.T) {
	kind, err := WindowArg(map[string]any{"window": WindowPreviousDay})
	if err != nil || kind != WindowPreviousDay {
		t.Fatalf("WindowArg = (%q, %v), want the kind", kind, err)
	}
	if _, err := WindowArg(map[string]any{"agent": "daily"}); err == nil {
		t.Fatal("WindowArg over args without a window: want an error")
	}
}
