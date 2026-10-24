package ledger

import "testing"

func TestCompletionEventRecordsRecoveryOnce(t *testing.T) {
	s := newStore(t)
	failNth(t, s, 1, ClassDied)
	run := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	event, err := s.CompleteEvent(run.ID, Result{})
	if err != nil || !event.Recovered {
		t.Fatalf("recovery: %+v/%v", event, err)
	}
	a, err := s.Agent(run.Agent)
	if err != nil || a.FailureStreak != 0 || !a.LastSuccessAt.Valid {
		t.Fatalf("recovery state: %+v/%v", a, err)
	}
	run = claim(t, s, "daily", "2026-09-03T04:30:00-07:00")
	event, err = s.CompleteEvent(run.ID, Result{})
	if err != nil || event.Recovered {
		t.Fatalf("second success: %+v/%v", event, err)
	}
	if ev, err := s.CompleteEvent(run.ID, Result{}); err == nil || ev.Recovered {
		t.Fatalf("replayed terminal write: %+v/%v", ev, err)
	}
}

func TestReconciledLeavesTheFailureHistory(t *testing.T) {
	s := newStore(t)
	var events []Event
	s.OnTerminal = func(event Event) { events = append(events, event) }
	var last string
	for n := 1; n <= maxFailureStreak; n++ {
		last = failNth(t, s, n, ClassOutput)
	}
	before, err := s.Get(last)
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := s.OpenIncidents()
	if err != nil || len(incidents) != 1 {
		t.Fatalf("incidents = %d, %v", len(incidents), err)
	}
	events = nil
	if err := s.Reconciled(last, true); err != nil {
		t.Fatalf("reconciled: %v", err)
	}
	row, err := s.Get(last)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != StatusCompleted || row.FailureClass != before.FailureClass || row.ErrorSignature != before.ErrorSignature ||
		!row.Silent || row.EndedAt != before.EndedAt {
		t.Fatalf("row = %+v, want completed with its failure and ended_at kept", row)
	}
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatal(err)
	}
	if agent.FailureStreak != maxFailureStreak || !agent.Paused || agent.PausedReason.String != PauseStreak || agent.LastSuccessAt.Valid {
		t.Fatalf("agent = %+v, want the streak, the pause and no success left as the failures made them", agent)
	}
	after, err := s.OpenIncidents()
	if err != nil || len(after) != 1 || after[0].Count != incidents[0].Count || after[0].LastRunID != incidents[0].LastRunID {
		t.Fatalf("incidents = %+v, %v; want the open incident untouched", after, err)
	}
	if len(events) != 0 {
		t.Fatalf("events = %+v, want none", events)
	}
	if err := s.Reconciled(last, true); err == nil {
		t.Fatal("reconciling a completed run succeeded, want a refusal")
	}
	claimed := claim(t, s, "daily", "2026-09-20T04:30:00-07:00")
	if err := s.Reconciled(claimed.ID, false); err == nil {
		t.Fatal("reconciling a claimed run succeeded, want a refusal")
	}
	// The reconciled run is the agent's latest terminal row; the next real
	// success is still the recovery from it.
	event, err := s.CompleteEvent(claimed.ID, Result{})
	if err != nil || !event.Recovered {
		t.Fatalf("next success = %+v, %v; want it recorded as the recovery", event, err)
	}
}
