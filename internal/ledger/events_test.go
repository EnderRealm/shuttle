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
