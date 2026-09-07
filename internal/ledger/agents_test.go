package ledger

import (
	"fmt"
	"testing"
)

// failNth claims the nth daily slot and fails it with the given class.
func failNth(t *testing.T, s *Store, n int, class string) string {
	t.Helper()
	run := claim(t, s, "daily", fmt.Sprintf("2026-09-%02dT04:30:00-07:00", n))
	if err := s.Fail(run.ID, class, "the runtime exited 1", Result{}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	return run.ID
}

func TestFailureStreakPausesAtFive(t *testing.T) {
	s := newStore(t)
	for n := 1; n <= 5; n++ {
		failNth(t, s, n, ClassDied)
		agent, err := s.Agent("daily")
		if err != nil {
			t.Fatalf("agent: %v", err)
		}
		if agent.FailureStreak != n {
			t.Fatalf("after %d failures streak = %d", n, agent.FailureStreak)
		}
		wantPaused := n >= maxFailureStreak
		if agent.Paused != wantPaused {
			t.Fatalf("after %d failures paused = %t, want %t", n, agent.Paused, wantPaused)
		}
		if wantPaused {
			if agent.PausedReason.String != PauseStreak {
				t.Fatalf("paused_reason = %q, want %q", agent.PausedReason.String, PauseStreak)
			}
			if !agent.PausedAt.Valid {
				t.Fatal("paused_at unset on an auto-paused agent")
			}
		}
	}
}

func TestCompletionResetsStreak(t *testing.T) {
	s := newStore(t)
	failNth(t, s, 1, ClassDied)
	failNth(t, s, 2, ClassProvider)
	run := claim(t, s, "daily", "2026-09-03T04:30:00-07:00")
	if err := s.Complete(run.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if agent.FailureStreak != 0 {
		t.Fatalf("streak = %d after a completion, want 0", agent.FailureStreak)
	}
	if !agent.LastSuccessAt.Valid {
		t.Fatal("last_success_at unset after a completion")
	}
}

func TestUnknownAndSkippedLeaveTheStreakAlone(t *testing.T) {
	s := newStore(t)
	failNth(t, s, 1, ClassDied)

	// A restart over an in-flight run, and a slot that was never launched:
	// neither is evidence the agent is broken (§6.2).
	inFlight := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	if err := s.MarkUnknown(inFlight.ID); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}
	if _, _, err := s.Skip("daily", slot(t, "2026-09-03T04:30:00-07:00"), TriggerCron, SkipOverlap); err != nil {
		t.Fatalf("skip: %v", err)
	}

	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if agent.FailureStreak != 1 {
		t.Fatalf("streak = %d, want the one failure only", agent.FailureStreak)
	}
	if agent.Paused {
		t.Fatal("agent paused by unknown or skipped rows")
	}
}

func TestAuthFailurePausesOnTheFirstOccurrence(t *testing.T) {
	s := newStore(t)
	failNth(t, s, 1, ClassAuth)
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if !agent.Paused || agent.PausedReason.String != PauseAuth {
		t.Fatalf("paused = %t/%q, want a pause with reason %q", agent.Paused, agent.PausedReason.String, PauseAuth)
	}
}

func TestResumeClearsThePauseAndTheStreak(t *testing.T) {
	s := newStore(t)
	for n := 1; n <= 5; n++ {
		failNth(t, s, n, ClassDied)
	}
	if err := s.Resume("daily"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if agent.Paused || agent.PausedReason.Valid || agent.PausedAt.Valid || agent.FailureStreak != 0 {
		t.Fatalf("after resume: %+v", agent)
	}
	if err := s.Resume("nobody"); err == nil {
		t.Fatal("resuming an unknown agent succeeded, want an error naming it")
	}
}

func TestAgentWithNoHistoryReadsAsZero(t *testing.T) {
	s := newStore(t)
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if agent.Paused || agent.FailureStreak != 0 || agent.LastFiredSlot.Valid {
		t.Fatalf("unfired agent = %+v, want a zero row", agent)
	}
}
