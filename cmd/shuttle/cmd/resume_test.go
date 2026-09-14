package cmd

import (
	"testing"
	"time"

	"shuttle/internal/ledger"
	"shuttle/internal/schedule"
)

func TestResumeCommandAllowsNextSlotAfterFiveFailures(t *testing.T) {
	shuttleEnv(t)
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	sched := &schedule.Scheduler{Ledger: store, Now: func() time.Time { return now }}
	for i := 0; i < 5; i++ {
		now = now.Add(time.Hour)
		r, reason, err := sched.Claim("daily", now, ledger.TriggerCron)
		if err != nil || reason != "" || r == nil {
			t.Fatalf("claim: %s/%v", reason, err)
		}
		if err := store.Fail(r.ID, ledger.ClassDied, "exit 1", ledger.Result{}); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Hour)
	if r, reason, err := sched.Claim("daily", now, ledger.TriggerCron); r != nil || reason != ledger.SkipPaused || err != nil {
		t.Fatalf("paused slot: %+v/%s/%v", r, reason, err)
	}
	rootCmd.SetArgs([]string{"resume", "daily"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	if err := rootCmd.Execute(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	r, reason, err := sched.Claim("daily", now, ledger.TriggerCron)
	if err != nil || reason != "" || r == nil {
		t.Fatalf("resumed claim: %s/%v", reason, err)
	}
	event, err := store.CompleteEvent(r.ID, ledger.Result{})
	if err != nil || !event.Recovered {
		t.Fatalf("recovery after paused slot/resume: %+v/%v", event, err)
	}
}
