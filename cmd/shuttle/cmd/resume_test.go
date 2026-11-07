package cmd

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"shuttle/internal/ledger"
	"shuttle/internal/schedule"
)

// Pause and resume write the ledger from their own process, so the host event
// a connected client hears has to be written by the command itself.
func TestPauseAndResumeCommandsWriteTheHostEvent(t *testing.T) {
	shuttleEnv(t)
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	for _, verb := range []string{"pause", "resume"} {
		rootCmd.SetArgs([]string{verb, ledger.WeftAgent})
		if err := rootCmd.Execute(); err != nil {
			t.Fatalf("%s: %v", verb, err)
		}
	}
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	events, err := store.WeftEventsAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	var dispatch []string
	for _, e := range events {
		if e.Type == "host" {
			var doc struct {
				Dispatch string `json:"dispatch"`
			}
			if err := json.Unmarshal(e.Payload, &doc); err != nil {
				t.Fatal(err)
			}
			dispatch = append(dispatch, doc.Dispatch)
		}
	}
	if !reflect.DeepEqual(dispatch, []string{"paused", "open"}) {
		t.Fatalf("host events = %v", dispatch)
	}
}

// Past the barrier the update cannot be abandoned, so the operator is told the
// resume is taken and when it lands, rather than left reading a pause that did
// not lift.
func TestResumeCommandSaysWhenAnUpdateHoldsThePause(t *testing.T) {
	shuttleEnv(t)
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if claimed, _, err := store.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("pause = %t/%v", claimed, err)
	}
	if err := store.ArmRestartBarrier(time.Hour); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"resume", ledger.WeftAgent})
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetArgs(nil)
	})
	if err := rootCmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "resumes when the update ends") {
		t.Fatalf("stdout = %q", out.String())
	}
	if a, err := store.Agent(ledger.WeftAgent); err != nil || !a.Paused || a.PausedReason.String != ledger.PauseUpdate {
		t.Fatalf("agent = %+v/%v", a, err)
	}
}

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
