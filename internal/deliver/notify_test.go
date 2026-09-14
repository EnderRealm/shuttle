package deliver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/ledger"
)

func TestIncidentNotificationCooldownAckAndRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := ledger.Open(filepath.Join(dir, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now })
	var sent []string
	send := func(_ context.Context, title, body string) error { sent = append(sent, body); return nil }
	notifier := &Notifier{StatePath: filepath.Join(dir, "notify.state"), Enabled: true, Cooldown: time.Hour, Now: func() time.Time { return now }, Send: send}
	claim := func() string {
		t.Helper()
		now = now.Add(time.Minute)
		r, ok, e := store.Claim("daily", now, ledger.TriggerManual)
		if e != nil || !ok {
			t.Fatalf("claim: %v", e)
		}
		return r.ID
	}
	fail := func() ledger.Event {
		t.Helper()
		ev, e := store.FailEvent(claim(), ledger.ClassDied, "exit 137 at /run/1", ledger.Result{})
		if e != nil {
			t.Fatal(e)
		}
		return ev
	}
	notify := func(ev ledger.Event, want bool) {
		t.Helper()
		got, e := notifier.Notify(context.Background(), ev)
		if e != nil || got != want {
			t.Fatalf("notify=%t/%v, want %t; %+v", got, e, want, ev)
		}
	}
	first := fail()
	notify(first, true)
	notify(first, false)
	notify(fail(), false)
	if err := store.Ack(first.IncidentID); err != nil {
		t.Fatal(err)
	}
	recurrence := fail()
	if !recurrence.NewIncident || recurrence.IncidentID == first.IncidentID {
		t.Fatalf("recurrence: %+v", recurrence)
	}
	// A fresh notifier/process reloads the cooldown from disk.
	notifier = &Notifier{StatePath: notifier.StatePath, Enabled: true, Cooldown: time.Hour, Now: func() time.Time { return now }, Send: send}
	notify(recurrence, false)
	now = now.Add(time.Hour)
	if err := store.Ack(recurrence.IncidentID); err != nil {
		t.Fatal(err)
	}
	notify(fail(), true)
	fifth := fail()
	if fifth.PausedReason != ledger.PauseStreak {
		t.Fatalf("fifth: %+v", fifth)
	}
	notify(fifth, true)
	if err := store.Resume("daily"); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.CompleteEvent(claim(), ledger.Result{})
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.Recovered {
		t.Fatal("recovery lost across resume")
	}
	notify(recovered, true)
	notify(recovered, false)
	healthy, err := store.CompleteEvent(claim(), ledger.Result{})
	if err != nil {
		t.Fatal(err)
	}
	notify(healthy, false)
	if len(sent) != 4 || !strings.Contains(sent[2], "Paused for streak") || !strings.Contains(sent[3], "recovered") {
		t.Fatalf("sent: %v", sent)
	}
}

func TestNotificationFailureDoesNotConsumeCooldown(t *testing.T) {
	n := &Notifier{StatePath: filepath.Join(t.TempDir(), "notify.state"), Enabled: true, Cooldown: time.Hour, Send: func(context.Context, string, string) error { return fmt.Errorf("delivery failed") }}
	ev := ledger.Event{Agent: "daily", RunID: "run", Class: ledger.ClassAuth, NewIncident: true}
	if sent, err := n.Notify(context.Background(), ev); sent || err == nil {
		t.Fatalf("notify: %t/%v", sent, err)
	}
	n.Send = func(context.Context, string, string) error { return nil }
	if sent, err := n.Notify(context.Background(), ev); !sent || err != nil {
		t.Fatalf("retry: %t/%v", sent, err)
	}
}

func TestLiveNotifications(t *testing.T) {
	if os.Getenv("SHUTTLE_LIVE_NOTIFY") != "1" {
		t.Skip("set SHUTTLE_LIVE_NOTIFY=1 to observe macOS failure/recovery notifications")
	}
	n := &Notifier{StatePath: filepath.Join(t.TempDir(), "notify.state"), Enabled: true, Cooldown: time.Hour}
	ev := ledger.Event{Agent: "notification-check", RunID: "local-check", Class: ledger.ClassPreflight, Signature: "preflight: notification delivery check", IncidentID: 1, NewIncident: true}
	if sent, err := n.Notify(context.Background(), ev); !sent || err != nil {
		t.Fatalf("failure: %t/%v", sent, err)
	}
	if sent, err := n.Notify(context.Background(), ev); sent || err != nil {
		t.Fatalf("repeat: %t/%v", sent, err)
	}
	time.Sleep(3 * time.Second)
	ev = ledger.Event{Agent: ev.Agent, RunID: "local-recovery-check", Recovered: true}
	if sent, err := n.Notify(context.Background(), ev); !sent || err != nil {
		t.Fatalf("recovery: %t/%v", sent, err)
	}
}
