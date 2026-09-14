package runner

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
)

func TestConfiguredTerminalOutcomesNotifyAndPersistCooldown(t *testing.T) {
	home := t.TempDir()
	cfg := &config.Config{Notify: config.Notify{Enabled: true, CooldownMinutes: 60}}
	store, err := ledger.Open(filepath.Join(home, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var logs bytes.Buffer
	logger := log.New(&logs, "", 0)
	sent := []string{}
	send := func(_ context.Context, title, body string) error { sent = append(sent, body); return nil }
	notifier := ConfigureOutcomes(store, cfg, home, logger)
	notifier.Send = send
	slot := time.Now()
	claim := func() string {
		t.Helper()
		slot = slot.Add(time.Minute)
		r, ok, err := store.Claim("daily", slot, ledger.TriggerCron)
		if err != nil || !ok {
			t.Fatalf("claim: %v", err)
		}
		return r.ID
	}
	fail := func() {
		t.Helper()
		if err := store.Fail(claim(), ledger.ClassDied, "exit 1", ledger.Result{}); err != nil {
			t.Fatal(err)
		}
	}
	fail()
	fail()
	incidents, err := store.OpenIncidents()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(incidents[0].ID); err != nil {
		t.Fatal(err)
	}
	// The same production setup reloads persisted cooldown rather than keeping
	// it only in the first notifier's process memory.
	notifier = ConfigureOutcomes(store, cfg, home, logger)
	notifier.Send = send
	fail()
	if len(sent) != 1 {
		t.Fatalf("repeat or recurrence bypassed configured cooldown: %v", sent)
	}
	if err := store.Complete(claim(), ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(claim(), ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || !strings.Contains(sent[1], "recovered") {
		t.Fatalf("recovery notifications: %v", sent)
	}
	if _, err := os.Stat(filepath.Join(home, "notify.state")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "notification=true") || !strings.Contains(logs.String(), "recovery=true") {
		t.Fatalf("missing diagnostic events: %s", &logs)
	}
	cfg.Notify.Enabled = false
	notifier = ConfigureOutcomes(store, cfg, home, logger)
	notifier.Send = send
	fail()
	if len(sent) != 2 {
		t.Fatalf("disabled notification delivered: %v", sent)
	}
}
