package runner

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/deliver"
	"shuttle/internal/ledger"
)

const senderTestToken = "cf-runner-secret-token"

// liveSender is the production adapter over the real client, pointed at a
// local server answering every request with status and body.
func liveSender(t *testing.T, f *liveFixture, status int, body string) (*bytes.Buffer, *int) {
	t.Helper()
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	client := deliver.NewCloudflare(f.pipeline.Config)
	client.BaseURL = srv.URL
	client.Token = func() (string, error) { return senderTestToken, nil }
	client.Wait = func(context.Context, time.Duration) error { return nil }
	var logs bytes.Buffer
	f.pipeline.Sender = &cloudflareSender{client: client, logger: log.New(&logs, "", 0)}
	return &logs, &requests
}

func TestDeliveredRunRecordsDeliveredAt(t *testing.T) {
	f := newLiveFixture(t, "success")
	logs, requests := liveSender(t, f, http.StatusOK,
		`{"success":true,"errors":[],"messages":[],"result":{"delivered":["to@example.test"],"queued":[],"permanent_bounces":[]}}`)
	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{}); err != nil {
		t.Fatal(err)
	}
	row, _ := f.store.Get(run.ID)
	if row.Status != ledger.StatusCompleted || row.DeliveryStatus.String != ledger.DeliveryDelivered ||
		!row.DeliveredAt.Valid || row.DeliveryError.Valid {
		t.Fatalf("row = %+v", row)
	}
	if *requests != 1 || !strings.Contains(logs.String(), "run="+run.ID) || !strings.Contains(logs.String(), "http=200 attempts=1") {
		t.Fatalf("requests = %d, log = %q", *requests, logs)
	}
}

func TestDeliveryAuthFailureOpensAnIncidentAndLeavesTheRunCompleted(t *testing.T) {
	f := newLiveFixture(t, "success")
	logs, requests := liveSender(t, f, http.StatusUnauthorized,
		`{"success":false,"errors":[{"code":10000,"message":"Authentication error"}],"messages":[],"result":null}`)
	var notes []string
	notifier := &deliver.Notifier{
		StatePath: filepath.Join(f.root, "notify.state"), Enabled: true, Cooldown: time.Hour,
		Send: func(_ context.Context, _, body string) error { notes = append(notes, body); return nil },
	}
	var events []ledger.Event
	f.store.OnTerminal = func(event ledger.Event) {
		events = append(events, event)
		if _, err := notifier.Notify(context.Background(), event); err != nil {
			t.Errorf("notify: %v", err)
		}
	}
	run := f.claim(t, ledger.TriggerManual)
	_, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("run error = %v, want the 401", err)
	}
	if *requests != 1 {
		t.Fatalf("requests = %d, want no retry after a 401", *requests)
	}
	row, _ := f.store.Get(run.ID)
	if row.Status != ledger.StatusCompleted || row.DeliveryStatus.String != ledger.DeliveryFailed ||
		!strings.Contains(row.DeliveryError.String, "401") || row.DeliveredAt.Valid {
		t.Fatalf("row = %+v", row)
	}
	agent, err := f.store.Agent(f.def.Name)
	if err != nil || agent.FailureStreak != 0 || agent.Paused {
		t.Fatalf("agent = %+v, %v; a delivery failure is not an agent failure", agent, err)
	}
	incidents, err := f.store.OpenIncidents()
	if err != nil || len(incidents) != 1 || incidents[0].Agent != f.def.Name ||
		!strings.HasPrefix(incidents[0].Signature, ledger.IncidentDeliveryAuth) ||
		incidents[0].LastRunID.String != run.ID {
		t.Fatalf("incidents = %+v, %v", incidents, err)
	}
	// Completion and the incident are the two terminal events of this run.
	if len(events) != 2 || events[1].Class != ledger.IncidentDeliveryAuth || !events[1].NewIncident ||
		events[1].IncidentID != incidents[0].ID {
		t.Fatalf("events = %+v", events)
	}
	if len(notes) != 1 || strings.Contains(notes[0], "Paused") || !strings.Contains(notes[0], "not delivered") {
		t.Fatalf("notifications = %q", notes)
	}

	// A delivery retry against the same revoked token counts the incident
	// rather than opening another, and still runs no model.
	if err := f.pipeline.RetryDelivery(context.Background(), f.def, row); err == nil {
		t.Fatal("retry against a rejected token succeeded")
	}
	incidents, _ = f.store.OpenIncidents()
	if len(incidents) != 1 || incidents[0].Count != 2 || events[len(events)-1].NewIncident {
		t.Fatalf("incidents after retry = %+v, last event %+v", incidents, events[len(events)-1])
	}
	if calls := readCalls(t, f.calls); calls != 2 {
		t.Fatalf("model calls = %d, want the retry to spend none", calls)
	}

	for _, where := range []string{logs.String(), row.DeliveryError.String, incidents[0].Signature} {
		if strings.Contains(where, senderTestToken) {
			t.Fatalf("token leaked into %q", where)
		}
	}
	if !strings.Contains(logs.String(), "http=401 attempts=1") {
		t.Fatalf("log = %q", logs)
	}
}
