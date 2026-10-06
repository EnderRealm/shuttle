package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/deliver"
)

func TestDeliverTestSendsOneMessageWithoutALedgerRow(t *testing.T) {
	shuttleEnv(t)
	writeConfig(t, `{"email":{"provider":"cloudflare","account_id":"acct","from":"shuttle@example.test","to":["steve@example.test"],"keychain_item":"mail"}}`)
	var got struct {
		From    string            `json:"from"`
		To      []string          `json:"to"`
		Subject string            `json:"subject"`
		Text    string            `json:"text"`
		HTML    string            `json:"html"`
		Headers map[string]string `json:"headers"`
	}
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"messages":[],"result":{"delivered":["steve@example.test"],"queued":[],"permanent_bounces":[]}}`))
	}))
	defer srv.Close()
	original := newDeliverClient
	newDeliverClient = func(cfg *config.Config) *deliver.Cloudflare {
		client := original(cfg)
		client.BaseURL = srv.URL
		client.Token = func() (string, error) { return "fixture-token", nil }
		client.Wait = func(context.Context, time.Duration) error { return nil }
		return client
	}
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"deliver", "--test"})
	t.Cleanup(func() {
		newDeliverClient = original
		rootCmd.SetOut(nil)
		rootCmd.SetArgs(nil)
		deliverTest = false
	})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("deliver --test: %v; %s", err, &out)
	}
	runID := got.Headers[deliver.HeaderRun]
	if auth != "Bearer fixture-token" || got.From != "shuttle@example.test" ||
		len(got.To) != 1 || got.To[0] != "steve@example.test" ||
		!strings.Contains(strings.ToLower(got.Subject), "test") ||
		got.Headers[deliver.HeaderAgent] != deliverTestAgent || !strings.HasPrefix(runID, deliverTestAgent+"-") ||
		got.Text == "" || got.HTML == "" {
		t.Fatalf("request = %+v, auth %q", got, auth)
	}
	if !strings.Contains(out.String(), runID) || !strings.Contains(out.String(), "delivered\tsteve@example.test") {
		t.Fatalf("output = %q", &out)
	}
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if runs, err := store.Runs("", 0); err != nil || len(runs) != 0 {
		t.Fatalf("ledger rows = %d, %v; a test send records nothing", len(runs), err)
	}
}
