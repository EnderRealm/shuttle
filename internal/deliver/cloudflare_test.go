package deliver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testToken     = "cf-secret-token-value"
	testRecipient = "steve@example.test"
)

type reply struct {
	status int
	body   string
}

type capture struct {
	mu       sync.Mutex
	replies  []reply
	requests []*http.Request
	bodies   []sendRequest
}

func (c *capture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var body sendRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.requests = append(c.requests, r)
	c.bodies = append(c.bodies, body)
	next := c.replies[0]
	if len(c.replies) > 1 {
		c.replies = c.replies[1:]
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(next.status)
	_, _ = w.Write([]byte(next.body))
}

func ok(result string) reply {
	return reply{http.StatusOK, `{"success":true,"errors":[],"messages":[],"result":` + result + `}`}
}

func failure(status, code int, message string) reply {
	body, _ := json.Marshal(map[string]any{
		"success": false, "messages": []any{}, "result": nil,
		"errors": []map[string]any{{"code": code, "message": message}},
	})
	return reply{status, string(body)}
}

func TestCloudflareSendClassifiesEachResponse(t *testing.T) {
	tests := []struct {
		name     string
		replies  []reply
		attempts int
		waits    int
		status   int
		errText  []string
		auth     bool
	}{
		{name: "delivered", replies: []reply{ok(`{"delivered":["` + testRecipient + `"],"queued":[],"permanent_bounces":[]}`)}, attempts: 1, status: 200},
		{name: "queued", replies: []reply{ok(`{"delivered":[],"queued":["` + testRecipient + `"],"permanent_bounces":[]}`)}, attempts: 1, status: 200},
		{name: "permanent bounce", replies: []reply{ok(`{"delivered":[],"queued":[],"permanent_bounces":["` + testRecipient + `"]}`)},
			attempts: 1, status: 200, errText: []string{"HTTP 200", "permanent bounce", testRecipient}},
		{name: "unconfirmed recipient", replies: []reply{ok(`{"delivered":[],"queued":[],"permanent_bounces":[]}`)},
			attempts: 1, status: 200, errText: []string{"neither delivered nor queued", testRecipient}},
		{name: "429 then success", replies: []reply{failure(429, 10004, "rate limited"), ok(`{"delivered":["` + testRecipient + `"],"queued":[],"permanent_bounces":[]}`)},
			attempts: 2, waits: 1, status: 200},
		{name: "429 twice", replies: []reply{failure(429, 10004, "rate limited")},
			attempts: 2, waits: 1, status: 429, errText: []string{"HTTP 429", "rate limited"}},
		{name: "500", replies: []reply{failure(500, 10002, "internal error")},
			attempts: 2, waits: 1, status: 500, errText: []string{"HTTP 500", "internal error"}},
		{name: "401", replies: []reply{failure(401, 10000, "Authentication error")},
			attempts: 1, status: 401, errText: []string{"HTTP 401", "Authentication error"}, auth: true},
		{name: "403", replies: []reply{failure(403, 10000, "Forbidden")},
			attempts: 1, status: 403, errText: []string{"HTTP 403", "Forbidden"}, auth: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &capture{replies: tt.replies}
			srv := httptest.NewServer(server)
			defer srv.Close()
			tokenReads := 0
			var waits []time.Duration
			client := &Cloudflare{
				AccountID: "acct", From: "shuttle@example.test", To: []string{testRecipient},
				Token:   func() (string, error) { tokenReads++; return testToken, nil },
				BaseURL: srv.URL,
				Wait: func(_ context.Context, d time.Duration) error {
					waits = append(waits, d)
					return nil
				},
			}
			receipt, err := client.Send(context.Background(), Message{
				RunID: "daily-20260901-043000-abcd", Agent: "daily", Subject: "Daily report",
				Text: []byte("# Report\n"), HTML: []byte("<h1>Report</h1>\n"),
			})

			if len(server.requests) != tt.attempts || receipt.Attempts != tt.attempts {
				t.Fatalf("attempts = %d requests, receipt %d; want %d", len(server.requests), receipt.Attempts, tt.attempts)
			}
			if len(waits) != tt.waits {
				t.Fatalf("waits = %v, want %d", waits, tt.waits)
			}
			for _, d := range waits {
				if d != 30*time.Second {
					t.Fatalf("retry wait = %s, want 30s", d)
				}
			}
			if receipt.Status != tt.status {
				t.Fatalf("receipt status = %d, want %d", receipt.Status, tt.status)
			}
			if tokenReads != 1 {
				t.Fatalf("token reads = %d, want one per send", tokenReads)
			}
			for i, r := range server.requests {
				if r.Method != http.MethodPost || r.URL.Path != "/accounts/acct/email/sending/send" {
					t.Fatalf("request %d = %s %s", i, r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
					t.Fatalf("request %d authorization = %q", i, got)
				}
				body := server.bodies[i]
				if body.From != "shuttle@example.test" || len(body.To) != 1 || body.To[0] != testRecipient ||
					body.Subject != "Daily report" || body.Text != "# Report\n" || body.HTML != "<h1>Report</h1>\n" {
					t.Fatalf("request %d body = %+v", i, body)
				}
				if body.Headers[HeaderRun] != "daily-20260901-043000-abcd" || body.Headers[HeaderAgent] != "daily" {
					t.Fatalf("request %d headers = %v", i, body.Headers)
				}
			}

			if len(tt.errText) == 0 {
				if err != nil {
					t.Fatalf("send: %v", err)
				}
				return
			}
			var cfErr *Error
			if !errors.As(err, &cfErr) {
				t.Fatalf("err = %v, want *Error", err)
			}
			if cfErr.Status != tt.status || cfErr.Auth() != tt.auth {
				t.Fatalf("error status %d auth %t, want %d %t", cfErr.Status, cfErr.Auth(), tt.status, tt.auth)
			}
			for _, want := range tt.errText {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not name %q", err, want)
				}
			}
			if strings.Contains(err.Error(), testToken) {
				t.Fatalf("error carries the token: %q", err)
			}
		})
	}
}

func TestCloudflareTransportErrorIsNotRetriedAndHidesTheToken(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	client := &Cloudflare{
		AccountID: "acct", From: "shuttle@example.test", To: []string{testRecipient},
		Token:   func() (string, error) { return testToken, nil },
		BaseURL: srv.URL,
		Wait: func(context.Context, time.Duration) error {
			t.Fatal("a transport error was retried")
			return nil
		},
	}
	receipt, err := client.Send(context.Background(), Message{RunID: "r", Agent: "a", Subject: "s"})
	if err == nil || receipt.Attempts != 1 || receipt.Status != 0 {
		t.Fatalf("receipt %+v, err %v", receipt, err)
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatalf("error carries the token: %q", err)
	}
}
