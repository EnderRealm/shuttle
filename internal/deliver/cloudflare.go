package deliver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"shuttle/internal/config"
)

const (
	// CloudflareAPI is the v4 API root §10.1 posts under.
	CloudflareAPI = "https://api.cloudflare.com/client/v4"

	// HeaderRun and HeaderAgent trace a message in the inbox back to its
	// ledger row and run directory.
	HeaderRun   = "X-Shuttle-Run"
	HeaderAgent = "X-Shuttle-Agent"

	// retryWait is §10.1's single back-off before the one retry of a 429 or
	// 5xx.
	retryWait = 30 * time.Second

	// responseMax bounds the response body read: the API answers with a small
	// JSON envelope, and anything larger is not one.
	responseMax = 1 << 20
)

// defaultHTTP bounds one attempt, so a stalled connection cannot hold a run
// worker indefinitely.
var defaultHTTP = &http.Client{Timeout: 60 * time.Second}

// Message is one email: report.md as the text part, its rendering as the
// HTML part, and the run it came from for the custom headers.
type Message struct {
	RunID   string
	Agent   string
	Subject string
	Text    []byte
	HTML    []byte
}

// Receipt is what one Send observed, kept for the delivery log line. Status is
// the last HTTP status, 0 when no response arrived.
type Receipt struct {
	Status    int
	Attempts  int
	Delivered []string
	Queued    []string
	Bounced   []string
}

// Error is a send Cloudflare refused or did not confirm. Status is the HTTP
// status, 0 when no response arrived; Detail is Cloudflare's own error text or
// the recipients it did not accept. Neither ever carries the token.
type Error struct {
	Status int
	Detail string
}

func (e *Error) Error() string {
	msg := "cloudflare email"
	if e.Status != 0 {
		msg += fmt.Sprintf(": HTTP %d", e.Status)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// Auth reports a rejected credential. A revoked or expired token does not fix
// itself thirty seconds later, so it is never retried.
func (e *Error) Auth() bool {
	return e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden
}

// Cloudflare is the §10.1 Email Service client.
type Cloudflare struct {
	AccountID string
	From      string
	To        []string
	// Token is called on every Send, so the credential is read at send time
	// and held no longer than the request.
	Token func() (string, error)
	// BaseURL overrides CloudflareAPI.
	BaseURL string
	// HTTP overrides the bounded default client.
	HTTP *http.Client
	// Wait overrides the real back-off before the retry.
	Wait func(context.Context, time.Duration) error
}

// NewCloudflare builds the client from config.json's email block, with the
// token read from the Keychain on each send.
func NewCloudflare(cfg *config.Config) *Cloudflare {
	return &Cloudflare{
		AccountID: cfg.Email.AccountID, From: cfg.Email.From, To: cfg.Email.To,
		Token: cfg.EmailToken,
	}
}

type sendRequest struct {
	From    string            `json:"from"`
	To      []string          `json:"to"`
	Subject string            `json:"subject"`
	HTML    string            `json:"html"`
	Text    string            `json:"text"`
	Headers map[string]string `json:"headers"`
}

type sendResponse struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result *struct {
		Delivered        []string `json:"delivered"`
		Queued           []string `json:"queued"`
		PermanentBounces []string `json:"permanent_bounces"`
	} `json:"result"`
}

// Send posts the message and classifies the answer per recipient: every
// configured recipient must be delivered or queued. 429 and 5xx are retried
// once after the back-off; every other status, including 401 and 403, is
// final on the first attempt.
func (c *Cloudflare) Send(ctx context.Context, msg Message) (Receipt, error) {
	var receipt Receipt
	token, err := c.Token()
	if err != nil {
		return receipt, fmt.Errorf("cloudflare email token: %w", err)
	}
	body, err := json.Marshal(sendRequest{
		From: c.From, To: c.To, Subject: msg.Subject, HTML: string(msg.HTML), Text: string(msg.Text),
		Headers: map[string]string{HeaderRun: msg.RunID, HeaderAgent: msg.Agent},
	})
	if err != nil {
		return receipt, err
	}
	base := c.BaseURL
	if base == "" {
		base = CloudflareAPI
	}
	endpoint := strings.TrimSuffix(base, "/") + "/accounts/" + c.AccountID + "/email/sending/send"
	for {
		receipt.Attempts++
		status, resp, err := c.post(ctx, endpoint, token, body)
		receipt.Status = status
		if err != nil {
			// A transport error may follow a request that was accepted, so it
			// is not retried: a duplicate report is worse than a failed one
			// that a delivery retry can resend.
			return receipt, &Error{Detail: err.Error()}
		}
		if (status == http.StatusTooManyRequests || status >= 500) && receipt.Attempts == 1 {
			if err := c.wait(ctx); err != nil {
				return receipt, err
			}
			continue
		}
		if status < 200 || status > 299 || !resp.Success || resp.Result == nil {
			return receipt, &Error{Status: status, Detail: errorDetail(resp)}
		}
		return receipt, classify(&receipt, c.To, resp)
	}
}

func (c *Cloudflare) post(ctx context.Context, endpoint, token string, body []byte) (int, *sendResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := c.HTTP
	if client == nil {
		client = defaultHTTP
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, responseMax))
	if err != nil {
		return res.StatusCode, nil, err
	}
	var parsed sendResponse
	// An unparseable body leaves the envelope empty; the status still
	// classifies the attempt.
	_ = json.Unmarshal(data, &parsed)
	return res.StatusCode, &parsed, nil
}

func (c *Cloudflare) wait(ctx context.Context) error {
	if c.Wait != nil {
		return c.Wait(ctx, retryWait)
	}
	timer := time.NewTimer(retryWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// classify applies §10.1's per-recipient rule. A recipient Cloudflare lists in
// none of the three arrays was not confirmed either, so it fails the send too.
func classify(receipt *Receipt, to []string, resp *sendResponse) error {
	r := resp.Result
	receipt.Delivered, receipt.Queued, receipt.Bounced = r.Delivered, r.Queued, r.PermanentBounces
	var bounced, missing []string
	for _, addr := range to {
		switch {
		case containsFold(r.PermanentBounces, addr):
			bounced = append(bounced, addr)
		case containsFold(r.Delivered, addr), containsFold(r.Queued, addr):
		default:
			missing = append(missing, addr)
		}
	}
	var details []string
	if len(bounced) > 0 {
		details = append(details, "permanent bounce: "+strings.Join(bounced, ", "))
	}
	if len(missing) > 0 {
		details = append(details, "neither delivered nor queued: "+strings.Join(missing, ", "))
	}
	if len(details) > 0 {
		return &Error{Status: receipt.Status, Detail: strings.Join(details, "; ")}
	}
	return nil
}

func errorDetail(resp *sendResponse) string {
	var parts []string
	for _, e := range resp.Errors {
		parts = append(parts, fmt.Sprintf("%d %s", e.Code, e.Message))
	}
	return strings.Join(parts, "; ")
}

func containsFold(list []string, addr string) bool {
	for _, item := range list {
		if strings.EqualFold(item, addr) {
			return true
		}
	}
	return false
}
