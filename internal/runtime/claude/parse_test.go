package claude

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

// feedFixture reads a testdata stream into a Stream through the production
// drain, so the fixtures exercise the line splitting a real run uses.
func feedFixture(t *testing.T, name string) *Stream {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var d drain
	drainStdout(f, io.Discard, &d)
	stream, _, _, _ := d.snapshot()
	return &stream
}

func TestStreamFixtures(t *testing.T) {
	tests := []struct {
		fixture      string
		sessionID    string
		authFailed   bool
		hasResult    bool
		subtype      string
		success      bool
		costReported bool
		cost         float64
		numTurns     int
		inputTokens  int
		outputTokens int
	}{
		{
			fixture:      "claude-stream-success.ndjson",
			sessionID:    "81758e9e-a4da-4df1-93a2-ee5b4a6bf573",
			hasResult:    true,
			subtype:      "success",
			success:      true,
			costReported: true,
			cost:         0.029032500000000003,
			numTurns:     1,
			inputTokens:  10,
			outputTokens: 41,
		},
		{
			fixture:      "claude-stream-tool-heavy.ndjson",
			sessionID:    "redacted-session-id",
			hasResult:    true,
			subtype:      "success",
			success:      true,
			costReported: true,
			numTurns:     20,
		},
		{
			fixture:      "api-retry-auth.ndjson",
			sessionID:    "auth-fixture-session",
			authFailed:   true,
			hasResult:    true,
			subtype:      subtypeDuringExecution,
			costReported: true,
		},
		{
			fixture:      "budget-stop.ndjson",
			sessionID:    "budget-fixture-session",
			hasResult:    true,
			subtype:      subtypeMaxBudget,
			costReported: true,
			cost:         0.0512,
			numTurns:     4,
			inputTokens:  1200,
			outputTokens: 340,
		},
		{
			fixture:      "string-message.ndjson",
			sessionID:    "string-message-session",
			hasResult:    true,
			subtype:      "success",
			success:      true,
			costReported: true,
			cost:         0.0031,
			numTurns:     2,
			inputTokens:  900,
			outputTokens: 120,
		},
		{
			fixture:   "eof-no-result.ndjson",
			sessionID: "eof-fixture-session",
		},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			stream := feedFixture(t, tt.fixture)
			if stream.SessionID != tt.sessionID {
				t.Errorf("session id = %q, want %q", stream.SessionID, tt.sessionID)
			}
			if stream.AuthFailed != tt.authFailed {
				t.Errorf("auth failed = %v, want %v", stream.AuthFailed, tt.authFailed)
			}
			if (stream.Result != nil) != tt.hasResult {
				t.Fatalf("result present = %v, want %v", stream.Result != nil, tt.hasResult)
			}
			if !tt.hasResult {
				return
			}
			res := stream.Result
			if res.Subtype != tt.subtype {
				t.Errorf("subtype = %q, want %q", res.Subtype, tt.subtype)
			}
			if res.Success() != tt.success {
				t.Errorf("success = %v, want %v", res.Success(), tt.success)
			}
			if res.CostReported != tt.costReported {
				t.Errorf("cost reported = %v, want %v", res.CostReported, tt.costReported)
			}
			if res.TotalCostUSD != tt.cost {
				t.Errorf("cost = %v, want %v", res.TotalCostUSD, tt.cost)
			}
			if res.NumTurns != tt.numTurns {
				t.Errorf("num turns = %d, want %d", res.NumTurns, tt.numTurns)
			}
			if res.InputTokens != tt.inputTokens || res.OutputTokens != tt.outputTokens {
				t.Errorf("tokens = %d/%d, want %d/%d", res.InputTokens, res.OutputTokens, tt.inputTokens, tt.outputTokens)
			}
		})
	}
}

// A failed result still carries what it burned getting there, on both shapes
// the CLI reports failure with.
func TestStreamFailedResultCarriesCost(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{
			name: "is_error true",
			line: `{"type":"result","subtype":"success","is_error":true,"result":"boom","total_cost_usd":0.21}`,
		},
		{
			name: "subtype not success, is_error absent",
			line: `{"type":"result","subtype":"error_max_turns","result":"out of turns","total_cost_usd":0.21}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stream Stream
			stream.Line([]byte(tt.line))
			if stream.Result == nil {
				t.Fatal("no result recorded")
			}
			if stream.Result.Success() {
				t.Error("classified success, want failed")
			}
			if !stream.Result.CostReported || stream.Result.TotalCostUSD != 0.21 {
				t.Errorf("cost = %v (reported %v), want 0.21", stream.Result.TotalCostUSD, stream.Result.CostReported)
			}
		})
	}
}

// A cost of zero is not the same as no cost reported (§12).
func TestStreamCostReportedDistinguishesZero(t *testing.T) {
	var stream Stream
	stream.Line([]byte(`{"type":"result","subtype":"success","is_error":false,"result":"ok"}`))
	if stream.Result == nil {
		t.Fatal("no result recorded")
	}
	if stream.Result.CostReported {
		t.Error("cost reported on a result with no total_cost_usd")
	}
}

func TestStreamIgnoresUndecodableLine(t *testing.T) {
	var stream Stream
	stream.Line([]byte("not json at all"))
	stream.Line([]byte(`{"type":"result","subtype":"success","is_error":false,"result":"ok","total_cost_usd":0.01}`))
	if stream.Result == nil || !stream.Result.Success() {
		t.Fatalf("result after an undecodable line = %+v", stream.Result)
	}
}

// The CLI is unpinned: a type change on one payload field costs that field and
// nothing else. Dropping the line would report a completed, paid run as
// no_result on an exit-0 run (§7.6).
func TestStreamToleratesATypeChangeOnAPayloadField(t *testing.T) {
	var stream Stream
	stream.Line([]byte(`{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":"12","session_id":"typed-session","total_cost_usd":0.21,"usage":{"input_tokens":900,"output_tokens":"120"}}`))
	if stream.Result == nil {
		t.Fatal("no result recorded: num_turns changing type cost the whole line")
	}
	if !stream.Result.Success() {
		t.Errorf("result = %+v, want the verdict the line still carries", stream.Result)
	}
	if stream.Result.NumTurns != 0 || stream.Result.OutputTokens != 0 {
		t.Errorf("num turns = %d, output tokens = %d, want the changed fields to read as absent",
			stream.Result.NumTurns, stream.Result.OutputTokens)
	}
	if !stream.Result.CostReported || stream.Result.TotalCostUSD != 0.21 || stream.Result.InputTokens != 900 {
		t.Errorf("result = %+v, want the unchanged fields intact", stream.Result)
	}
	if stream.Result.SessionID != "typed-session" {
		t.Errorf("session id = %q, want typed-session", stream.Result.SessionID)
	}
}

// The result is terminal for the turn: a second one is not the verdict.
func TestStreamFirstResultWins(t *testing.T) {
	var stream Stream
	stream.Line([]byte(`{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.01}`))
	stream.Line([]byte(`{"type":"result","subtype":"error_max_turns","is_error":true,"total_cost_usd":0.99}`))
	if stream.Result == nil {
		t.Fatal("no result recorded")
	}
	if stream.Result.Subtype != "success" {
		t.Errorf("subtype = %q, want the first result's", stream.Result.Subtype)
	}
}
