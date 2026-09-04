package claude

import "encoding/json"

// Result subtypes the CLI reports on its terminal line. Only success is a
// successful run; the other three are failures that still carry cost (§7.4).
// These three and authentication_failed below were read out of the string
// table of the shipped claude binary on 2.1.259 and again on 2.1.260 — a
// budget stop has not been captured live, so what is verified is that the CLI
// carries these strings, not that it emits this one at the cap. A rename there
// costs the budget class: the run would classify as died with nothing here
// failing.
const (
	subtypeSuccess         = "success"
	subtypeMaxTurns        = "error_max_turns"
	subtypeMaxBudget       = "error_max_budget_usd"
	subtypeDuringExecution = "error_during_execution"
)

// The system line that reports a rejected credential (§7.4).
const (
	subtypeAPIRetry           = "api_retry"
	errorAuthenticationFailed = "authentication_failed"
)

// Result is the terminal `result` line. TotalCostUSD is only meaningful when
// CostReported is set: §12 turns on "reported no cost" being distinguishable
// from a cost of zero, which is why the ledger's own Result uses sql.Null*.
type Result struct {
	Subtype      string
	IsError      bool
	Text         string
	SessionID    string
	NumTurns     int
	TotalCostUSD float64
	CostReported bool
	InputTokens  int
	OutputTokens int
}

// Success is weft's rule verbatim: is_error falsy AND subtype success. A
// subtype the parser does not know is therefore a failure, not a pass.
func (r *Result) Success() bool {
	return !r.IsError && r.Subtype == subtypeSuccess
}

// Stream accumulates what the harness needs from stdout: the session id, the
// auth signal and the terminal verdict. Assistant content is not accumulated —
// the transcript is recorded verbatim (§7.2), so nothing else has to be held
// in memory. IO-free on purpose: the caller owns the reader.
type Stream struct {
	SessionID  string
	AuthFailed bool
	Result     *Result
}

// envelope is the subset of a line shape this package reads. Every field is
// raw and read through field(), because the CLI is an unpinned dependency and
// a field whose type changes there must cost that field rather than the whole
// line: an undecodable terminal line would report a completed, paid run as
// no_result on an exit-0 run (§7.6), indistinguishable in the ledger from the
// model never finishing. `message` is absent for a different reason — nothing
// here reads a message body, so a line whose `message` is a STRING rather than
// an object (system/permission_denied) costs nothing at all.
type envelope struct {
	Type         json.RawMessage `json:"type"`
	Subtype      json.RawMessage `json:"subtype"`
	IsError      json.RawMessage `json:"is_error"`
	Result       json.RawMessage `json:"result"`
	SessionID    json.RawMessage `json:"session_id"`
	NumTurns     json.RawMessage `json:"num_turns"`
	TotalCostUSD json.RawMessage `json:"total_cost_usd"`
	Error        json.RawMessage `json:"error"`
	Usage        json.RawMessage `json:"usage"`
}

// usage is the token block of the terminal line, raw for the same reason.
type usage struct {
	InputTokens  json.RawMessage `json:"input_tokens"`
	OutputTokens json.RawMessage `json:"output_tokens"`
}

// field reads one raw field as T, and reads as absent when the field is
// missing or is not a T.
func field[T any](raw json.RawMessage) T {
	var v T
	if json.Unmarshal(raw, &v) != nil {
		var zero T
		return zero
	}
	return v
}

// Line feeds one stdout line. An undecodable line is ignored rather than
// failed on (§7.4): the CLI's stdout is not a contract the harness may abort a
// paid run over.
func (s *Stream) Line(line []byte) {
	var env envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return
	}
	switch field[string](env.Type) {
	case "system":
		switch field[string](env.Subtype) {
		case "init":
			s.SessionID = field[string](env.SessionID)
		case subtypeAPIRetry:
			// Recorded the moment it appears (§7.4): the run's own verdict may
			// arrive later as a generic error, and this is the only line that
			// says the credential was rejected.
			if field[string](env.Error) == errorAuthenticationFailed {
				s.AuthFailed = true
			}
		}
	case "result":
		// Terminal for the turn: anything after it is not part of the verdict.
		if s.Result != nil {
			return
		}
		u := field[usage](env.Usage)
		s.Result = &Result{
			Subtype:      field[string](env.Subtype),
			IsError:      field[bool](env.IsError),
			Text:         field[string](env.Result),
			SessionID:    field[string](env.SessionID),
			NumTurns:     field[int](env.NumTurns),
			InputTokens:  field[int](u.InputTokens),
			OutputTokens: field[int](u.OutputTokens),
		}
		// The cost rides the failing envelope too: an errored turn consumed
		// everything it burned getting there, and dropping it would report the
		// runaway shapes as the cheapest runs in the ledger.
		if cost := field[*float64](env.TotalCostUSD); cost != nil {
			s.Result.TotalCostUSD = *cost
			s.Result.CostReported = true
		}
	}
	// "user" (tool results) and "rate_limit_event" are ignored, as is every
	// other type: the transcript already has them.
}
