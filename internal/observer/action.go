package observer

import "fmt"

// Action kinds. Both are proposals about a ticket and neither writes one: the
// notebook admits the proposal and records its receipt, and the store that owns
// tickets is a different store with its own failure mode. That is why admission
// and the write are ordered rather than transactional.
const (
	ActionTicketNote = "ticket-note"
	ActionTicketOpen = "ticket-open"
)

// Receipt statuses.
const (
	ReceiptInFlight = "in-flight"
	ReceiptSettled  = "settled"
)

// Receipt outcomes: what the far store did with the action.
const (
	ReceiptApplied = "applied"
	ReceiptRefused = "refused"
	ReceiptFailed  = "failed"
)

// Action is ticket-facing feedback under a stable id. The id is qualified by
// domain and observer like an entry id, so two observers writing about the same
// ticket cannot collide on one receipt. QuestionID is the entry the action
// belongs to, which for an entry opened by an unprompted finding is that
// finding's id.
type Action struct {
	Envelope
	Identity   Identity `json:"identity"`
	ID         string   `json:"id"`
	QuestionID string   `json:"question_id"`
	Kind       string   `json:"kind"`
	Ticket     string   `json:"ticket"`
	Body       string   `json:"body"`
	Evidence   []Source `json:"evidence"`
}

// Receipt is what became of an action, under the action's own id. A claim puts
// the id in flight; a settle records what the far store did with it. An
// uncertain response is replayed under the same id, so a ticket write that
// landed while the claim looked lost is reconciled rather than written twice.
type Receipt struct {
	Envelope
	Identity  Identity `json:"identity"`
	ActionID  string   `json:"action_id"`
	Status    string   `json:"status"`
	Outcome   string   `json:"outcome,omitempty"`
	Reference string   `json:"reference,omitempty"`
	At        string   `json:"at"`
}

func (a *Action) validate() error {
	if err := a.checkVersion(); err != nil {
		return err
	}
	if err := a.Identity.validate("identity"); err != nil {
		return err
	}
	if err := checkQualified("id", a.ID, a.Identity); err != nil {
		return err
	}
	if err := checkQualified("question_id", a.QuestionID, a.Identity); err != nil {
		return err
	}
	if a.Kind != ActionTicketNote && a.Kind != ActionTicketOpen {
		return fieldErr("kind", "%q, one of %s, %s", a.Kind, ActionTicketNote, ActionTicketOpen)
	}
	// A note goes on an existing ticket; opening one has no ticket yet.
	if a.Kind == ActionTicketNote && a.Ticket == "" {
		return fieldErr("ticket", "required for %s", ActionTicketNote)
	}
	if a.Kind == ActionTicketOpen && a.Ticket != "" {
		return fieldErr("ticket", "%q, but %s opens one", a.Ticket, ActionTicketOpen)
	}
	if a.Body == "" {
		return fieldErr("body", "required")
	}
	if len(a.Evidence) == 0 {
		return fieldErr("evidence", "required: what a reader follows to check the claim")
	}
	for i, s := range a.Evidence {
		if s.Kind == "" || s.Ref == "" {
			return fieldErr(fmt.Sprintf("evidence[%d]", i), "kind and ref are required")
		}
	}
	return nil
}

func (r *Receipt) validate() error {
	if err := r.checkVersion(); err != nil {
		return err
	}
	if err := r.Identity.validate("identity"); err != nil {
		return err
	}
	if err := checkQualified("action_id", r.ActionID, r.Identity); err != nil {
		return err
	}
	switch r.Status {
	case ReceiptInFlight:
		if r.Outcome != "" {
			return fieldErr("outcome", "%q, but the action is still %s", r.Outcome, ReceiptInFlight)
		}
	case ReceiptSettled:
		switch r.Outcome {
		case ReceiptApplied:
			// The far store's own id is what makes a replay reconcilable
			// rather than a second write.
			if r.Reference == "" {
				return fieldErr("reference", "required when the outcome is %s: the id the write produced", ReceiptApplied)
			}
		case ReceiptRefused, ReceiptFailed:
		default:
			return fieldErr("outcome", "%q, one of %s, %s, %s", r.Outcome, ReceiptApplied, ReceiptRefused, ReceiptFailed)
		}
	default:
		return fieldErr("status", "%q, one of %s, %s", r.Status, ReceiptInFlight, ReceiptSettled)
	}
	return checkTimestamp("at", r.At)
}

func (a *Action) clone() *Action {
	if a == nil {
		return nil
	}
	out := *a
	out.Evidence = append([]Source(nil), a.Evidence...)
	return &out
}

// clone copies a receipt. Every field is a value, so the struct copy is the
// deep one; the method exists so the notebook's copy-in, copy-out rule reads
// the same at every call site.
func (r *Receipt) clone() *Receipt {
	if r == nil {
		return nil
	}
	out := *r
	return &out
}
