package observer

import "errors"

// The refusals a notebook adapter returns. They are sentinels because the
// caller's next move differs per class: a stale revision is re-read and
// retried, a retired question is not, and a request conflict is a bug in the
// caller rather than a race.
var (
	// ErrStaleRevision means the decision was taken against a revision that is
	// no longer current. The caller re-reads the entry and decides again; it
	// never retries with the same revision, because the newer decision it
	// cannot see is exactly what the precondition is protecting.
	ErrStaleRevision = errors.New("notebook revision is not current")

	// ErrRetired means the question was explicitly taken off the list. It
	// admits no observation and no new claim.
	ErrRetired = errors.New("the question is retired")

	// ErrNoEntry means the notebook holds no entry under that id.
	ErrNoEntry = errors.New("no such entry")

	// ErrNotClaimed means the action id was never put in flight, so there is
	// nothing to settle or reconcile under it.
	ErrNotClaimed = errors.New("the action was never claimed")

	// ErrRequestConflict means a request id was reused with changed content. A
	// request id identifies one decision; reusing it for another is refused
	// rather than applied, or a replay could not be told from a new write.
	ErrRequestConflict = errors.New("the request id was applied with different content")

	// ErrActionHeld means the action id is already in flight or settled. An
	// action id identifies one proposal; a new proposal is claimed under its
	// own id rather than a second claim of one the notebook already holds.
	ErrActionHeld = errors.New("the action id is already held")
)

// Request is the envelope every mutation carries.
//
// Revision is the notebook revision the caller decided against. A mutation
// whose revision is not current is refused with ErrStaleRevision and changes
// nothing: a stale request must never replace a decision taken since it was
// composed.
//
// RequestID makes a replay after an uncertain response safe. A repeat of the
// same id with the same content returns the original outcome; the same id with
// changed content is refused with ErrRequestConflict. The id is checked before
// the revision, because a replay necessarily carries the revision the caller
// last saw rather than the one its own first attempt produced. It is scoped by
// Identity like every other name here, so it need only be unique to the
// observer that issued it: two observers picking the same natural id never
// meet.
type Request struct {
	Identity  Identity `json:"identity"`
	Revision  int64    `json:"revision"`
	RequestID string   `json:"request_id"`
}

// Retirement is what an explicit retirement returns. InFlight names the actions
// that were already claimed when it landed. They are returned rather than
// swallowed because retirement bounds admission and nothing else: it refuses
// every later claim, and it makes no promise about a ticket write already on
// the wire. Those actions may still settle, or be reconciled by receipt under
// the same id, and neither reactivates the question.
type Retirement struct {
	EntryID  string   `json:"entry_id"`
	Reason   string   `json:"reason"`
	At       string   `json:"at"`
	InFlight []string `json:"in_flight"`
	Revision int64    `json:"revision"`
}

// Entry is one line of the notebook: the question or unprompted finding, the
// experiment attached to it, its state, the observations recorded against it in
// order, and the actions still in flight.
type Entry struct {
	ID               string        `json:"id"`
	Identity         Identity      `json:"identity"`
	Question         *Question     `json:"question,omitempty"`
	Finding          *Finding      `json:"finding,omitempty"`
	Experiment       *Experiment   `json:"experiment,omitempty"`
	ExperimentStatus string        `json:"experiment_status,omitempty"`
	State            string        `json:"state"`
	Observations     []*Assessment `json:"observations"`
	InFlight         []string      `json:"in_flight"`
	Retirement       *Retirement   `json:"retirement,omitempty"`
}

func (r *Retirement) clone() *Retirement {
	if r == nil {
		return nil
	}
	out := *r
	out.InFlight = append([]string(nil), r.InFlight...)
	return &out
}

// Notebook is the adapter protocol. The notebook itself stays domain-owned — a
// domain persists its entries however it already stores them — and shuttle
// supplies the validation and the state-transition rules the implementation
// applies. Memory is the reference implementation both are checked against.
//
// The protocol in full:
//
//   - Ids are stable and qualified. An entry id and an action id are assigned by
//     the observer as <domain>/<observer>/<name> and never reassigned, so two
//     observers can hold a question of the same name and neither one's receipts
//     can be mistaken for the other's.
//   - Every mutation carries a revision precondition. A stale one is refused
//     with ErrStaleRevision and writes nothing.
//   - Observations are append-only. Recording adds a row; it never edits or
//     removes one, and a later reading of the same window is another row rather
//     than a correction of the first.
//   - A duplicate request id returns the original outcome. Changed content
//     under a request id already applied is refused with ErrRequestConflict.
//     Request ids are scoped to the identity that issued them, so an adapter
//     keys its request log by observer and id together rather than by the id
//     alone: unqualified, two observers sharing a notebook would collide on a
//     natural id and neither could clear it.
//   - Retirement is explicit. Nothing retires on a deadline, and nothing
//     reopens on one either: Reopen takes a named condition.
//   - An uncertain response is replayed under the same request id, and a claim
//     whose far-store write may or may not have landed is reconciled by settling
//     the same action id.
//
// Claim and Retire are serialized against each other by the implementation,
// which is what makes their order decidable: retirement first admits no
// subsequent claim, and a claim first leaves an in-flight id that the
// retirement reports and that may still settle.
type Notebook interface {
	// Revision returns the revision a caller puts in its next Request.
	Revision() int64

	// Entry returns a snapshot of one entry, or ErrNoEntry.
	Entry(id string) (*Entry, error)

	// Register admits a standing question under its own id.
	Register(req Request, q *Question) (*Entry, error)

	// Activate attaches an experiment to a question. A pending registration may
	// be replaced by the live one when the deploy happens; a live experiment is
	// not replaced, since that would silently re-date a window already being
	// measured against.
	Activate(req Request, exp *Experiment) (*Entry, error)

	// Record appends an assessment, applying the transition rules. An
	// assessment carrying a finding opens the entry it names.
	Record(req Request, a *Assessment) (*Entry, error)

	// Claim puts an action id in flight. It is what makes the action
	// identified before anything is written to the store that owns tickets.
	Claim(req Request, act *Action) (*Receipt, error)

	// Settle records what the far store did with a claimed action. It never
	// changes the question's state: a ticket write landing does not answer a
	// question and does not reactivate a retired one.
	Settle(req Request, rec *Receipt) (*Receipt, error)

	// Retire ends a question explicitly and reports the actions in flight.
	Retire(req Request, entryID, reason string) (*Retirement, error)

	// Reopen returns an answered question to observing under a named condition.
	Reopen(req Request, entryID, condition string) (*Entry, error)
}
