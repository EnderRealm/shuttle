package observer

import (
	"errors"
	"sync"
	"testing"
)

// settle turns a claim's receipt into the settlement the far store's answer
// would produce.
func settle(rec *Receipt, reference string) *Receipt {
	out := *rec
	out.Status = ReceiptSettled
	out.Outcome = ReceiptApplied
	out.Reference = reference
	return &out
}

// Retirement first: nothing is in flight, and no action is admitted afterwards.
func TestRetirementBeforeClaimAdmitsNoAction(t *testing.T) {
	nb, q, act := seed(t)
	ret, err := nb.Retire(at(nb, q.Identity, "retire"), q.ID, "Wren-1 was archived")
	if err != nil {
		t.Fatal(err)
	}
	if len(ret.InFlight) != 0 {
		t.Fatalf("in flight = %v, want none", ret.InFlight)
	}
	_, err = nb.Claim(at(nb, q.Identity, "claim"), act)
	if !errors.Is(err, ErrRetired) {
		t.Fatalf("got %v, want %v", err, ErrRetired)
	}
	refusal(t, err, "question_id")
}

// Claim first: the retirement reports the in-flight id rather than pretending
// the write can be recalled, that one action still settles under its own id,
// and settling neither reopens the question nor admits anything else.
func TestClaimBeforeRetirementReportsTheInFlightAction(t *testing.T) {
	nb, q, act := seed(t)
	claim := at(nb, q.Identity, "claim")
	rec, err := nb.Claim(claim, act)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := nb.Retire(at(nb, q.Identity, "retire"), q.ID, "Wren-1 was archived")
	if err != nil {
		t.Fatal(err)
	}
	if len(ret.InFlight) != 1 || ret.InFlight[0] != act.ID {
		t.Fatalf("in flight = %v, want %q: retirement bounds admission, it does not cancel a write already on the wire", ret.InFlight, act.ID)
	}
	// The retirement promised nothing about the ticket write, so the entry
	// still carries the action as in flight rather than as cancelled.
	e, err := nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.InFlight) != 1 || e.InFlight[0] != act.ID {
		t.Fatalf("in flight = %v after retirement, want %q still out", e.InFlight, act.ID)
	}

	// A caller whose claim response was lost replays it under the same id
	// after the retirement. That is reconciliation by receipt: the original
	// in-flight receipt comes back, nothing new is admitted, and the
	// retirement does not turn the replay into a refusal.
	replayed, err := nb.Claim(claim, act)
	if err != nil {
		t.Fatalf("replayed claim: got %v, want the original receipt", err)
	}
	if replayed.Status != ReceiptInFlight || replayed.At != rec.At {
		t.Fatalf("replayed receipt = %+v, want the original %+v", replayed, rec)
	}

	// The claimed action settles, or is reconciled by receipt, under the id it
	// was claimed with.
	settled, err := nb.Settle(at(nb, q.Identity, "settle"), settle(rec, "village2/show-whether-week-a0f2#note-1"))
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != ReceiptSettled || settled.Outcome != ReceiptApplied {
		t.Fatalf("receipt = %+v, want it settled as %s", settled, ReceiptApplied)
	}

	e, err = nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != StateRetired {
		t.Fatalf("state = %q, want the settlement to leave it %q", e.State, StateRetired)
	}
	if len(e.InFlight) != 0 {
		t.Fatalf("in flight = %v, want the settled action cleared", e.InFlight)
	}
	// Settling did not reactivate the question, and nothing else can: a
	// retired entry records no further observation and does not reopen.
	a := mustAssessment(t, "wren", "assessment.json")
	if _, err := nb.Record(at(nb, q.Identity, "record-after-settle"), a); !errors.Is(err, ErrRetired) {
		t.Fatalf("record after settlement: got %v, want %v", err, ErrRetired)
	}
	_, err = nb.Reopen(at(nb, q.Identity, "reopen-after-settle"), q.ID, ReopenReactivated)
	refusal(t, err, "state")
	if e, err = nb.Entry(q.ID); err != nil || e.State != StateRetired {
		t.Fatalf("entry after refused reopen = %+v, %v; want it still %s", e, err, StateRetired)
	}

	// Only that action. A second one is refused by the retirement, and an id
	// that was never claimed has nothing to settle.
	second := *act
	second.ID = act.Identity.Qualify("note-days-repeating-2")
	_, err = nb.Claim(at(nb, q.Identity, "claim-2"), &second)
	if !errors.Is(err, ErrRetired) {
		t.Fatalf("second claim: got %v, want %v", err, ErrRetired)
	}
	unclaimed := settle(rec, "village2/show-whether-week-a0f2#note-2")
	unclaimed.ActionID = second.ID
	_, err = nb.Settle(at(nb, q.Identity, "settle-2"), unclaimed)
	if !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("settle of an unclaimed id: got %v, want %v", err, ErrNotClaimed)
	}
}

// maxAttempts bounds until. Two goroutines mutating once each need two attempts
// between them; the cap is generous and exists so a regression fails by name
// rather than spinning to the package timeout.
const maxAttempts = 100

// until retries a mutation while the notebook moves under it. A stale revision
// is the caller re-reading and deciding again, which is the protocol's own
// answer to a race; anything else is the outcome under test. t.Errorf rather
// than t.Fatalf because this runs off the test's own goroutine.
func until[T any](t *testing.T, nb *Memory, op string, id Identity, requestID string, do func(Request) (T, error)) (T, error) {
	t.Helper()
	var out T
	var err error
	var revision int64
	for range maxAttempts {
		req := at(nb, id, requestID)
		revision = req.Revision
		out, err = do(req)
		if !errors.Is(err, ErrStaleRevision) {
			return out, err
		}
	}
	t.Errorf("%s lost the revision precondition %d times running, last at revision %d", op, maxAttempts, revision)
	return out, err
}

// The two orderings are the only two outcomes. Whichever the notebook admits
// first is the one that happened, and the invariant it carries is the one the
// deterministic tests above spell out.
func TestConcurrentClaimAndRetirement(t *testing.T) {
	nb, q, act := seed(t)

	var wg sync.WaitGroup
	var rec *Receipt
	var ret *Retirement
	var claimErr, retireErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		rec, claimErr = until(t, nb, "claim", q.Identity, "claim", func(req Request) (*Receipt, error) {
			return nb.Claim(req, act)
		})
	}()
	go func() {
		defer wg.Done()
		ret, retireErr = until(t, nb, "retire", q.Identity, "retire", func(req Request) (*Retirement, error) {
			return nb.Retire(req, q.ID, "Wren-1 was archived")
		})
	}()
	wg.Wait()

	if retireErr != nil {
		t.Fatalf("the retirement itself was refused: %v", retireErr)
	}
	if claimErr != nil {
		// Retirement first.
		if !errors.Is(claimErr, ErrRetired) {
			t.Fatalf("claim: got %v, want %v", claimErr, ErrRetired)
		}
		if len(ret.InFlight) != 0 {
			t.Fatalf("in flight = %v, want none when the retirement was admitted first", ret.InFlight)
		}
		return
	}

	// Claim first.
	if len(ret.InFlight) != 1 || ret.InFlight[0] != act.ID {
		t.Fatalf("in flight = %v, want %q reported by the retirement", ret.InFlight, act.ID)
	}
	// Only that action: the retirement admits no other, and an unclaimed id
	// has nothing to settle.
	second := *act
	second.ID = act.Identity.Qualify("note-days-repeating-2")
	if _, err := nb.Claim(at(nb, q.Identity, "claim-2"), &second); !errors.Is(err, ErrRetired) {
		t.Fatalf("second claim: got %v, want %v", err, ErrRetired)
	}
	unclaimed := settle(rec, "village2/show-whether-week-a0f2#note-2")
	unclaimed.ActionID = second.ID
	if _, err := nb.Settle(at(nb, q.Identity, "settle-2"), unclaimed); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("settle of an unclaimed id: got %v, want %v", err, ErrNotClaimed)
	}
	if _, err := nb.Settle(at(nb, q.Identity, "settle"), settle(rec, "village2/show-whether-week-a0f2#note-1")); err != nil {
		t.Fatalf("the in-flight action could not settle: %v", err)
	}
	e, err := nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != StateRetired {
		t.Fatalf("state = %q, want %q", e.State, StateRetired)
	}
	if len(e.InFlight) != 0 {
		t.Fatalf("in flight = %v, want the settled action cleared", e.InFlight)
	}
}
