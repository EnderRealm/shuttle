package observer

import (
	"errors"
	"testing"
)

// refusal asserts the error is a contract refusal naming field. Every test in
// this package checks the field rather than the message, since the field is the
// part of the refusal a producer acts on.
func refusal(t *testing.T, err error, field string) *Error {
	t.Helper()
	if err == nil {
		t.Fatalf("got no error, want a refusal naming %s", field)
	}
	var verr *Error
	if !errors.As(err, &verr) {
		t.Fatalf("got %T (%v), want an *Error naming %s", err, err, field)
	}
	if verr.Field != field {
		t.Fatalf("field = %q, want %q (%v)", verr.Field, field, err)
	}
	return verr
}

// seed returns a notebook holding the Wren question with its experiment live,
// which is the state the protocol and ordering tests start from.
func seed(t *testing.T) (*Memory, *Question, *Action) {
	t.Helper()
	nb := NewMemory()
	q := mustQuestion(t, "wren", "question.json")
	if _, err := nb.Register(at(nb, q.Identity, "seed-register"), q); err != nil {
		t.Fatal(err)
	}
	exp := mustExperiment(t, "wren", "experiment.json")
	if _, err := nb.Activate(at(nb, q.Identity, "seed-activate"), exp); err != nil {
		t.Fatal(err)
	}
	return nb, q, mustAction(t, "wren", "action.json")
}

// A request decided against a revision that is no longer current is refused and
// writes nothing: the decision taken in between is exactly what the caller
// could not see.
func TestStaleRequestCannotReplaceANewerDecision(t *testing.T) {
	nb, q, act := seed(t)
	stale := at(nb, q.Identity, "stale-claim")

	a := mustAssessment(t, "wren", "assessment.json")
	if _, err := nb.Record(at(nb, q.Identity, "record"), a); err != nil {
		t.Fatal(err)
	}

	_, err := nb.Claim(stale, act)
	if !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("got %v, want %v", err, ErrStaleRevision)
	}
	refusal(t, err, "request.revision")

	e, err := nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.InFlight) != 0 {
		t.Fatalf("in flight = %v, want the stale claim to have written nothing", e.InFlight)
	}
	if e.State != StateAnswered {
		t.Fatalf("state = %q, want the newer decision %q to stand", e.State, StateAnswered)
	}
}

// The replay path: a caller that never saw its response repeats the request
// under the same id, still carrying the revision it last saw, and gets the
// original outcome rather than a second write.
func TestReplayReturnsTheOriginalOutcome(t *testing.T) {
	nb, q, act := seed(t)
	req := at(nb, q.Identity, "claim")
	first, err := nb.Claim(req, act)
	if err != nil {
		t.Fatal(err)
	}
	after := nb.Revision()

	second, err := nb.Claim(req, act)
	if err != nil {
		t.Fatal(err)
	}
	if *second != *first {
		t.Fatalf("replay returned %+v, want the original %+v", second, first)
	}
	if nb.Revision() != after {
		t.Fatalf("revision = %d, want the replay to have applied nothing (%d)", nb.Revision(), after)
	}
	e, err := nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.InFlight) != 1 {
		t.Fatalf("in flight = %v, want the one claim", e.InFlight)
	}
}

// A request id identifies one decision. Reusing it for another is refused
// rather than applied, or a replay could not be told from a new write.
func TestChangedContentUnderAnAppliedRequestID(t *testing.T) {
	nb, q, act := seed(t)
	req := at(nb, q.Identity, "claim")
	if _, err := nb.Claim(req, act); err != nil {
		t.Fatal(err)
	}
	changed := *act
	changed.Body = "the same id, a different note"
	_, err := nb.Claim(req, &changed)
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("got %v, want %v", err, ErrRequestConflict)
	}
	refusal(t, err, "request.request_id")
}

// History is appended to and never rewritten: a second reading of the same
// question is another row, the first row is untouched, and the slice a caller
// holds is its own copy.
func TestObservationsAreAppendOnly(t *testing.T) {
	nb, q, _ := seed(t)
	first := mustAssessment(t, "wren", "assessment.json")
	e, err := nb.Record(at(nb, q.Identity, "record-1"), first)
	if err != nil {
		t.Fatal(err)
	}
	e.Observations[0] = nil

	second := mustAssessment(t, "wren", "assessment.json")
	second.At = "2026-09-24T07:00:00-07:00"
	second.Interpretation = "A second week, read the same way."
	e, err = nb.Record(at(nb, q.Identity, "record-2"), second)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(e.Observations))
	}
	if e.Observations[0] == nil || e.Observations[0].At != first.At {
		t.Fatal("the first observation was rewritten through the snapshot handed to the caller")
	}
	if e.Observations[1].At != second.At {
		t.Fatalf("observations[1].at = %q, want %q", e.Observations[1].At, second.At)
	}
}

// Retirement is explicit and it is a wall: nothing further is recorded against
// the question and no further action is admitted under it.
func TestRetirementRefusesLaterWork(t *testing.T) {
	nb, q, act := seed(t)
	if _, err := nb.Retire(at(nb, q.Identity, "retire"), q.ID, "Wren-1 was archived"); err != nil {
		t.Fatal(err)
	}

	_, err := nb.Record(at(nb, q.Identity, "late-record"), mustAssessment(t, "wren", "assessment.json"))
	if !errors.Is(err, ErrRetired) {
		t.Fatalf("record: got %v, want %v", err, ErrRetired)
	}
	_, err = nb.Claim(at(nb, q.Identity, "late-claim"), act)
	if !errors.Is(err, ErrRetired) {
		t.Fatalf("claim: got %v, want %v", err, ErrRetired)
	}
	_, err = nb.Retire(at(nb, q.Identity, "retire-again"), q.ID, "again")
	if !errors.Is(err, ErrRetired) {
		t.Fatalf("retire: got %v, want %v", err, ErrRetired)
	}
	// A retirement with no reason is not a retirement.
	_, err = nb.Retire(at(nb, q.Identity, "retire-no-reason"), q.ID, "")
	refusal(t, err, "reason")
}

// A notebook holds one observer's entry under one id. An id qualified for
// somebody else is refused before it reaches the store.
func TestNotebookRefusesAForeignID(t *testing.T) {
	nb, q, _ := seed(t)
	other := Identity{Observer: "boswell", Domain: "wren"}
	_, err := nb.Retire(at(nb, other, "foreign"), q.ID, "not mine")
	refusal(t, err, "entry_id")
}

// An entry the notebook does not hold answers nothing, and the refusal says so
// rather than opening one.
func TestUnknownEntry(t *testing.T) {
	nb := NewMemory()
	_, err := nb.Entry("wren/mead/days-repeating")
	if !errors.Is(err, ErrNoEntry) {
		t.Fatalf("got %v, want %v", err, ErrNoEntry)
	}
}

// Request ids are the caller's own strings. Two observers sharing a notebook
// pick the same natural id sooner or later, and neither may see the other's
// outcome or be refused for it.
func TestRequestIDsAreScopedToTheObserver(t *testing.T) {
	nb := NewMemory()
	wren := mustQuestion(t, "wren", "question.json")
	harness := mustQuestion(t, "harness", "question.json")

	if _, err := nb.Register(at(nb, wren.Identity, "register"), wren); err != nil {
		t.Fatal(err)
	}
	// The same raw request id, different content, different observer: a
	// keyspace shared across observers would refuse this as a conflict.
	if _, err := nb.Register(at(nb, harness.Identity, "register"), harness); err != nil {
		t.Fatal(err)
	}

	replayed, err := nb.Register(at(nb, wren.Identity, "register"), wren)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != wren.ID {
		t.Fatalf("the replay returned %q, want %q: the two observers share a request log", replayed.ID, wren.ID)
	}
	if nb.Revision() != 2 {
		t.Fatalf("revision = %d, want 2: one register each and a replay that applied nothing", nb.Revision())
	}
}

// The append-only guarantee is only worth what the copies are worth. Neither
// the pointer a caller passed in nor the one it was handed back may reach a
// stored record, or admission, validation and the revision can all be walked
// around in place.
func TestStoredRecordsAreNotReachableThroughPointers(t *testing.T) {
	nb, q, _ := seed(t)
	a := mustAssessment(t, "wren", "assessment.json")
	wantInterpretation := a.Interpretation
	wantReading := a.Evidence.Readings[0].Name

	first, err := nb.Record(at(nb, q.Identity, "record"), a)
	if err != nil {
		t.Fatal(err)
	}

	// The input, after the call.
	a.Interpretation = "rewritten through the caller's own pointer"
	a.Evidence.Readings[0].Name = "rewritten"
	a.Evidence.Readings[0].Value = nil

	// The nested payloads of the returned snapshot.
	first.Observations[0].Interpretation = "rewritten through the snapshot"
	first.Observations[0].Evidence.Readings[0].Name = "rewritten"
	first.Experiment.Evidence.Readings[0] = "rewritten"
	first.Experiment.Evidence.Baseline.Readings[0].Name = "rewritten"
	first.Question.Question = "rewritten"

	fresh, err := nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := fresh.Observations[0]
	if got.Interpretation != wantInterpretation {
		t.Fatalf("interpretation = %q, want %q", got.Interpretation, wantInterpretation)
	}
	if got.Evidence.Readings[0].Name != wantReading || got.Evidence.Readings[0].Value == nil {
		t.Fatalf("reading = %+v, want %q with its value", got.Evidence.Readings[0], wantReading)
	}
	if fresh.Experiment.Evidence.Readings[0] != "repeat_rate" {
		t.Fatalf("experiment requirement = %q, want %q", fresh.Experiment.Evidence.Readings[0], "repeat_rate")
	}
	if fresh.Experiment.Evidence.Baseline.Readings[0].Name != "repeat_rate" {
		t.Fatalf("baseline reading = %q, want %q", fresh.Experiment.Evidence.Baseline.Readings[0].Name, "repeat_rate")
	}
	if fresh.Question.Question == "rewritten" {
		t.Fatal("the stored question was rewritten through the snapshot")
	}

	// And the replay reports what the first attempt produced, not what a
	// caller did to the copy it was handed.
	replayed, err := nb.Record(at(nb, q.Identity, "record"), mustAssessment(t, "wren", "assessment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Observations[0].Interpretation != wantInterpretation {
		t.Fatalf("replayed interpretation = %q, want %q", replayed.Observations[0].Interpretation, wantInterpretation)
	}
}

// An absence needs something to be absent from. A question that never declared
// an expected effect declares no opportunity either, so the outcome that says
// "the change did not work" has nothing to rest on.
func TestEffectAbsentNeedsAnExperiment(t *testing.T) {
	nb := NewMemory()
	q := mustQuestion(t, "wren", "question.json")
	if _, err := nb.Register(at(nb, q.Identity, "register"), q); err != nil {
		t.Fatal(err)
	}
	a := mustAssessment(t, "wren", "assessment.json")
	a.Outcome = OutcomeEffectAbsent
	_, err := nb.Record(at(nb, q.Identity, "record"), a)
	refusal(t, err, "outcome")

	e, err := nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != StateObserving || len(e.Observations) != 0 {
		t.Fatalf("entry = %s with %d observations, want it untouched", e.State, len(e.Observations))
	}
}

// The revision a retirement reports is the one the next request is decided
// against. It is the only place the notebook states what a mutation does to the
// revision, so this is what pins that.
func TestRetirementReportsTheNextRevision(t *testing.T) {
	nb, q, _ := seed(t)
	ret, err := nb.Retire(at(nb, q.Identity, "retire"), q.ID, "Wren-1 was archived")
	if err != nil {
		t.Fatal(err)
	}
	if ret.Revision != nb.Revision() {
		t.Fatalf("retirement revision = %d, the notebook is at %d", ret.Revision, nb.Revision())
	}
	// And it is usable as it stands: a caller that holds only the retirement
	// can decide its next request against it.
	harness := mustQuestion(t, "harness", "question.json")
	if _, err := nb.Register(Request{Identity: harness.Identity, Revision: ret.Revision, RequestID: "register"}, harness); err != nil {
		t.Fatalf("the reported revision was not current: %v", err)
	}
}

// A reading of another instance is not evidence about this question. The
// experiment baseline catches it when there is one; these two paths have none,
// which is why the entry holds the subject to its own.
func TestRecordRefusesAnotherSubject(t *testing.T) {
	for _, outcome := range []string{OutcomeEffectObserved, OutcomeInsufficient} {
		t.Run(outcome, func(t *testing.T) {
			nb := NewMemory()
			q := mustQuestion(t, "wren", "question.json")
			if _, err := nb.Register(at(nb, q.Identity, "register"), q); err != nil {
				t.Fatal(err)
			}
			a := mustAssessment(t, "wren", "assessment.json")
			a.Outcome = outcome
			a.Evidence.Subject.Instance = "wren-2"
			_, err := nb.Record(at(nb, q.Identity, "record"), a)
			refusal(t, err, "evidence.subject")

			e, err := nb.Entry(q.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(e.Observations) != 0 {
				t.Fatalf("observations = %d, want the reading refused", len(e.Observations))
			}
		})
	}
}
