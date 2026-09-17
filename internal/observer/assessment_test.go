package observer

import (
	"errors"
	"testing"
)

// Outcomes and states are separate vocabularies. This is the whole table: what
// the evidence showed on one axis, what the notebook did about it on the other,
// and insufficient-evidence closing nothing anywhere in it.
func TestOutcomesAreNotStates(t *testing.T) {
	cases := []struct {
		state   string
		outcome string
		want    string
	}{
		{StateObserving, OutcomeEffectObserved, StateAnswered},
		{StateObserving, OutcomeEffectAbsent, StateAnswered},
		{StateObserving, OutcomeInsufficient, StateObserving},
		{StateAnswered, OutcomeEffectObserved, StateAnswered},
		{StateAnswered, OutcomeInsufficient, StateAnswered},
	}
	for _, c := range cases {
		got, err := TransitionFrom(c.state, c.outcome)
		if err != nil {
			t.Fatalf("%s + %s: %v", c.state, c.outcome, err)
		}
		if got != c.want {
			t.Errorf("%s + %s = %s, want %s", c.state, c.outcome, got, c.want)
		}
	}

	// A retired question records nothing further, and neither a state nor an
	// outcome outside the vocabulary transitions at all.
	_, err := TransitionFrom(StateRetired, OutcomeEffectObserved)
	refusal(t, err, "state")
	_, err = TransitionFrom(StateObserving, "effect-probably-observed")
	refusal(t, err, "outcome")
	_, err = TransitionFrom("open", OutcomeEffectObserved)
	refusal(t, err, "state")
}

// Time expiring is not evidence about the subject: it says only that nobody
// looked. Reopening takes a named condition, and a retired question does not
// take one at all.
func TestExpiredTimeNeverReopens(t *testing.T) {
	for _, condition := range []string{"window-expired", "deadline-passed", ""} {
		_, err := ReopenFrom(StateAnswered, condition)
		refusal(t, err, "condition")
	}
	got, err := ReopenFrom(StateAnswered, ReopenContradicted)
	if err != nil {
		t.Fatal(err)
	}
	if got != StateObserving {
		t.Fatalf("state = %q, want %q", got, StateObserving)
	}
	_, err = ReopenFrom(StateRetired, ReopenReactivated)
	refusal(t, err, "state")
	_, err = ReopenFrom(StateObserving, ReopenReactivated)
	refusal(t, err, "state")
}

// The same rule through the adapter, since that is where a caller meets it: an
// answered question reopens on a condition and a retired one never does.
func TestNotebookReopen(t *testing.T) {
	nb, q, _ := seed(t)
	if _, err := nb.Record(at(nb, q.Identity, "record"), mustAssessment(t, "wren", "assessment.json")); err != nil {
		t.Fatal(err)
	}
	_, err := nb.Reopen(at(nb, q.Identity, "reopen-on-time"), q.ID, "window-expired")
	refusal(t, err, "condition")

	e, err := nb.Reopen(at(nb, q.Identity, "reopen"), q.ID, ReopenMethodRevised)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != StateObserving {
		t.Fatalf("state = %q, want %q", e.State, StateObserving)
	}
	// The reopen kept the history rather than starting a new record.
	if len(e.Observations) != 1 {
		t.Fatalf("observations = %d, want the answered reading to survive the reopen", len(e.Observations))
	}

	if _, err := nb.Retire(at(nb, q.Identity, "retire"), q.ID, "Wren-1 was archived"); err != nil {
		t.Fatal(err)
	}
	_, err = nb.Reopen(at(nb, q.Identity, "reopen-retired"), q.ID, ReopenReactivated)
	refusal(t, err, "state")
	e, err = nb.Entry(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != StateRetired {
		t.Fatalf("state = %q, want %q", e.State, StateRetired)
	}
}

// An assessment is a reading of evidence, and the evidence has to be the
// observer's own. A bundle signed by another observer is refused rather than
// quoted as this one's.
func TestAssessmentCannotSignAnotherObserversEvidence(t *testing.T) {
	a := mustAssessment(t, "wren", "assessment.json")
	a.Evidence.Identity = Identity{Observer: "shuttle", Domain: "harness"}
	refusal(t, a.validate(), "evidence.identity")

	// And the interpretation is required precisely because it is the part that
	// is generated: an assessment with no reading of the bundle is a bundle.
	a = mustAssessment(t, "wren", "assessment.json")
	a.Interpretation = ""
	refusal(t, a.validate(), "interpretation")
	a.Interpretation = "as before"
	a.Evidence = nil
	refusal(t, a.validate(), "evidence")
}

func TestFindingRequiresAValidSubject(t *testing.T) {
	a := mustAssessment(t, "harness", "assessment-unprompted.json")
	a.Finding.Subject.Instance = ""
	refusal(t, a.validate(), "finding.subject.instance")
}

// An assessment against a question the notebook does not hold is refused, not
// filed: an unprompted observation says so with a finding, which carries the
// id it wants to be known by.
func TestAssessmentAgainstAnUnknownQuestion(t *testing.T) {
	nb := NewMemory()
	a := mustAssessment(t, "wren", "assessment.json")
	_, err := nb.Record(at(nb, a.Identity, "record"), a)
	if !errors.Is(err, ErrNoEntry) {
		t.Fatalf("got %v, want %v", err, ErrNoEntry)
	}
	refusal(t, err, "question_id")
}
