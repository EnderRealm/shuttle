package observer

import (
	"strings"
	"testing"
)

// A standing question is complete on its own. Its ticket says where it came
// from and nothing more: there is no deadline on it, and none is required for
// it to be admitted and observed.
func TestStandingQuestionNeedsNoExperiment(t *testing.T) {
	q := mustQuestion(t, "wren", "question.json")
	if q.Ticket == "" {
		t.Fatal("the fixture is meant to carry a provenance ticket")
	}
	nb := NewMemory()
	e, err := nb.Register(at(nb, q.Identity, "register"), q)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != StateObserving {
		t.Fatalf("state = %q, want %q", e.State, StateObserving)
	}
	if e.Experiment != nil {
		t.Fatal("a standing question was given an experiment it never declared")
	}

	// And a question with no ticket at all is just as valid: provenance is
	// optional, which is the other half of "a ticket is not a deadline".
	untitled := *q
	untitled.Ticket = ""
	if err := untitled.validate(); err != nil {
		t.Fatalf("a question with no ticket was refused: %v", err)
	}
}

// A commit time is not an activation. The change exists; nothing has run with
// it yet, and the experiment stays pending rather than being dated from the
// merge.
func TestCommitTimeIsNotActivation(t *testing.T) {
	pending := mustExperiment(t, "wren", "experiment-pending.json")
	if pending.Activation.CommitTime == "" {
		t.Fatal("the fixture is meant to carry a commit time")
	}
	if got := pending.Status(); got != ExperimentPending {
		t.Fatalf("status = %q, want %q", got, ExperimentPending)
	}
	if _, err := pending.ActivatedAt(); err == nil {
		t.Fatal("a commit time answered as an activation instant")
	}

	live := mustExperiment(t, "wren", "experiment.json")
	if got := live.Status(); got != ExperimentLive {
		t.Fatalf("status = %q, want %q", got, ExperimentLive)
	}
	activated, err := live.ActivatedAt()
	if err != nil {
		t.Fatal(err)
	}
	if got := activated.Format("2006-01-02T15:04:05Z07:00"); got != live.Activation.At {
		t.Fatalf("activated at %s, want the deployment at %s", got, live.Activation.At)
	}
	if live.Activation.At == live.Activation.CommitTime {
		t.Fatal("the fixture dates the activation from the commit")
	}
}

// Feedback on a ticket needs all four declarations. Each is refused on its own
// and the refusal names it, because the alternative is a harness that fills one
// in and reports a measurement nobody declared.
func TestActivatingFeedbackRequiresItsDeclarations(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		mutate func(e *Experiment)
	}{
		{"no expected effect", "expected_effect", func(e *Experiment) { e.ExpectedEffect = "" }},
		{"no window", "observation.min_duration", func(e *Experiment) { e.Observation.MinDuration = "" }},
		{"no opportunity", "observation.min_opportunities", func(e *Experiment) { e.Observation.MinOpportunities = 0 }},
		{"no evidence requirements", "evidence.readings", func(e *Experiment) { e.Evidence.Readings = nil }},
		{"no baseline identity", "evidence.baseline.method.revision", func(e *Experiment) { e.Evidence.Baseline.Method.Revision = "" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := mustExperiment(t, "wren", "experiment.json")
			c.mutate(e)
			refusal(t, e.validate(), c.field)
		})
	}
}

// A pending experiment cannot carry a conclusive outcome: nothing has been live
// for the subject to have had the opportunity in.
func TestPendingExperimentAnswersNothing(t *testing.T) {
	nb := NewMemory()
	q := mustQuestion(t, "wren", "question.json")
	if _, err := nb.Register(at(nb, q.Identity, "register"), q); err != nil {
		t.Fatal(err)
	}
	pending := mustExperiment(t, "wren", "experiment-pending.json")
	if _, err := nb.Activate(at(nb, q.Identity, "register-experiment"), pending); err != nil {
		t.Fatal(err)
	}
	a := mustAssessment(t, "wren", "assessment.json")
	_, err := nb.Record(at(nb, q.Identity, "record"), a)
	refusal(t, err, "outcome")
	if !strings.Contains(err.Error(), ExperimentPending) {
		t.Fatalf("the refusal does not say the experiment is %s: %v", ExperimentPending, err)
	}
}

// The declared window and opportunity are enforced, not decorative: an absence
// read over too short a window, or over fewer opportunities than the experiment
// asked for, is refused rather than recorded as a finding of no effect.
func TestEffectAbsentNeedsTheDeclaredOpportunity(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		mutate func(a *Assessment)
	}{
		{"window opens before the activation", "evidence.window.start", func(a *Assessment) {
			a.Evidence.Window.Start = "2026-09-01T00:00:00-07:00"
		}},
		{"window shorter than declared", "evidence.window", func(a *Assessment) {
			a.Evidence.Window.End = "2026-09-14T00:00:00-07:00"
		}},
		{"fewer opportunities than declared", "evidence.coverage.observed", func(a *Assessment) {
			a.Evidence.Coverage.Observed = 1
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			nb, q, _ := seed(t)
			// The fixture's own window and coverage satisfy what the
			// experiment declared, so each case breaks exactly one of them.
			a := mustAssessment(t, "wren", "assessment.json")
			a.Outcome = OutcomeEffectAbsent
			c.mutate(a)
			_, err := nb.Record(at(nb, q.Identity, "record"), a)
			refusal(t, err, c.field)
		})
	}
}

// An omitted activation and an empty one claim the same thing: no deploy, no
// restart, no commit. They are refused identically, because pending has a floor
// — a commit time — and a registration that names nothing at all is a
// declaration missing rather than a state.
func TestActivationOmittedAndEmptyRefuseIdentically(t *testing.T) {
	omitted := mustExperiment(t, "wren", "experiment.json")
	omitted.Activation = nil
	first := refusal(t, omitted.validate(), "activation")

	empty := mustExperiment(t, "wren", "experiment.json")
	empty.Activation = &Activation{}
	second := refusal(t, empty.validate(), "activation")

	if first.Error() != second.Error() {
		t.Fatalf("omitted refused with %q, empty with %q", first, second)
	}
}
