package observer

import (
	"fmt"
	"strings"
)

// Assessment outcomes. They are outcomes and not states: effect-observed says
// the expected effect showed up; effect-absent-with-sufficient-opportunity says
// it did not, and that the subject had the declared opportunity to show it;
// insufficient-evidence says the reading could not answer the question at all,
// which is the outcome that must never close one.
const (
	OutcomeEffectObserved = "effect-observed"
	OutcomeEffectAbsent   = "effect-absent-with-sufficient-opportunity"
	OutcomeInsufficient   = "insufficient-evidence"
)

// Reopen conditions. Each is a fact about the subject or the measurement.
// Elapsed time is not among them.
const (
	ReopenReactivated   = "re-activated"
	ReopenMethodRevised = "method-revised"
	ReopenContradicted  = "contradicted"
)

// Observation kinds. Every row of an entry's history is an Assessment, and
// its kind is read from the fields it carries rather than declared beside
// them, so a payload cannot claim one kind while carrying another's fields:
//
//   - the observer's own assessment, from a run: no author or the observer's,
//     a bundle, and neither corrects nor carried_over;
//   - a person's observation: author.kind human, the facts as they state them,
//     and no bundle;
//   - a correction: a person's observation that also names, in corrects, the
//     earlier one it corrects and what was wrong with it;
//   - a carried-over reading: one first recorded elsewhere, carrying the
//     source it came from in carried_over and the date it was written there.
const (
	ObservationAssessment = "assessment"
	ObservationPerson     = "person"
	ObservationCorrection = "correction"
	ObservationCarried    = "carried-over"
)

// Assessment is one row of an entry's history. The observer's is its reading
// of a bundle: the outcome and the prose interpretation, carried alongside the
// evidence rather than merged into it. The interpretation is generated text
// and the bundle is recorded fact; stored as one field, no later reader could
// tell which of the two they were quoting. A person's carries the facts as
// they state them instead of a bundle, and Author says whose they are.
// QuestionID is the entry being answered; a later assessment about an entry an
// unprompted finding opened carries that finding's id here.
type Assessment struct {
	Envelope
	Identity   Identity `json:"identity"`
	QuestionID string   `json:"question_id,omitempty"`
	Finding    *Finding `json:"finding,omitempty"`
	// Author is who the row is by. Absent is the observer, which is every row
	// a run records; a run's row naming anyone else is refused (§10).
	Author         *Author `json:"author,omitempty"`
	Outcome        string  `json:"outcome"`
	Interpretation string  `json:"interpretation,omitempty"`
	// Facts are a person's statement of what they saw, one fact a line, kept
	// as they wrote them.
	Facts       []string     `json:"facts,omitempty"`
	Corrects    *Correction  `json:"corrects,omitempty"`
	CarriedOver *CarriedOver `json:"carried_over,omitempty"`
	Evidence    *Bundle      `json:"evidence,omitempty"`
	At          string       `json:"at"`
}

// Correction names the earlier observation a correction corrects, by its
// 1-based position in the entry's observations, and says what was wrong with
// it. Observations are append-only, so a position never comes to name another
// row. The corrected row is kept as it was recorded: the record has to show
// what was believed on the day it was believed.
type Correction struct {
	Observation int    `json:"observation"`
	Wrong       string `json:"wrong"`
}

// CarriedOver marks a reading first recorded somewhere else — a domain's own
// file, before its notebook moved here — with the source it came from. Its
// At is the date it was written there, not the date it was carried.
type CarriedOver struct {
	Source string `json:"source"`
}

// Finding is what an unprompted observation carries instead of a question id:
// the observer saw something nobody had asked about. It enters the notebook as
// a new entry under the id the observer assigns and pins the subject every later
// observation under that id must read.
type Finding struct {
	ID       string  `json:"id"`
	Subject  Subject `json:"subject"`
	Question string  `json:"question"`
	Changes  string  `json:"changes"`
}

// Kind is which of the observation kinds the row is, from the fields it
// carries.
func (a *Assessment) Kind() string {
	switch {
	case a.Corrects != nil:
		return ObservationCorrection
	case a.CarriedOver != nil:
		return ObservationCarried
	case a.Author != nil && a.Author.Kind == AuthorHuman:
		return ObservationPerson
	}
	return ObservationAssessment
}

// movesState reports whether the row goes through TransitionFrom. A
// correction and a carried-over reading are recorded and move nothing: the
// first says what was wrong with a row already applied, and the second was
// applied, if at all, by the store it came from.
func (a *Assessment) movesState() bool {
	kind := a.Kind()
	return kind == ObservationAssessment || kind == ObservationPerson
}

// EntryID is the notebook entry the assessment belongs to: the question it
// answers, or the finding it opens.
func (a *Assessment) EntryID() string {
	if a.Finding != nil {
		return a.Finding.ID
	}
	return a.QuestionID
}

// TransitionFrom applies an outcome to a question's state. The rules live here and
// not on the assessment: an assessment records what the evidence showed and
// never sets a state itself, so a later change to these rules cannot rewrite
// what was observed. insufficient-evidence leaves a question where it was, and
// no outcome moves an answered question — that needs Reopen.
func TransitionFrom(state, outcome string) (string, error) {
	if err := validateState("state", state); err != nil {
		return "", err
	}
	if err := validateOutcome("outcome", outcome); err != nil {
		return "", err
	}
	if state == StateRetired {
		return "", fieldErr("state", "%s: a retired question records nothing further; open a new one", StateRetired)
	}
	if state == StateAnswered || outcome == OutcomeInsufficient {
		return state, nil
	}
	return StateAnswered, nil
}

// ReopenFrom returns an answered question to observing under an explicit
// condition.
// Time is not one of them: a window that expired says only that nobody looked,
// and a deadline passing has never been evidence about the subject. A retired
// question does not reopen at all — it was taken off the list deliberately, and
// a new question is the honest way back on.
func ReopenFrom(state, condition string) (string, error) {
	if err := validateState("state", state); err != nil {
		return "", err
	}
	switch condition {
	case ReopenReactivated, ReopenMethodRevised, ReopenContradicted:
	default:
		return "", fieldErr("condition", "%q does not authorize a reopen; one of %s, %s, %s", condition, ReopenReactivated, ReopenMethodRevised, ReopenContradicted)
	}
	if state != StateAnswered {
		return "", fieldErr("state", "%s: only an %s question reopens", state, StateAnswered)
	}
	return StateObserving, nil
}

func (a *Assessment) validate() error {
	if err := a.checkVersion(); err != nil {
		return err
	}
	if err := a.Identity.validate("identity"); err != nil {
		return err
	}
	if a.Author != nil {
		if err := a.Author.validate("author"); err != nil {
			return err
		}
	}
	if a.Corrects != nil && a.CarriedOver != nil {
		return fieldErr("carried_over", "a correction is a person's statement made here, not a reading carried from elsewhere")
	}
	kind := a.Kind()
	switch {
	case kind == ObservationAssessment && a.Author != nil && a.Author.Kind != AuthorObserver:
		return fieldErr("author.kind", "%q records an observation only as a carried-over reading; a person's carries %s", a.Author.Kind, AuthorHuman)
	case kind == ObservationCorrection && (a.Author == nil || a.Author.Kind != AuthorHuman):
		return fieldErr("author.kind", "required %s: a correction is a person's", AuthorHuman)
	case kind == ObservationCarried && a.Author == nil:
		return fieldErr("author.kind", "required: a carried-over reading declares who first wrote it")
	}
	// Only the observer's own reading opens an entry nobody asked about; every
	// other row is about an entry the notebook already holds.
	if kind != ObservationAssessment && a.Finding != nil {
		return fieldErr("finding", "only the observer's own assessment opens a finding; a %s names question_id", kind)
	}
	// Exactly one of the two: an assessment against a question answers it, and
	// one carrying a finding opens an entry nobody asked for.
	switch {
	case a.QuestionID == "" && a.Finding == nil:
		return fieldErr("question_id", "required unless the assessment carries an unprompted finding")
	case a.QuestionID != "" && a.Finding != nil:
		return fieldErr("finding", "an assessment answers a question or opens a finding, not both")
	case a.Finding != nil:
		if err := checkQualified("finding.id", a.Finding.ID, a.Identity); err != nil {
			return err
		}
		if err := a.Finding.Subject.validate("finding.subject"); err != nil {
			return err
		}
		if a.Finding.Question == "" {
			return fieldErr("finding.question", "required: what the finding is a question about")
		}
		if a.Finding.Changes == "" {
			return fieldErr("finding.changes", "required: what it would change")
		}
	default:
		if err := checkQualified("question_id", a.QuestionID, a.Identity); err != nil {
			return err
		}
	}
	if err := validateOutcome("outcome", a.Outcome); err != nil {
		return err
	}
	for i, fact := range a.Facts {
		if strings.TrimSpace(fact) == "" {
			return fieldErr(fmt.Sprintf("facts[%d]", i), "is empty")
		}
	}
	switch kind {
	case ObservationAssessment:
		if a.Interpretation == "" {
			return fieldErr("interpretation", "required: the reading of the evidence, kept apart from it")
		}
		if len(a.Facts) != 0 {
			return fieldErr("facts", "a person's statement; the observer's reading carries its bundle")
		}
		if a.Evidence == nil {
			return fieldErr("evidence", "required: an assessment with no bundle is an opinion")
		}
		if err := a.validateEvidence(); err != nil {
			return err
		}
		return checkTimestamp("at", a.At)
	case ObservationPerson, ObservationCorrection:
		if len(a.Facts) == 0 {
			return fieldErr("facts", "required: what the person saw, as they state it")
		}
		// A bundle is what the observer read; a person's statement is not one,
		// and dressing it as one would make it checkable in a way it is not.
		if a.Evidence != nil {
			return fieldErr("evidence", "a person's observation carries their facts, not a bundle")
		}
		if kind == ObservationCorrection {
			if a.Corrects.Observation < 1 {
				return fieldErr("corrects.observation", "%d, the 1-based position of the observation corrected", a.Corrects.Observation)
			}
			if strings.TrimSpace(a.Corrects.Wrong) == "" {
				return fieldErr("corrects.wrong", "required: what was wrong with the observation corrected")
			}
		}
		return checkTimestamp("at", a.At)
	default: // ObservationCarried
		if strings.TrimSpace(a.CarriedOver.Source) == "" {
			return fieldErr("carried_over.source", "required: where the reading was first recorded")
		}
		if a.Interpretation == "" {
			return fieldErr("interpretation", "required: the reading as it was recorded")
		}
		if a.Evidence != nil {
			if err := a.validateEvidence(); err != nil {
				return err
			}
		}
		// The date it was written where it came from, as written there: a
		// time of day the source never recorded is not the contract's to add.
		return checkDay("at", a.At)
	}
}

func (a *Assessment) validateEvidence() error {
	if err := wrapField("evidence", a.Evidence.validate()); err != nil {
		return err
	}
	// The bundle's identity is the observer's own: an assessment may not sign
	// somebody else's reading as its evidence.
	if a.Evidence.Identity != a.Identity {
		return fieldErr("evidence.identity", "%s/%s, the assessment is by %s/%s", a.Evidence.Identity.Domain, a.Evidence.Identity.Observer, a.Identity.Domain, a.Identity.Observer)
	}
	return nil
}

func validateOutcome(field, outcome string) error {
	switch outcome {
	case OutcomeEffectObserved, OutcomeEffectAbsent, OutcomeInsufficient:
		return nil
	}
	return fieldErr(field, "%q, one of %s, %s, %s", outcome, OutcomeEffectObserved, OutcomeEffectAbsent, OutcomeInsufficient)
}

func (f *Finding) clone() *Finding {
	if f == nil {
		return nil
	}
	out := *f
	return &out
}

func (a *Assessment) clone() *Assessment {
	if a == nil {
		return nil
	}
	out := *a
	out.Finding = a.Finding.clone()
	if a.Author != nil {
		author := a.Author.clone()
		out.Author = &author
	}
	out.Facts = append([]string(nil), a.Facts...)
	if a.Corrects != nil {
		corrects := *a.Corrects
		out.Corrects = &corrects
	}
	if a.CarriedOver != nil {
		carried := *a.CarriedOver
		out.CarriedOver = &carried
	}
	out.Evidence = a.Evidence.clone()
	return &out
}
