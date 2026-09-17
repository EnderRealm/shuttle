package observer

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

// Assessment is the observer's reading of a bundle: the outcome and the prose
// interpretation, carried alongside the evidence rather than merged into it.
// The interpretation is generated text and the bundle is recorded fact; stored
// as one field, no later reader could tell which of the two they were quoting.
// QuestionID is the entry being answered; a later assessment about an entry an
// unprompted finding opened carries that finding's id here.
type Assessment struct {
	Envelope
	Identity       Identity `json:"identity"`
	QuestionID     string   `json:"question_id,omitempty"`
	Finding        *Finding `json:"finding,omitempty"`
	Outcome        string   `json:"outcome"`
	Interpretation string   `json:"interpretation"`
	Evidence       *Bundle  `json:"evidence"`
	At             string   `json:"at"`
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
	if a.Interpretation == "" {
		return fieldErr("interpretation", "required: the reading of the evidence, kept apart from it")
	}
	if a.Evidence == nil {
		return fieldErr("evidence", "required: an assessment with no bundle is an opinion")
	}
	if err := wrapField("evidence", a.Evidence.validate()); err != nil {
		return err
	}
	// The bundle's identity is the observer's own: an assessment may not sign
	// somebody else's reading as its evidence.
	if a.Evidence.Identity != a.Identity {
		return fieldErr("evidence.identity", "%s/%s, the assessment is by %s/%s", a.Evidence.Identity.Domain, a.Evidence.Identity.Observer, a.Identity.Domain, a.Identity.Observer)
	}
	return checkTimestamp("at", a.At)
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
	out.Evidence = a.Evidence.clone()
	return &out
}
