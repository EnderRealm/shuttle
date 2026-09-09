package observer

import "time"

// Question states. A question is observing until evidence answers it, answered
// when evidence settled it either way, and retired when it was explicitly
// taken off the list. Nothing else is a state, and none of the three is an
// assessment outcome: what the evidence showed and what the notebook did about
// it are separate records (see assessment.go).
const (
	StateObserving = "observing"
	StateAnswered  = "answered"
	StateRetired   = "retired"
)

// Speeds. A day of record answers a fast question; a slow one needs two
// windows, a stretch of days or a comparison run. It is declared on every
// question so a slow one cannot be answered early off whichever fast readings
// happened to be cheap.
const (
	SpeedFast = "fast"
	SpeedSlow = "slow"
)

// Experiment activation status. Pending is a registration that is complete but
// has not been observed running; live is one whose activation event happened.
const (
	ExperimentPending = "pending"
	ExperimentLive    = "live"
)

// Activation kinds: the events that make a change live for a subject. A commit
// is deliberately not on the list.
const (
	ActivationDeployment = "deployment"
	ActivationRestart    = "restart"
)

// Question is a standing question about a subject: what would answer it, what
// answering it would change, and how fast its inputs move. A ticket reference
// is provenance — where the question came from — and never a deadline: a
// question with a ticket and no experiment is complete on its own and stays
// observing until evidence answers it.
type Question struct {
	Envelope
	Identity Identity `json:"identity"`
	ID       string   `json:"id"`
	Subject  Subject  `json:"subject"`
	Question string   `json:"question"`
	Reading  string   `json:"reading"`
	Changes  string   `json:"changes"`
	Speed    string   `json:"speed"`
	Asked    string   `json:"asked"`
	Ticket   string   `json:"ticket,omitempty"`
}

// Experiment is what turns a ticket into feedback: the effect the change was
// expected to have, the moment it became live for the subject, the window and
// opportunity that must pass before an absence means anything, and the
// readings and baseline it will be judged by. Every part is declared by the
// worker registering it; none of it is inferred, and a missing part is pending
// or refused rather than filled in.
type Experiment struct {
	Envelope
	Identity       Identity     `json:"identity"`
	QuestionID     string       `json:"question_id"`
	Ticket         string       `json:"ticket"`
	ExpectedEffect string       `json:"expected_effect"`
	Activation     *Activation  `json:"activation"`
	Observation    Observation  `json:"observation"`
	Evidence       Requirements `json:"evidence"`
}

// Activation is the event that made the change live for the subject.
// CommitTime is provenance and is explicitly not activation: merged code is not
// running code, and starting the window at the commit counts opportunity the
// subject never had. A registration carrying only a commit time is pending.
type Activation struct {
	Kind       string `json:"kind,omitempty"`
	At         string `json:"at,omitempty"`
	Instance   string `json:"instance,omitempty"`
	Reference  string `json:"reference,omitempty"`
	CommitTime string `json:"commit_time,omitempty"`
}

// Observation is what must have happened before an absence of the expected
// effect is evidence of anything: time since activation, and the number of
// opportunities the subject actually had to show it. A window that elapsed
// without the opportunity occurring answers nothing, which is why the count is
// declared up front rather than argued about afterwards. Opportunity names the
// unit MinOpportunities is counted in, and a bundle answering this experiment
// reports Coverage.Observed in that same unit.
type Observation struct {
	MinDuration      string `json:"min_duration"`
	Opportunity      string `json:"opportunity"`
	MinOpportunities int    `json:"min_opportunities"`
}

// Requirements name the readings that must be present for the experiment to be
// judged, and the baseline they are measured against. The baseline carries the
// method revision, subject instance and scope, so a reading taken by another
// method cannot answer the experiment by accident.
type Requirements struct {
	Readings []string  `json:"readings"`
	Baseline *Baseline `json:"baseline"`
}

// Status reports whether the experiment is live or still pending activation.
// It is separate from validation on purpose: a missing declaration is a refusal
// naming the field, while a missing activation is a legitimate state — a worker
// registers the experiment when it lands the change, and the deploy follows.
func (e *Experiment) Status() string {
	if e.Activation == nil || e.Activation.At == "" {
		return ExperimentPending
	}
	return ExperimentLive
}

// ActivatedAt is the instant the change became live, valid only when Status is
// ExperimentLive.
func (e *Experiment) ActivatedAt() (time.Time, error) {
	if e.Status() != ExperimentLive {
		return time.Time{}, fieldErr("activation.at", "the experiment is %s", ExperimentPending)
	}
	return time.Parse(time.RFC3339, e.Activation.At)
}

func (q *Question) validate() error {
	if err := q.checkVersion(); err != nil {
		return err
	}
	if err := q.Identity.validate("identity"); err != nil {
		return err
	}
	if err := checkQualified("id", q.ID, q.Identity); err != nil {
		return err
	}
	if err := q.Subject.validate("subject"); err != nil {
		return err
	}
	if q.Question == "" {
		return fieldErr("question", "required")
	}
	if q.Reading == "" {
		return fieldErr("reading", "required: what would answer it")
	}
	// A question that changes nothing is idle watching; it gets a consequence
	// or it comes off the list.
	if q.Changes == "" {
		return fieldErr("changes", "required: what answering it would change")
	}
	if q.Speed != SpeedFast && q.Speed != SpeedSlow {
		return fieldErr("speed", "%q, one of %s, %s", q.Speed, SpeedFast, SpeedSlow)
	}
	if err := checkTimestamp("asked", q.Asked); err != nil {
		return err
	}
	return nil
}

func (e *Experiment) validate() error {
	if err := e.checkVersion(); err != nil {
		return err
	}
	if err := e.Identity.validate("identity"); err != nil {
		return err
	}
	if err := checkQualified("question_id", e.QuestionID, e.Identity); err != nil {
		return err
	}
	if e.Ticket == "" {
		return fieldErr("ticket", "required: an experiment answers for a change somebody made")
	}
	if e.ExpectedEffect == "" {
		return fieldErr("expected_effect", "required: without it there is nothing to observe and an absence means nothing")
	}
	if err := e.Observation.validate("observation"); err != nil {
		return err
	}
	if err := e.Evidence.validate("evidence"); err != nil {
		return err
	}
	// An omitted activation and an empty one assert the same thing — no
	// deploy, no restart, no commit — so they are refused the same way. Pending
	// has a floor of a commit time: a registration that names nothing at all is
	// a declaration missing, not a state.
	if e.Activation == nil {
		return fieldErr("activation", "%s", activationNamesNothing)
	}
	return e.Activation.validate("activation", e.Evidence.Baseline.Subject)
}

// activationNamesNothing is one message for both forms of the same claim.
const activationNamesNothing = "names neither an activation event nor a commit"

func (o Observation) validate(field string) error {
	d, err := time.ParseDuration(o.MinDuration)
	if err != nil {
		return fieldErr(field+".min_duration", "%q is not a duration: %v", o.MinDuration, err)
	}
	if d <= 0 {
		return fieldErr(field+".min_duration", "%s, must be positive", o.MinDuration)
	}
	if o.Opportunity == "" {
		return fieldErr(field+".opportunity", "required: what counts as a chance for the effect to show")
	}
	if o.MinOpportunities <= 0 {
		return fieldErr(field+".min_opportunities", "%d, must be positive: elapsed time alone is not opportunity", o.MinOpportunities)
	}
	return nil
}

func (r Requirements) validate(field string) error {
	if len(r.Readings) == 0 {
		return fieldErr(field+".readings", "required: the readings that must be present to judge the experiment")
	}
	declared := make(map[string]bool, len(r.Readings))
	for i, name := range r.Readings {
		if name == "" {
			return fieldErr(field+".readings", "[%d] is empty", i)
		}
		if declared[name] {
			return fieldErr(field+".readings", "[%d] repeats %q", i, name)
		}
		declared[name] = true
	}
	if r.Baseline == nil {
		return fieldErr(field+".baseline", "required: the method revision, subject instance and scope the answer will be measured against")
	}
	return r.Baseline.validate(field + ".baseline")
}

func (a *Activation) validate(field string, subject Subject) error {
	// A registration that names only the commit is the pending case: the change
	// exists and has not been observed running. Anything past that has to be a
	// complete activation event, since a half-filled one would date the window.
	if a.Kind == "" && a.At == "" {
		if a.CommitTime == "" {
			return fieldErr(field, "%s", activationNamesNothing)
		}
		return checkTimestamp(field+".commit_time", a.CommitTime)
	}
	if a.Kind != ActivationDeployment && a.Kind != ActivationRestart {
		return fieldErr(field+".kind", "%q is not an activation; one of %s, %s (a commit is not an activation)", a.Kind, ActivationDeployment, ActivationRestart)
	}
	if err := checkTimestamp(field+".at", a.At); err != nil {
		return err
	}
	if a.Instance != subject.Instance {
		return fieldErr(field+".instance", "%q, the subject under the baseline is %q", a.Instance, subject.Instance)
	}
	if a.Reference == "" {
		return fieldErr(field+".reference", "required: the deploy or restart the claim rests on")
	}
	if a.CommitTime != "" {
		if err := checkTimestamp(field+".commit_time", a.CommitTime); err != nil {
			return err
		}
	}
	return nil
}

// checkTimestamp refuses a bare date and a naive timestamp everywhere a time
// appears, for the same reason Window does: an instant with no offset is not an
// instant, and a notebook full of them cannot be ordered.
func checkTimestamp(field, value string) error {
	if value == "" {
		return fieldErr(field, "required")
	}
	if _, err := time.Parse(time.RFC3339, value); err != nil {
		return fieldErr(field, "%q is not an RFC3339 timestamp with an offset", value)
	}
	return nil
}

func validateState(field, state string) error {
	switch state {
	case StateObserving, StateAnswered, StateRetired:
		return nil
	}
	return fieldErr(field, "%q, one of %s, %s, %s", state, StateObserving, StateAnswered, StateRetired)
}

func (q *Question) clone() *Question {
	if q == nil {
		return nil
	}
	out := *q
	return &out
}

func (e *Experiment) clone() *Experiment {
	if e == nil {
		return nil
	}
	out := *e
	if e.Activation != nil {
		activation := *e.Activation
		out.Activation = &activation
	}
	out.Evidence.Readings = append([]string(nil), e.Evidence.Readings...)
	out.Evidence.Baseline = e.Evidence.Baseline.clone()
	return &out
}
