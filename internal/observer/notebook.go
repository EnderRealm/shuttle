package observer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Memory is the reference notebook: the common validation and state-transition
// rules shuttle supplies, with the persistence a domain adapter would provide
// left as maps. Both domain fixtures are exercised against it, so a domain
// adapter has one implementation to match rather than a prose description.
//
// Safe for concurrent use. Claim and Retire take the same lock, which is what
// makes the order between them decidable rather than a race: whichever the lock
// admits first is the one that happened, and the other sees its result.
type Memory struct {
	mu       sync.Mutex
	revision int64
	entries  map[string]*Entry
	actions  map[string]*Action
	receipts map[string]*Receipt
	requests map[string]*applied
}

// applied is the outcome of a request, kept under its request id so a replay
// returns what the first attempt produced rather than deriving it again.
type applied struct {
	fingerprint string
	entry       *Entry
	receipt     *Receipt
	retirement  *Retirement
}

var _ Notebook = (*Memory)(nil)

// NewMemory returns an empty notebook at revision 0.
func NewMemory() *Memory {
	return &Memory{
		entries:  map[string]*Entry{},
		actions:  map[string]*Action{},
		receipts: map[string]*Receipt{},
		requests: map[string]*applied{},
	}
}

func (m *Memory) Revision() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revision
}

func (m *Memory) Entry(id string) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[id]
	if !ok {
		return nil, &Error{Field: "id", Err: fmt.Errorf("%q: %w", id, ErrNoEntry)}
	}
	return snapshot(e), nil
}

func (m *Memory) Register(req Request, q *Question) (*Entry, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(q.Identity); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "register", q)
	if err != nil || prev != nil {
		return entryOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	if held, ok := m.entries[q.ID]; ok {
		return nil, fieldErr("id", "%q is already registered by %s/%s", q.ID, held.Identity.Domain, held.Identity.Observer)
	}
	// The stored question is the notebook's own copy: retaining the caller's
	// pointer would leave a way around admission, validation and the revision.
	e := &Entry{ID: q.ID, Identity: q.Identity, Question: q.clone(), State: StateObserving}
	m.entries[q.ID] = e
	stored := &applied{entry: snapshot(e)}
	m.commit(req, "register", q, stored)
	return entryOf(stored), nil
}

func (m *Memory) Activate(req Request, exp *Experiment) (*Entry, error) {
	if err := exp.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(exp.Identity); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "activate", exp)
	if err != nil || prev != nil {
		return entryOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	e, err := m.open(exp.QuestionID, "question_id")
	if err != nil {
		return nil, err
	}
	// The experiment is about the question's subject; an activation on another
	// instance is about another subject and would answer for the wrong one.
	if e.Question != nil && exp.Evidence.Baseline.Subject != e.Question.Subject {
		return nil, fieldErr("evidence.baseline.subject", "%s %s, the question is about %s %s", exp.Evidence.Baseline.Subject.Kind, exp.Evidence.Baseline.Subject.Instance, e.Question.Subject.Kind, e.Question.Subject.Instance)
	}
	// A pending registration is replaced by the live one when the deploy
	// happens. A live one is not: re-dating a window already being measured
	// against would silently discard the opportunity already accrued.
	if e.ExperimentStatus == ExperimentLive {
		return nil, fieldErr("activation", "%q already carries a %s experiment; reopen the question for a new one", e.ID, ExperimentLive)
	}
	e.Experiment = exp.clone()
	e.ExperimentStatus = exp.Status()
	stored := &applied{entry: snapshot(e)}
	m.commit(req, "activate", exp, stored)
	return entryOf(stored), nil
}

func (m *Memory) Record(req Request, a *Assessment) (*Entry, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(a.Identity); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "record", a)
	if err != nil || prev != nil {
		return entryOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	// A finding opens its entry; the entry is not stored until the rules below
	// have admitted the observation, so a refusal leaves no half-open question.
	var e *Entry
	opened := false
	if a.Finding != nil {
		if held, ok := m.entries[a.EntryID()]; ok {
			return nil, fieldErr("finding.id", "%q is already an entry registered by %s/%s; record against question_id", held.ID, held.Identity.Domain, held.Identity.Observer)
		}
		e = &Entry{ID: a.EntryID(), Identity: a.Identity, Finding: a.Finding.clone(), State: StateObserving}
		opened = true
	} else {
		e, err = m.open(a.EntryID(), "question_id")
		if err != nil {
			return nil, err
		}
	}
	// The reading has to be of the entry's subject. Nothing downstream checks
	// this for a standing question, a finding, or an insufficient-evidence
	// outcome, so a reading of another instance would file silently here.
	subject := e.subject()
	if a.Evidence.Subject != subject {
		return nil, fieldErr("evidence.subject", "%s %s, the entry is about %s %s", a.Evidence.Subject.Kind, a.Evidence.Subject.Instance, subject.Kind, subject.Instance)
	}
	if err := checkRequirements(e, a); err != nil {
		return nil, err
	}
	state, err := TransitionFrom(e.State, a.Outcome)
	if err != nil {
		return nil, err
	}
	if opened {
		m.entries[e.ID] = e
	}
	// Append-only: a second reading of the same window is another row, never a
	// correction of the first, and the row is the notebook's copy of it.
	e.Observations = append(e.Observations, a.clone())
	e.State = state
	stored := &applied{entry: snapshot(e)}
	m.commit(req, "record", a, stored)
	return entryOf(stored), nil
}

// subject is the one path from either entry shape to the instance its
// observations must read.
func (e *Entry) subject() Subject {
	if e.Question != nil {
		return e.Question.Subject
	}
	return e.Finding.Subject
}

func (m *Memory) Claim(req Request, act *Action) (*Receipt, error) {
	if err := act.validate(); err != nil {
		return nil, err
	}
	if err := req.validate(act.Identity); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "claim", act)
	if err != nil || prev != nil {
		return receiptOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	e, err := m.open(act.QuestionID, "question_id")
	if err != nil {
		return nil, err
	}
	if held, ok := m.receipts[act.ID]; ok {
		return nil, fieldErr("id", "%q is already %s", act.ID, held.Status)
	}
	rec := &Receipt{
		Envelope: Envelope{ContractVersion: ContractVersion},
		Identity: act.Identity,
		ActionID: act.ID,
		Status:   ReceiptInFlight,
		At:       now(),
	}
	m.actions[act.ID] = act.clone()
	m.receipts[act.ID] = rec
	e.InFlight = append(e.InFlight, act.ID)
	stored := &applied{receipt: rec.clone()}
	m.commit(req, "claim", act, stored)
	return receiptOf(stored), nil
}

func (m *Memory) Settle(req Request, rec *Receipt) (*Receipt, error) {
	if err := rec.validate(); err != nil {
		return nil, err
	}
	if rec.Status != ReceiptSettled {
		return nil, fieldErr("status", "%q, settling records the outcome of a claim", rec.Status)
	}
	if err := req.validate(rec.Identity); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "settle", rec)
	if err != nil || prev != nil {
		return receiptOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	held, ok := m.receipts[rec.ActionID]
	if !ok {
		return nil, &Error{Field: "action_id", Err: fmt.Errorf("%q: %w", rec.ActionID, ErrNotClaimed)}
	}
	if held.Status == ReceiptSettled {
		return nil, fieldErr("action_id", "%q already settled as %s", rec.ActionID, held.Outcome)
	}
	// A retired question still settles its in-flight actions: the write may
	// already have landed, and reconciling it under the same id is how that is
	// recognised rather than written twice. The entry's state is not touched.
	*held = *rec.clone()
	act := m.actions[rec.ActionID]
	if e, ok := m.entries[act.QuestionID]; ok {
		e.InFlight = remove(e.InFlight, rec.ActionID)
	}
	stored := &applied{receipt: held.clone()}
	m.commit(req, "settle", rec, stored)
	return receiptOf(stored), nil
}

func (m *Memory) Retire(req Request, entryID, reason string) (*Retirement, error) {
	if err := req.validate(req.Identity); err != nil {
		return nil, err
	}
	if err := checkQualified("entry_id", entryID, req.Identity); err != nil {
		return nil, err
	}
	if reason == "" {
		return nil, fieldErr("reason", "required: retirement is a decision somebody took, not a deadline that passed")
	}
	payload := struct {
		EntryID string
		Reason  string
	}{entryID, reason}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "retire", payload)
	if err != nil || prev != nil {
		return retirementOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	e, err := m.open(entryID, "entry_id")
	if err != nil {
		return nil, err
	}
	e.State = StateRetired
	ret := &Retirement{
		EntryID:  e.ID,
		Reason:   reason,
		At:       now(),
		InFlight: append([]string(nil), e.InFlight...),
		// The retirement takes effect at the revision the commit below
		// advances to.
		Revision: m.revision + 1,
	}
	e.Retirement = ret
	stored := &applied{retirement: ret.clone()}
	m.commit(req, "retire", payload, stored)
	return retirementOf(stored), nil
}

func (m *Memory) Reopen(req Request, entryID, condition string) (*Entry, error) {
	if err := req.validate(req.Identity); err != nil {
		return nil, err
	}
	if err := checkQualified("entry_id", entryID, req.Identity); err != nil {
		return nil, err
	}
	payload := struct {
		EntryID   string
		Condition string
	}{entryID, condition}
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, err := m.replay(req, "reopen", payload)
	if err != nil || prev != nil {
		return entryOf(prev), err
	}
	if err := m.checkRevision(req); err != nil {
		return nil, err
	}
	e, ok := m.entries[entryID]
	if !ok {
		return nil, &Error{Field: "entry_id", Err: fmt.Errorf("%q: %w", entryID, ErrNoEntry)}
	}
	state, err := ReopenFrom(e.State, condition)
	if err != nil {
		return nil, err
	}
	e.State = state
	stored := &applied{entry: snapshot(e)}
	m.commit(req, "reopen", payload, stored)
	return entryOf(stored), nil
}

// open returns an entry that still admits writes, or the refusal that says why
// it does not.
func (m *Memory) open(id, field string) (*Entry, error) {
	e, ok := m.entries[id]
	if !ok {
		return nil, &Error{Field: field, Err: fmt.Errorf("%q: %w", id, ErrNoEntry)}
	}
	if e.State == StateRetired {
		return nil, &Error{Field: field, Err: fmt.Errorf("%q: %w", id, ErrRetired)}
	}
	return e, nil
}

// replay returns the outcome already recorded under the request id, or nil when
// the request is new. It runs before the revision check because a replay after
// an uncertain response carries the revision the caller last saw, which is by
// definition the one its own first attempt superseded.
func (m *Memory) replay(req Request, op string, payload any) (*applied, error) {
	prev, ok := m.requests[requestKey(req)]
	if !ok {
		return nil, nil
	}
	if prev.fingerprint != fingerprint(op, payload) {
		return nil, &Error{Field: "request.request_id", Err: fmt.Errorf("%q: %w", req.RequestID, ErrRequestConflict)}
	}
	return prev, nil
}

// commit fixes a request's outcome under its id and advances the revision.
func (m *Memory) commit(req Request, op string, payload any, out *applied) {
	out.fingerprint = fingerprint(op, payload)
	m.requests[requestKey(req)] = out
	m.revision++
}

// requestKey scopes the request log by the requester, like every other name in
// the contract. Request ids are the caller's own strings, so two observers
// sharing a notebook would otherwise collide on a natural id like "record-1"
// and each read the other's outcome as a conflict it could never clear.
func requestKey(req Request) string { return req.Identity.Qualify(req.RequestID) }

func (m *Memory) checkRevision(req Request) error {
	if req.Revision != m.revision {
		return &Error{Field: "request.revision", Err: fmt.Errorf("%d, the notebook is at %d: %w", req.Revision, m.revision, ErrStaleRevision)}
	}
	return nil
}

func (r Request) validate(owner Identity) error {
	if r.RequestID == "" {
		return fieldErr("request.request_id", "required: it is what makes a replay after an uncertain response idempotent")
	}
	if err := r.Identity.validate("request.identity"); err != nil {
		return err
	}
	if r.Identity != owner {
		return fieldErr("request.identity", "%s/%s, the payload is by %s/%s", r.Identity.Domain, r.Identity.Observer, owner.Domain, owner.Observer)
	}
	return nil
}

// checkRequirements holds a conclusive outcome to what the experiment declared.
// This is the rule shuttle supplies rather than the adapter: a "no effect" read
// before the change was live, over too short a window, or over fewer
// opportunities than the experiment asked for, is insufficient-evidence wearing
// a label it did not earn.
func checkRequirements(e *Entry, a *Assessment) error {
	if a.Outcome == OutcomeInsufficient {
		return nil
	}
	if e.Experiment == nil {
		// A question with no experiment declared no expected effect, so there
		// is nothing for the opportunity to have been sufficient against.
		if a.Outcome == OutcomeEffectAbsent {
			return fieldErr("outcome", "%q needs an experiment declaring the effect and the opportunity it is absent against", a.Outcome)
		}
		return nil
	}
	exp := e.Experiment
	if err := wrapField("evidence", a.Evidence.Comparable(exp.Evidence.Baseline)); err != nil {
		return err
	}
	for _, name := range exp.Evidence.Readings {
		if !hasValue(a.Evidence, name) {
			return fieldErr("evidence.readings", "%q is required by the experiment and carries no value", name)
		}
	}
	if exp.Status() != ExperimentLive {
		return fieldErr("outcome", "%q needs a live activation; the experiment is %s", a.Outcome, ExperimentPending)
	}
	if a.Outcome != OutcomeEffectAbsent {
		return nil
	}
	at, err := exp.ActivatedAt()
	if err != nil {
		return err
	}
	start, end, err := a.Evidence.Window.bounds("evidence.window")
	if err != nil {
		return err
	}
	// Evidence taken before the change was live says nothing about the change.
	if start.Before(at) {
		return fieldErr("evidence.window.start", "%s precedes the activation at %s", a.Evidence.Window.Start, exp.Activation.At)
	}
	minDuration, err := time.ParseDuration(exp.Observation.MinDuration)
	if err != nil {
		return fieldErr("observation.min_duration", "%q is not a duration: %v", exp.Observation.MinDuration, err)
	}
	if end.Sub(start) < minDuration {
		return fieldErr("evidence.window", "covers %s, the experiment declared %s", end.Sub(start), exp.Observation.MinDuration)
	}
	if a.Evidence.Coverage.Observed < exp.Observation.MinOpportunities {
		return fieldErr("evidence.coverage.observed", "%d %s, the experiment declared %d", a.Evidence.Coverage.Observed, exp.Observation.Opportunity, exp.Observation.MinOpportunities)
	}
	return nil
}

func hasValue(b *Bundle, name string) bool {
	for _, r := range b.Readings {
		if r.Name == name {
			return r.Value != nil
		}
	}
	return false
}

// snapshot is the copy a caller may hold: deep, down to every payload the entry
// points at. Copying only the slice headers would leave the stored assessments
// reachable through the returned ones, and a history a caller can edit in place
// is not append-only.
func snapshot(e *Entry) *Entry {
	out := *e
	out.Question = e.Question.clone()
	out.Finding = e.Finding.clone()
	out.Experiment = e.Experiment.clone()
	out.Retirement = e.Retirement.clone()
	out.InFlight = append([]string(nil), e.InFlight...)
	if e.Observations != nil {
		out.Observations = make([]*Assessment, len(e.Observations))
		for i, a := range e.Observations {
			out.Observations[i] = a.clone()
		}
	}
	return &out
}

// fingerprint identifies the content a request id was applied with, so a replay
// can be told from a reuse.
func fingerprint(op string, payload any) string {
	data, err := json.Marshal(payload)
	if err != nil {
		// Unreachable for the contract types, which are plain structs; a
		// fingerprint that could not be taken must not compare equal to a
		// later one that could.
		return op + ":" + err.Error()
	}
	return op + ":" + string(data)
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

// remove allocates rather than filtering in place: every slice here is either
// the notebook's own or a copy handed to a caller, and the package's rule is
// that neither is written through an alias.
func remove(ids []string, id string) []string {
	out := make([]string, 0, len(ids))
	for _, held := range ids {
		if held != id {
			out = append(out, held)
		}
	}
	return out
}

func entryOf(prev *applied) *Entry {
	if prev == nil {
		return nil
	}
	return snapshot(prev.entry)
}

func receiptOf(prev *applied) *Receipt {
	if prev == nil {
		return nil
	}
	return prev.receipt.clone()
}

func retirementOf(prev *applied) *Retirement {
	if prev == nil {
		return nil
	}
	return prev.retirement.clone()
}
