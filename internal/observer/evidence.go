package observer

import (
	"fmt"
	"time"
)

// Window is the span a reading covers, half-open [Start, End). Both bounds are
// RFC3339 with an offset and the zone is named as well: a bare date or a naive
// timestamp is refused, because "2026-09-01" is a different 24 hours in two
// zones and a window that cannot say which one it meant cannot be compared
// with anything. The zone is carried next to the offset because an offset says
// what the clock read and only the zone name says which day the subject lived.
type Window struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	Timezone string `json:"timezone"`
}

// Scope is what was read: the collection the reading came from and the
// snapshot of it that was read. The snapshot is what makes a reading
// reproducible — the same method over the same snapshot is the same number —
// and the collection is what stops a baseline taken over one store being
// compared with a reading taken over another.
type Scope struct {
	Collection string `json:"collection"`
	Snapshot   string `json:"snapshot"`
}

// Source is a reference the reading can be traced back to: a ticket, a commit,
// a path, a query. It is what a reader follows to check the number themselves.
type Source struct {
	Kind string `json:"kind"`
	Ref  string `json:"ref"`
}

// Unavailable is a source that could not be read, with the reason. It is
// recorded rather than dropped: a reading taken over three of four sources is
// not the same fact as one taken over four, and the difference is only visible
// if the fourth says why it is missing.
type Unavailable struct {
	Kind   string `json:"kind"`
	Ref    string `json:"ref"`
	Reason string `json:"reason"`
}

// Coverage is how much of the population the reading actually saw. A rate over
// six of six hundred rows and the same rate over six hundred are different
// evidence, and nothing downstream can tell them apart from the rate alone.
// Observed doubles as the opportunity count an experiment is judged against, so
// it has to be counted in the unit that experiment's Observation.Opportunity
// names: rows counted against a requirement stated in days would satisfy the
// number without satisfying the rule.
type Coverage struct {
	Observed  int `json:"observed"`
	Available int `json:"available"`
}

// Reading is one recorded quantity. Value is a pointer because a reading that
// could not be taken is not a reading of zero: the two would otherwise be the
// same JSON, and an absence would compare against a baseline as a fall to zero.
// Exactly one of Value and Unavailable is set.
type Reading struct {
	Name        string   `json:"name"`
	Value       *float64 `json:"value"`
	Unit        string   `json:"unit,omitempty"`
	Unavailable string   `json:"unavailable,omitempty"`
}

// Bundle is recorded fact and nothing else: what was read, over which window,
// by which method revision, out of which snapshot, from which sources, and
// what could not be read at all. The observer's reading of it lives in an
// Assessment, stored apart so a generated interpretation can never be quoted
// as the evidence it was drawn from.
type Bundle struct {
	Envelope
	Identity    Identity      `json:"identity"`
	Subject     Subject       `json:"subject"`
	Method      Method        `json:"method"`
	Window      Window        `json:"window"`
	Scope       Scope         `json:"scope"`
	Sources     []Source      `json:"sources"`
	Unavailable []Unavailable `json:"unavailable,omitempty"`
	Coverage    Coverage      `json:"coverage"`
	Readings    []Reading     `json:"readings"`
}

// Baseline is the earlier reading a bundle is measured against, carrying the
// identity of how it was taken. Comparability is checked rather than assumed:
// a delta across a changed method revision, a different subject instance or
// another collection is a number about the measurement, not about the subject.
type Baseline struct {
	Subject  Subject   `json:"subject"`
	Method   Method    `json:"method"`
	Scope    Scope     `json:"scope"`
	Window   Window    `json:"window"`
	Source   string    `json:"source"`
	Readings []Reading `json:"readings"`
}

// Delta is one reading measured against its baseline.
type Delta struct {
	Name   string
	From   float64
	To     float64
	Change float64
}

// Comparison is what Compare produces. Missing names the readings that could
// not be measured because one side had no value; they are listed rather than
// compared, since an absence is not a zero and a delta computed from one is a
// fabricated number.
type Comparison struct {
	Deltas  []Delta
	Missing []string
}

// bounds parses the window. RFC3339 refuses a bare date and a naive timestamp
// on its own; the zone is loaded as well so an unknown or host-relative zone
// name is caught here rather than at the point somebody renders the number.
func (w Window) bounds(field string) (time.Time, time.Time, error) {
	start, err := time.Parse(time.RFC3339, w.Start)
	if err != nil {
		return time.Time{}, time.Time{}, fieldErr(field+".start", "%q is not an RFC3339 timestamp with an offset", w.Start)
	}
	end, err := time.Parse(time.RFC3339, w.End)
	if err != nil {
		return time.Time{}, time.Time{}, fieldErr(field+".end", "%q is not an RFC3339 timestamp with an offset", w.End)
	}
	if _, err := time.LoadLocation(w.Timezone); err != nil {
		return time.Time{}, time.Time{}, fieldErr(field+".timezone", "%q is not a loadable IANA zone: %v", w.Timezone, err)
	}
	return start, end, nil
}

func (w Window) validate(field string) error {
	if w.Timezone == "" {
		return fieldErr(field+".timezone", "required: an IANA zone name, so the window says which day it covered")
	}
	// Local resolves against whatever host reads the notebook, which makes the
	// same window mean two things on two machines.
	if w.Timezone == "Local" {
		return fieldErr(field+".timezone", "%q is host-relative; name the IANA zone", w.Timezone)
	}
	start, end, err := w.bounds(field)
	if err != nil {
		return err
	}
	if !end.After(start) {
		return fieldErr(field+".end", "%s is not after start %s", w.End, w.Start)
	}
	return nil
}

func (s Scope) validate(field string) error {
	if s.Collection == "" {
		return fieldErr(field+".collection", "required")
	}
	if s.Snapshot == "" {
		return fieldErr(field+".snapshot", "required: the revision of the collection that was read")
	}
	return nil
}

func (r Reading) validate(field string) error {
	if r.Name == "" {
		return fieldErr(field+".name", "required")
	}
	// The two states are exclusive so that a missing reading can never be read
	// as a value, nor a value explained away as missing.
	if r.Value == nil && r.Unavailable == "" {
		return fieldErr(field, "%q has neither a value nor an unavailable reason", r.Name)
	}
	if r.Value != nil && r.Unavailable != "" {
		return fieldErr(field, "%q carries both a value and an unavailable reason", r.Name)
	}
	if r.Value != nil && r.Unit == "" {
		return fieldErr(field+".unit", "required for %q, which has a value", r.Name)
	}
	return nil
}

func (c Coverage) validate(field string, unavailable int) error {
	if c.Observed < 0 || c.Available < 0 {
		return fieldErr(field, "observed %d of %d, both must be counts", c.Observed, c.Available)
	}
	if c.Observed > c.Available {
		return fieldErr(field, "observed %d of %d available", c.Observed, c.Available)
	}
	// Nothing available and nothing missing is a claim no reader can act on:
	// either the population was empty for a reason, or a source failed.
	if c.Available == 0 && unavailable == 0 {
		return fieldErr(field+".available", "0 with no unavailable source to explain it")
	}
	return nil
}

// validate holds a bundle to §2 of docs/observer-contract.md: who observed what, by which
// method revision, over which explicit window and snapshot, out of which
// sources, and with what it could not read named rather than omitted.
func (b *Bundle) validate() error {
	if err := b.checkVersion(); err != nil {
		return err
	}
	if err := b.Identity.validate("identity"); err != nil {
		return err
	}
	if err := b.Subject.validate("subject"); err != nil {
		return err
	}
	if err := b.Method.validate("method"); err != nil {
		return err
	}
	if err := b.Window.validate("window"); err != nil {
		return err
	}
	if err := b.Scope.validate("scope"); err != nil {
		return err
	}
	if len(b.Sources) == 0 && len(b.Unavailable) == 0 {
		return fieldErr("sources", "required: a reading traceable to nothing cannot be checked")
	}
	for i, s := range b.Sources {
		if s.Kind == "" || s.Ref == "" {
			return fieldErr(fmt.Sprintf("sources[%d]", i), "kind and ref are required")
		}
	}
	for i, u := range b.Unavailable {
		if u.Kind == "" || u.Ref == "" || u.Reason == "" {
			return fieldErr(fmt.Sprintf("unavailable[%d]", i), "kind, ref and reason are required")
		}
	}
	if err := b.Coverage.validate("coverage", len(b.Unavailable)); err != nil {
		return err
	}
	if len(b.Readings) == 0 {
		return fieldErr("readings", "required: at least one, even if it is an unavailable one")
	}
	// Reading names are keys: an experiment's requirements name one, a
	// comparison maps them. A repeat makes both answer by position instead.
	seen := make(map[string]bool, len(b.Readings))
	for i, r := range b.Readings {
		if err := r.validate(fmt.Sprintf("readings[%d]", i)); err != nil {
			return err
		}
		if seen[r.Name] {
			return fieldErr(fmt.Sprintf("readings[%d].name", i), "%q appears twice", r.Name)
		}
		seen[r.Name] = true
	}
	return nil
}

func (base *Baseline) validate(field string) error {
	if err := base.Subject.validate(field + ".subject"); err != nil {
		return err
	}
	if err := base.Method.validate(field + ".method"); err != nil {
		return err
	}
	if err := base.Scope.validate(field + ".scope"); err != nil {
		return err
	}
	if err := base.Window.validate(field + ".window"); err != nil {
		return err
	}
	if base.Source == "" {
		return fieldErr(field+".source", "required: where the baseline was reported")
	}
	if len(base.Readings) == 0 {
		return fieldErr(field+".readings", "required")
	}
	seen := make(map[string]bool, len(base.Readings))
	for i, r := range base.Readings {
		if err := r.validate(fmt.Sprintf("%s.readings[%d]", field, i)); err != nil {
			return err
		}
		if seen[r.Name] {
			return fieldErr(fmt.Sprintf("%s.readings[%d].name", field, i), "%q appears twice", r.Name)
		}
		seen[r.Name] = true
	}
	return nil
}

// Comparable reports why the bundle cannot be measured against base, naming
// the field that differs, or nil when it can.
func (b *Bundle) Comparable(base *Baseline) error {
	if base == nil {
		return fieldErr("baseline", "required: there is nothing to compare against")
	}
	if b.Subject != base.Subject {
		return fieldErr("baseline.subject", "taken on %s %s, the reading is of %s %s", base.Subject.Kind, base.Subject.Instance, b.Subject.Kind, b.Subject.Instance)
	}
	if b.Method != base.Method {
		return fieldErr("baseline.method", "taken by %s %s, the reading is by %s %s", base.Method.Name, base.Method.Revision, b.Method.Name, b.Method.Revision)
	}
	if b.Scope.Collection != base.Scope.Collection {
		return fieldErr("baseline.scope.collection", "taken over %q, the reading is over %q", base.Scope.Collection, b.Scope.Collection)
	}
	return nil
}

// Compare measures the bundle against a baseline, refusing an incompatible one
// rather than returning a delta nobody could read.
func (b *Bundle) Compare(base *Baseline) (*Comparison, error) {
	if err := b.Comparable(base); err != nil {
		return nil, err
	}
	values := make(map[string]*float64, len(base.Readings))
	for _, r := range base.Readings {
		values[r.Name] = r.Value
	}
	c := &Comparison{}
	read := make(map[string]bool, len(b.Readings))
	for _, r := range b.Readings {
		read[r.Name] = true
		from, ok := values[r.Name]
		if !ok || from == nil || r.Value == nil {
			c.Missing = append(c.Missing, r.Name)
			continue
		}
		c.Deltas = append(c.Deltas, Delta{Name: r.Name, From: *from, To: *r.Value, Change: *r.Value - *from})
	}
	// A reading the baseline carries and the bundle omits entirely is missing
	// on the same terms; dropping it would silently shorten the comparison
	// rather than report what was not measured this time.
	for _, r := range base.Readings {
		if !read[r.Name] {
			c.Missing = append(c.Missing, r.Name)
		}
	}
	return c, nil
}

// clone copies a reading, including the value behind the pointer that
// distinguishes an absent reading from a zero one.
func (r Reading) clone() Reading {
	if r.Value != nil {
		v := *r.Value
		r.Value = &v
	}
	return r
}

func cloneReadings(in []Reading) []Reading {
	if in == nil {
		return nil
	}
	out := make([]Reading, len(in))
	for i, r := range in {
		out[i] = r.clone()
	}
	return out
}

// clone is a deep copy. The notebook takes one on the way in and another on the
// way out, so neither the caller's pointer nor the one it is handed back ever
// reaches a stored record: a history a caller can edit in place is not
// append-only, whatever the protocol says.
func (b *Bundle) clone() *Bundle {
	if b == nil {
		return nil
	}
	out := *b
	out.Sources = append([]Source(nil), b.Sources...)
	out.Unavailable = append([]Unavailable(nil), b.Unavailable...)
	out.Readings = cloneReadings(b.Readings)
	return &out
}

func (base *Baseline) clone() *Baseline {
	if base == nil {
		return nil
	}
	out := *base
	out.Readings = cloneReadings(base.Readings)
	return &out
}
