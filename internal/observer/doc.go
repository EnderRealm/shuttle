// Package observer defines the interchange contract an observer and its domain
// adapter share: versioned evidence bundles, standing questions and the
// experiments that make a change answerable, assessments and proposed actions.
// Shuttle owns the wire format, the validation and the state-transition rules;
// a domain owns its notebook and persists them. Memory is the reference
// notebook both are checked against. docs/observer-contract.md carries the
// prose the types cannot: evidence against generated interpretation, domain
// ownership, retention, and how an experiment is registered.
package observer
