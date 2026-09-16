// Package runner drives one run end to end: the run directory, preflight,
// the input providers, the rendered prompt, the sandboxed runtime, outcome
// classification, out/ validation, output application and delivery, with
// the ledger row updated at each step.
//
// DryRun stops after providers, documents and prompt.md without a model call or
// ledger row. Pipeline drives an already claimed row through every live stage;
// RetryDelivery acts only on retained output. Preflight.Check performs the
// declared local requirements, AuthTick shares one sandboxed auth probe across
// a tick's claimed runs, and ConfigureOutcomes attaches configured notifications
// to ledger terminal writes.
package runner
