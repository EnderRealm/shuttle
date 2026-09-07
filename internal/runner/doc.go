// Package runner drives one run end to end: the run directory, preflight,
// the input providers, the rendered prompt, the sandboxed runtime, outcome
// classification, out/ validation, output application and delivery, with
// the ledger row updated at each step.
//
// Built so far: DryRun, which is that sequence up to the prompt (§7.1 steps 1
// to 4) and no further — providers, documents and prompt.md, no model call and
// no ledger row.
package runner
