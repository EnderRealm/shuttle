// Package sandbox generates the per-run srt profile — deny-then-allow
// reads, writes confined to the run's out/, tmp/ and claude-home/, and the
// allowed-domain list — and wraps the runtime invocation in srt.
//
// This is the OS boundary the runtime package is not: Launcher returns the
// argv prefix `srt -s <run>/srt.json --` that claude.Options.Launcher takes,
// and WriteProfile puts the document that prefix names on disk before it runs.
// srt 1.0.0 refuses to start on a missing, malformed or schema-invalid profile
// rather than falling back to its default configuration, so Check — the §7.5
// preflight — is what turns that refusal into a §7.6 sandbox class before any
// model spend. docs/sandbox.md records what was measured on the host, and the
// residuals the profile does not close.
//
// Hosted weft segments do not run under srt; their boundary is the agent user
// (docs/agent-users.md). What they take from this package is worktree.go's
// path derivation and repository identity.
package sandbox
