// Package sandbox generates the per-run srt profile — deny-then-allow
// reads, writes confined to the run's out/, tmp/ and claude-home/, and the
// allowed-domain list — and wraps the runtime invocation in srt.
package sandbox
