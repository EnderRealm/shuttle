// Package output validates a run's out/ directory against the fixed
// report.md and outputs.json contract, renders markdown to HTML through
// goldmark with raw HTML disabled and off-list links rewritten to text,
// and writes the report into the reports repo.
package output
