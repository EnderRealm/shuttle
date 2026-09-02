// Package ledger owns runs.db (modernc.org/sqlite): the runs, agents and
// incidents tables, the claimed/running/completed/failed/unknown/skipped
// state machine, unique slot claims, failure streaks, incident upserts
// and auto-pause, behind loom's two-way schema version guard.
package ledger
