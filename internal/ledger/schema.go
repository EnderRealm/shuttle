package ledger

// schemaVersion is the on-disk schema this binary reads and writes. Bumping it
// means writing a migration: unlike loom's summary DB, runs.db is the only
// record that a run happened and what it cost, so it is never rebuilt.
//
// schemaVersion 1: the §6 tables — runs, agents, incidents — with the unique
// slot index that makes a second claim of the same slot a no-op and the
// partial unique index that keeps one open incident per (agent, signature).
const schemaVersion = 1

// schemaSQL is the §6 DDL. IF NOT EXISTS throughout because Open applies it on
// every open, guarded by the version check rather than by a migration table.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS runs (
  id              TEXT PRIMARY KEY,      -- <agent>-<YYYYMMDD-HHMMSS>-<4 hex>
  agent           TEXT NOT NULL,
  trigger_kind    TEXT NOT NULL,         -- cron | manual
  scheduled_for   TEXT NOT NULL,         -- the slot, RFC3339 with offset
  claimed_at      TEXT NOT NULL,
  started_at      TEXT,
  ended_at        TEXT,
  status          TEXT NOT NULL,         -- claimed | running | completed | failed | unknown | skipped
  skip_reason     TEXT,                  -- missed | overlap | paused | limit
  failure_class   TEXT,                  -- §7.6
  error_signature TEXT,                  -- normalised first line, for incidents
  pid             INTEGER,
  run_dir         TEXT,
  session_id      TEXT,
  num_turns       INTEGER,
  cost_usd        REAL,
  input_tokens    INTEGER,
  output_tokens   INTEGER,
  silent          INTEGER NOT NULL DEFAULT 0,
  outputs_applied TEXT,                  -- JSON: what §9 applied, with paths and ids
  delivery_status TEXT,                  -- pending | delivered | failed | skipped
  delivery_error  TEXT,
  delivered_at    TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS runs_agent_slot ON runs(agent, scheduled_for, trigger_kind);
CREATE INDEX IF NOT EXISTS runs_agent_started ON runs(agent, started_at);

CREATE TABLE IF NOT EXISTS agents (
  name             TEXT PRIMARY KEY,
  last_fired_slot  TEXT,
  last_success_at  TEXT,
  failure_streak   INTEGER NOT NULL DEFAULT 0,
  paused           INTEGER NOT NULL DEFAULT 0,
  paused_reason    TEXT,
  paused_at        TEXT
);

CREATE TABLE IF NOT EXISTS incidents (
  id              INTEGER PRIMARY KEY,
  agent           TEXT NOT NULL,
  signature       TEXT NOT NULL,
  first_seen      TEXT NOT NULL,
  last_seen       TEXT NOT NULL,
  count           INTEGER NOT NULL,
  last_run_id     TEXT,
  acknowledged_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS incidents_open ON incidents(agent, signature) WHERE acknowledged_at IS NULL;
`
