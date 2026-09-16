package ledger

// schemaVersion is the on-disk schema this binary reads and writes. Bumping it
// means writing a migration: unlike loom's summary DB, runs.db is the only
// record that a run happened and what it cost, so it is never rebuilt.
//
// schemaVersion 1: the §6 tables — runs, agents and incidents.
// schemaVersion 2: the weft-remote/v1 durable host records.
const schemaVersion = 2

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

CREATE TABLE IF NOT EXISTS weft_runs (
  id              TEXT PRIMARY KEY,
  ticket          TEXT NOT NULL,
  project         TEXT NOT NULL,
  repo            TEXT NOT NULL,
  branch          TEXT NOT NULL,
  worktree        TEXT NOT NULL,
  base            TEXT,
  criteria        TEXT,
  phase           TEXT NOT NULL,
  outcome         TEXT,
  halt_stage      TEXT,
  halt_reason     TEXT,
  halt_detail     TEXT,
  head            TEXT,
  stopped         INTEGER NOT NULL DEFAULT 0,
  summary         TEXT,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  ended_at        TEXT
);
CREATE INDEX IF NOT EXISTS weft_runs_ticket ON weft_runs(ticket, created_at);

CREATE TABLE IF NOT EXISTS weft_segments (
  run_id          TEXT NOT NULL REFERENCES weft_runs(id),
  segment         INTEGER NOT NULL,
  kind            TEXT NOT NULL,
  ledger_run_id   TEXT NOT NULL REFERENCES runs(id),
  worker_run_id   TEXT,
  answer_id       TEXT,
  terminal        TEXT,
  failure         TEXT,
  PRIMARY KEY (run_id, segment)
);

CREATE TABLE IF NOT EXISTS weft_stages (
  run_id          TEXT NOT NULL,
  segment         INTEGER NOT NULL,
  stage           TEXT NOT NULL,
  occurrence      INTEGER NOT NULL,
  attempt         INTEGER NOT NULL,
  backend         TEXT,
  state           TEXT NOT NULL,
  session         TEXT,
  detail          TEXT,
  exit_code       INTEGER,
  turns           INTEGER,
  spend           TEXT,
  processing_ms   INTEGER,
  tokens          TEXT,
  at              TEXT NOT NULL,
  PRIMARY KEY (run_id, segment, stage, occurrence, attempt)
);

CREATE TABLE IF NOT EXISTS weft_questions (
  id              TEXT PRIMARY KEY,
  run_id          TEXT NOT NULL REFERENCES weft_runs(id),
  segment         INTEGER NOT NULL,
  stage           TEXT NOT NULL,
  occurrence      INTEGER NOT NULL,
  attempt         INTEGER NOT NULL,
  text            TEXT NOT NULL,
  asked_at        TEXT NOT NULL,
  state           TEXT NOT NULL,
  answer_id       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS weft_questions_pending ON weft_questions(run_id) WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS weft_answers (
  id              TEXT PRIMARY KEY,
  question_id     TEXT NOT NULL UNIQUE REFERENCES weft_questions(id),
  run_id          TEXT NOT NULL,
  text            TEXT NOT NULL,
  submitted_at    TEXT NOT NULL,
  segment         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS weft_events (
  seq             INTEGER PRIMARY KEY AUTOINCREMENT,
  ts              TEXT NOT NULL,
  run_id          TEXT NOT NULL,
  type            TEXT NOT NULL,
  payload         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weft_requests (
  token_id        TEXT NOT NULL,
  key             TEXT NOT NULL,
  method          TEXT NOT NULL,
  path            TEXT NOT NULL,
  body_sha256     TEXT NOT NULL,
  status          INTEGER NOT NULL,
  response        TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  PRIMARY KEY (token_id, key)
);

CREATE TABLE IF NOT EXISTS weft_repository_locks (
  repository      TEXT PRIMARY KEY,
  held_by_run     TEXT NOT NULL REFERENCES weft_runs(id),
  acquired_at     TEXT NOT NULL
);
`
