package ledger

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Run statuses, §6.1. claimed → running → completed | failed, plus unknown
// (the daemon restarted over it) and skipped (it was never launched).
const (
	StatusClaimed   = "claimed"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusUnknown   = "unknown"
	StatusSkipped   = "skipped"
)

// Trigger kinds, §5.
const (
	TriggerCron   = "cron"
	TriggerManual = "manual"
)

// Skip reasons, §5.
const (
	SkipMissed  = "missed"
	SkipOverlap = "overlap"
	SkipPaused  = "paused"
	SkipLimit   = "limit"
)

// Delivery statuses. A separate column from the run status on purpose (§6.1):
// a run whose report was produced but whose email failed is completed with a
// failed delivery, and retrying delivery must not re-run the model.
const (
	DeliveryPending   = "pending"
	DeliveryDelivered = "delivered"
	DeliveryFailed    = "failed"
	DeliverySkipped   = "skipped"
)

// Failure classes, §7.6. Nothing is ever classified as "flake".
const (
	ClassPreflight = "preflight"
	ClassAuth      = "auth"
	ClassProvider  = "provider"
	ClassDied      = "died"
	ClassNoResult  = "no_result"
	ClassStuck     = "stuck"
	ClassBudget    = "budget"
	ClassOutput    = "output"
	ClassSandbox   = "sandbox"
)

// runColumns is the select list every read shares, in scanRun's order.
const runColumns = `id, agent, trigger_kind, scheduled_for, claimed_at, started_at, ended_at,
	status, skip_reason, failure_class, error_signature, pid, run_dir, session_id,
	num_turns, cost_usd, input_tokens, output_tokens, silent, outputs_applied,
	delivery_status, delivery_error, delivered_at`

// Run is one row of the runs table. The optional columns are nullable rather
// than zero-valued: §12 turns on "reported no cost" being distinguishable from
// a cost of zero.
type Run struct {
	ID             string
	Agent          string
	TriggerKind    string
	ScheduledFor   string
	ClaimedAt      string
	StartedAt      sql.NullString
	EndedAt        sql.NullString
	Status         string
	SkipReason     sql.NullString
	FailureClass   sql.NullString
	ErrorSignature sql.NullString
	PID            sql.NullInt64
	RunDir         sql.NullString
	SessionID      sql.NullString
	NumTurns       sql.NullInt64
	CostUSD        sql.NullFloat64
	InputTokens    sql.NullInt64
	OutputTokens   sql.NullInt64
	Silent         bool
	OutputsApplied sql.NullString
	DeliveryStatus sql.NullString
	DeliveryError  sql.NullString
	DeliveredAt    sql.NullString
}

// Result is what the runtime reported, recorded on a terminal run row. A
// failed run may still carry cost: the budget stop and error_max_turns both
// report total_cost_usd (§7.4).
type Result struct {
	SessionID    string
	NumTurns     sql.NullInt64
	CostUSD      sql.NullFloat64
	InputTokens  sql.NullInt64
	OutputTokens sql.NullInt64
	Silent       bool
}

// Claim inserts the claimed row for a slot and advances the agent's
// last_fired_slot. A second claim of the same (agent, scheduled_for,
// trigger_kind) returns claimed=false and writes nothing — the unique index is
// the arbiter, deliberately not a SELECT-then-INSERT that two ticks could race.
func (s *Store) Claim(agent string, slot time.Time, triggerKind string) (*Run, bool, error) {
	return s.insertSlot(agent, slot, triggerKind, StatusClaimed, "")
}

// Skip records a slot that was never launched (§5): missed, overlapped, paused
// or over a limit. The row is terminal on insert, and it is subject to the same
// unique index as Claim, so a repeat is the same no-op.
func (s *Store) Skip(agent string, slot time.Time, triggerKind, reason string) (*Run, bool, error) {
	return s.insertSlot(agent, slot, triggerKind, StatusSkipped, reason)
}

func (s *Store) insertSlot(agent string, slot time.Time, triggerKind, status, skipReason string) (*Run, bool, error) {
	id, err := newRunID(agent, slot)
	if err != nil {
		return nil, false, err
	}
	run := &Run{
		ID:           id,
		Agent:        agent,
		TriggerKind:  triggerKind,
		ScheduledFor: slot.Format(time.RFC3339),
		ClaimedAt:    s.timestamp(),
		Status:       status,
		SkipReason:   nullString(skipReason),
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		`INSERT INTO runs(id, agent, trigger_kind, scheduled_for, claimed_at, status, skip_reason)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.Agent, run.TriggerKind, run.ScheduledFor, run.ClaimedAt, run.Status, run.SkipReason,
	)
	if isSlotTaken(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("claim %s slot %s: %w", agent, run.ScheduledFor, err)
	}
	// §5: last_fired_slot advances only on a successful claim. A skipped slot
	// leaves it where it was.
	if status == StatusClaimed {
		if _, err := tx.Exec(
			`INSERT INTO agents(name, last_fired_slot) VALUES (?, ?)
				ON CONFLICT(name) DO UPDATE SET last_fired_slot = excluded.last_fired_slot`,
			agent, run.ScheduledFor,
		); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return run, true, nil
}

// newRunID builds the §7.2 id: <agent>-<YYYYMMDD-HHMMSS>-<4 hex>. The
// timestamp is the slot rendered in the slot's own location, so the id reads
// as the wall-clock time the operator scheduled.
func newRunID(agent string, slot time.Time) (string, error) {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s-%s", agent, slot.Format("20060102-150405"), hex.EncodeToString(b[:])), nil
}

// MarkRunning records the launched process against a claimed run.
func (s *Store) MarkRunning(id string, pid int, runDir string) error {
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := requireStatus(tx, id, StatusRunning, StatusClaimed); err != nil {
			return err
		}
		_, err := tx.Exec(
			`UPDATE runs SET status = ?, started_at = ?, pid = ?, run_dir = ? WHERE id = ?`,
			StatusRunning, s.timestamp(), pid, runDir, id,
		)
		return err
	})
}

// Complete closes a run as successful: it resets the agent's failure streak
// and stamps last_success_at (§6.2).
func (s *Store) Complete(id string, res Result) error {
	now := s.timestamp()
	return s.withTx(func(tx *sql.Tx) error {
		agent, err := requireStatus(tx, id, StatusCompleted, StatusClaimed, StatusRunning)
		if err != nil {
			return err
		}
		if err := finishRun(tx, id, StatusCompleted, now, res, "", ""); err != nil {
			return err
		}
		return recordSuccess(tx, agent, now)
	})
}

// Fail closes a run as failed: it advances the streak, applies the §6.3
// auto-pause rules and upserts the incident for the normalised error.
func (s *Store) Fail(id, class, errText string, res Result) error {
	now := s.timestamp()
	signature := Signature(class, errText)
	return s.withTx(func(tx *sql.Tx) error {
		agent, err := requireStatus(tx, id, StatusFailed, StatusClaimed, StatusRunning)
		if err != nil {
			return err
		}
		if err := finishRun(tx, id, StatusFailed, now, res, class, signature); err != nil {
			return err
		}
		if err := recordFailure(tx, agent, class, now); err != nil {
			return err
		}
		return upsertIncident(tx, agent, signature, id, now)
	})
}

func finishRun(tx *sql.Tx, id, status, now string, res Result, class, signature string) error {
	_, err := tx.Exec(
		`UPDATE runs SET status = ?, ended_at = ?, failure_class = ?, error_signature = ?,
			session_id = ?, num_turns = ?, cost_usd = ?, input_tokens = ?, output_tokens = ?, silent = ?
			WHERE id = ?`,
		status, now, nullString(class), nullString(signature),
		nullString(res.SessionID), res.NumTurns, res.CostUSD, res.InputTokens, res.OutputTokens, res.Silent,
		id,
	)
	return err
}

// Reconcile marks every run still claimed or running as unknown, which is what
// a daemon start does to the rows its predecessor left behind (§5). It never
// auto-retries: a run that may have spent model quota is not re-fired
// automatically, only by an operator's `shuttle run`.
func (s *Store) Reconcile() (int, error) {
	res, err := s.db.Exec(
		`UPDATE runs SET status = ?, ended_at = ? WHERE status IN (?, ?)`,
		StatusUnknown, s.timestamp(), StatusClaimed, StatusRunning,
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// SetDelivery records the delivery outcome independently of the run status
// (§6.1). delivered_at is stamped only when the message actually left.
func (s *Store) SetDelivery(id, status, deliveryErr string) error {
	var deliveredAt sql.NullString
	if status == DeliveryDelivered {
		deliveredAt = nullString(s.timestamp())
	}
	return s.updateOne(
		`UPDATE runs SET delivery_status = ?, delivery_error = ?, delivered_at = ? WHERE id = ?`,
		id, status, nullString(deliveryErr), deliveredAt, id,
	)
}

// SetOutputsApplied records what §9 applied, with paths and ids.
func (s *Store) SetOutputsApplied(id string, applied json.RawMessage) error {
	return s.updateOne(
		`UPDATE runs SET outputs_applied = ? WHERE id = ?`,
		id, string(applied), id,
	)
}

// Get reads one run by id.
func (s *Store) Get(id string) (*Run, error) {
	row := s.db.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, id)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("run %s: not in the ledger", id)
	}
	return run, err
}

// Runs lists ledger rows most recent first. An empty agent lists every agent;
// a limit of zero or less is unbounded.
func (s *Store) Runs(agent string, limit int) ([]*Run, error) {
	query := `SELECT ` + runColumns + ` FROM runs`
	var args []any
	if agent != "" {
		query += ` WHERE agent = ?`
		args = append(args, agent)
	}
	query += ` ORDER BY claimed_at DESC, id DESC`
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []*Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanRun(sc scanner) (*Run, error) {
	var r Run
	err := sc.Scan(
		&r.ID, &r.Agent, &r.TriggerKind, &r.ScheduledFor, &r.ClaimedAt, &r.StartedAt, &r.EndedAt,
		&r.Status, &r.SkipReason, &r.FailureClass, &r.ErrorSignature, &r.PID, &r.RunDir, &r.SessionID,
		&r.NumTurns, &r.CostUSD, &r.InputTokens, &r.OutputTokens, &r.Silent, &r.OutputsApplied,
		&r.DeliveryStatus, &r.DeliveryError, &r.DeliveredAt,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// requireStatus enforces the §6.1 state machine inside the transaction that is
// about to write: only the listed statuses may move to next, so a terminal row
// never moves again. It returns the run's agent for the streak and incident
// updates that follow.
func requireStatus(tx *sql.Tx, id, next string, from ...string) (string, error) {
	var agent, status string
	err := tx.QueryRow(`SELECT agent, status FROM runs WHERE id = ?`, id).Scan(&agent, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("run %s: not in the ledger", id)
	}
	if err != nil {
		return "", err
	}
	if !slices.Contains(from, status) {
		return "", fmt.Errorf("run %s: cannot move from %s to %s", id, status, next)
	}
	return agent, nil
}

func (s *Store) withTx(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// updateOne runs a single-row update and reports a missing id rather than
// succeeding silently over nothing.
func (s *Store) updateOne(query, id string, args ...any) error {
	res, err := s.db.Exec(query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("run %s: not in the ledger", id)
	}
	return nil
}
