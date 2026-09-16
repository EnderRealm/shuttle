package ledger

import (
	"database/sql"
	"errors"
	"fmt"
)

// Pause reasons, §6.3.
const (
	PauseStreak = "streak"
	PauseAuth   = "auth"
	PauseManual = "operator"
)

// maxFailureStreak is the §6.3 auto-pause threshold. The alternative is a
// nightly job that fails identically for a month while burning preflight calls.
const maxFailureStreak = 5

// Agent is one row of the agents table: the scheduler's memory of where an
// agent got to.
type Agent struct {
	Name          string
	LastFiredSlot sql.NullString
	LastSuccessAt sql.NullString
	FailureStreak int
	Paused        bool
	PausedReason  sql.NullString
	PausedAt      sql.NullString
}

// Pause prevents future claims for an agent until Resume clears it.
func (s *Store) Pause(name string) error {
	_, err := s.db.Exec(
		`INSERT INTO agents(name,paused,paused_reason,paused_at) VALUES(?,1,?,?)
		 ON CONFLICT(name) DO UPDATE SET paused=1,paused_reason=excluded.paused_reason,paused_at=excluded.paused_at`,
		name, PauseManual, s.timestamp(),
	)
	return err
}

// Agent reads an agent's ledger row. An agent that has never fired has no row;
// it reads as a zero row rather than an error, because "never fired" is exactly
// an unpaused agent with no streak and no last slot.
func (s *Store) Agent(name string) (*Agent, error) {
	a := &Agent{Name: name}
	err := s.db.QueryRow(
		`SELECT last_fired_slot, last_success_at, failure_streak, paused, paused_reason, paused_at
			FROM agents WHERE name = ?`, name,
	).Scan(&a.LastFiredSlot, &a.LastSuccessAt, &a.FailureStreak, &a.Paused, &a.PausedReason, &a.PausedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return a, nil
	}
	if err != nil {
		return nil, err
	}
	return a, nil
}

// Resume clears an auto-pause and the streak that caused it (§6.3).
func (s *Store) Resume(name string) error {
	res, err := s.db.Exec(
		`UPDATE agents SET paused = 0, paused_reason = NULL, paused_at = NULL, failure_streak = 0
			WHERE name = ?`, name,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("agent %s: not in the ledger", name)
	}
	return nil
}

// recordSuccess resets the streak and stamps last_success_at. The pause is not
// cleared here: only `shuttle resume` lifts it (§6.3).
func recordSuccess(tx *sql.Tx, agent, now string) error {
	_, err := tx.Exec(
		`INSERT INTO agents(name, last_success_at, failure_streak) VALUES (?, ?, 0)
			ON CONFLICT(name) DO UPDATE SET last_success_at = excluded.last_success_at, failure_streak = 0`,
		agent, now,
	)
	return err
}

// recordFailure advances the streak and applies the §6.3 auto-pause rules.
// Only a failed run reaches here: unknown and skipped leave the streak alone.
func recordFailure(tx *sql.Tx, agent, class, now string) error {
	var streak int
	err := tx.QueryRow(
		`INSERT INTO agents(name, failure_streak) VALUES (?, 1)
			ON CONFLICT(name) DO UPDATE SET failure_streak = failure_streak + 1
			RETURNING failure_streak`, agent,
	).Scan(&streak)
	if err != nil {
		return err
	}
	reason := ""
	switch {
	case class == ClassAuth:
		// There is no headless recovery from a rejected credential, so it
		// pauses on the first occurrence rather than on the fifth.
		reason = PauseAuth
	case streak >= maxFailureStreak:
		reason = PauseStreak
	}
	if reason == "" {
		return nil
	}
	// `AND paused = 0` keeps the first pause's reason and timestamp: that is
	// the one that explains why the agent stopped.
	_, err = tx.Exec(
		`UPDATE agents SET paused = 1, paused_reason = ?, paused_at = ? WHERE name = ? AND paused = 0`,
		reason, now, agent,
	)
	return err
}
