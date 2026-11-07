package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// restartBarrierKey is the weft_settings row of the restart barrier. Its value
// is the Unix second the barrier lapses at.
const restartBarrierKey = "restart_barrier_until"

// ErrRestarting refuses a claim under the restart barrier. Nothing was written,
// so a scheduled slot stays due and the next tick after the barrier claims it.
var ErrRestarting = errors.New("shuttle is restarting for an update, which holds new work until it finishes; nothing was recorded, so try again then")

// deliveryDrainWindow is how long after a run completes its pending email
// counts as in flight. The send follows the row going completed, in the same
// worker; a row still pending past this was orphaned by an earlier restart and
// would otherwise hold every update back forever.
const deliveryDrainWindow = 10 * time.Minute

// ClaimUpdatePause pauses weft-worker for the updater, unless it is already
// paused: an operator's, auth or streak pause is not the updater's to take
// over, so claimed is false and reason names it. An agent that has never run
// has no row, and the claim makes one.
func (s *Store) ClaimUpdatePause() (claimed bool, reason string, err error) {
	err = s.withTx(func(tx *sql.Tx) error {
		return weftHostStepTx(tx, WeftAgent, weftTimestamp(s.now()), func() error {
			res, err := tx.Exec(
				`INSERT INTO agents(name,paused,paused_reason,paused_at) VALUES(?,1,?,?)
				 ON CONFLICT(name) DO UPDATE SET paused=1,paused_reason=excluded.paused_reason,paused_at=excluded.paused_at
				 WHERE agents.paused=0`,
				WeftAgent, PauseUpdate, s.timestamp(),
			)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if claimed = n == 1; claimed {
				return nil
			}
			_, reason, _, err = weftHostPaused(tx)
			return err
		})
	})
	return claimed, reason, err
}

// ReleaseUpdatePause ends the update's hold on the host: it lifts the updater's
// pause and no other, and takes down the restart barrier whichever pause
// stands, reporting what it found. Unlike Resume it leaves failure_streak alone:
// an update is no evidence that whatever was failing has stopped.
func (s *Store) ReleaseUpdatePause() (lifted, barrier bool, err error) {
	err = s.withTx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM weft_settings WHERE key=?`, restartBarrierKey)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		barrier = n == 1
		return weftHostStepTx(tx, WeftAgent, weftTimestamp(s.now()), func() error {
			res, err := tx.Exec(
				`UPDATE agents SET paused=0,paused_reason=NULL,paused_at=NULL WHERE name=? AND paused=1 AND paused_reason=?`,
				WeftAgent, PauseUpdate,
			)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			lifted = n == 1
			return err
		})
	})
	return lifted, barrier, err
}

// RequireUpdatePause fails once the update pause has stopped being what keeps
// dispatch shut: an operator lifted it or put their own in its place, and their
// intent now stands over the update's.
func (s *Store) RequireUpdatePause() error { return requireUpdatePause(s.db) }

func requireUpdatePause(q querier) error {
	paused, reason, _, err := weftHostPaused(q)
	if err != nil {
		return err
	}
	if !paused || reason != PauseUpdate {
		return fmt.Errorf("the update pause no longer stands (paused=%t reason=%q), so nothing holds dispatch shut", paused, reason)
	}
	return nil
}

// ArmRestartBarrier is the updater's final check and what keeps it true through
// the restart: in one transaction it requires the update pause and an empty
// RestartWork and raises the barrier, refusing with neither written otherwise.
// Every transaction here is BEGIN IMMEDIATE, so across processes a claim either
// committed first and is seen here, or runs after and sees the barrier.
//
// Scheduled and manual claims and terminals read the barrier itself. Every weft
// claim reads the pause, which Resume will not lift under the barrier, so the
// same transaction shuts both, and the host event and dashboard stay as the
// pause alone made them. What a settlement claims rather than dispatch is
// claimed as a segment closes, and none is in flight here, short of the window
// docs/releasing.md names in a daemon's own restart pass.
//
// The barrier lapses after lease. ReleaseUpdatePause is what takes it down; the
// lease is for an updater that died and has no successor to recover it, which
// must not leave every slot held for good.
func (s *Store) ArmRestartBarrier(lease time.Duration) error {
	return s.withTx(func(tx *sql.Tx) error {
		if err := requireUpdatePause(tx); err != nil {
			return err
		}
		work, err := s.restartWork(tx)
		if err != nil {
			return err
		}
		if !work.Empty() {
			return fmt.Errorf("work appeared after the drain: %s", strings.Join(slices.Concat(work.Runs, work.Terminals), ", "))
		}
		_, err = tx.Exec(`INSERT INTO weft_settings(key,value) VALUES(?,?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`, restartBarrierKey, s.now().Add(lease).Unix())
		return err
	})
}

// RestartBarrier reports whether the barrier stands now.
func (s *Store) RestartBarrier() (bool, error) { return s.restartBarrier(s.db) }

// restartBarrier reads the barrier. A claim calls it inside its own
// transaction, which is what makes the refusal atomic with the claim.
func (s *Store) restartBarrier(q querier) (bool, error) {
	var held bool
	err := q.QueryRow(`SELECT EXISTS(SELECT 1 FROM weft_settings WHERE key=? AND CAST(value AS INTEGER)>?)`,
		restartBarrierKey, s.now().Unix()).Scan(&held)
	return held, err
}

// RestartWork is what a daemon restart would kill, each named for the log.
type RestartWork struct {
	// Runs end on their own: every row still claimed or running, of any agent
	// and any segment kind, since the restart pass reaps them all, and an
	// email its run's worker is still sending.
	Runs []string
	// Terminals end only when a person ends them: §9.4 sessions do not survive
	// a restart, and a detached one is as live as an attached one.
	Terminals []string
}

// Empty reports whether a restart would kill nothing.
func (w RestartWork) Empty() bool { return len(w.Runs) == 0 && len(w.Terminals) == 0 }

// RestartWork reads what a daemon restart would kill now.
func (s *Store) RestartWork() (RestartWork, error) { return s.restartWork(s.db) }

func (s *Store) restartWork(q querier) (RestartWork, error) {
	var w RestartWork
	inFlight, err := inFlightRuns(q)
	if err != nil {
		return w, err
	}
	for _, r := range inFlight {
		w.Runs = append(w.Runs, fmt.Sprintf("%s (%s, %s)", r.ID, r.Agent, r.Status))
	}
	sending, err := queryRuns(q,
		`SELECT `+runColumns+` FROM runs WHERE status=? AND delivery_status=? AND ended_at>=? ORDER BY claimed_at,id`,
		StatusCompleted, DeliveryPending, s.now().Add(-deliveryDrainWindow).UTC().Format(time.RFC3339),
	)
	if err != nil {
		return w, err
	}
	for _, r := range sending {
		w.Runs = append(w.Runs, fmt.Sprintf("%s (%s, delivering)", r.ID, r.Agent))
	}
	rows, err := q.Query(`SELECT id,state,path FROM weft_terminals t WHERE ` + weftTerminalOpenSQL + ` ORDER BY created_at,id`)
	if err != nil {
		return w, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, state, path string
		if err := rows.Scan(&id, &state, &path); err != nil {
			return w, err
		}
		w.Terminals = append(w.Terminals, fmt.Sprintf("%s (%s, %s)", id, state, path))
	}
	return w, rows.Err()
}

// Backup writes a consistent copy of the live ledger to path, a new private
// file. VACUUM INTO reads through the WAL, which a copy of runs.db alone
// would miss.
func (s *Store) Backup(path string) error {
	if err := privateFile(path); err != nil {
		return err
	}
	if _, err := s.db.Exec(`VACUUM INTO ?`, path); err != nil {
		os.Remove(path)
		return fmt.Errorf("vacuum into %s: %w", path, err)
	}
	return nil
}

// SchemaVersion is the version the ledger's schema is at on disk now, which a
// newer binary's migration may have raised under this handle.
func (s *Store) SchemaVersion() (int, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

// RestoreBackup puts backup in place of the ledger at path, keeping the
// ledger it replaces, sidecars included, under aside. Nothing may have the
// ledger open: a daemon writing through the old -wal would corrupt the new
// file.
func RestoreBackup(path, backup, aside string) error {
	staged := path + ".restoring"
	if err := privateFile(staged); err != nil {
		return err
	}
	defer os.Remove(staged)
	data, err := os.ReadFile(backup)
	if err != nil {
		return err
	}
	if err := os.WriteFile(staged, data, ledgerMode); err != nil {
		return err
	}
	if err := os.Link(path, aside); err != nil {
		return err
	}
	for _, sidecar := range []string{"-wal", "-shm"} {
		if err := os.Rename(path+sidecar, aside+sidecar); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(staged, path)
}
