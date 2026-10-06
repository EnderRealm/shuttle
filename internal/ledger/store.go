package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// The ledger is a private operator record — session ids, error text, run
// directories and costs — so neither it nor the state root it sits in is
// readable by another local user.
const (
	homeMode   = 0o700
	ledgerMode = 0o600
)

// ErrSchemaOutdated is returned by Open when the database is older than this
// binary: the on-disk version is below schemaVersion. runs.db is not
// disposable — it is the only record that a run happened and what it cost — so
// the remedy is another explicit migration, never a rebuild. Only the
// immediately preceding schema is a supported source.
var ErrSchemaOutdated = errors.New("run ledger schema is outdated")

// ErrSchemaTooNew is the mirror case: the database is newer than this binary.
// Without the guard an old binary stamps its lower version over the marker and
// then writes old-shaped rows into a newer database — the columns it does not
// know about simply stop being written, and nothing surfaces the gap. The
// remedy is updating shuttle.
var ErrSchemaTooNew = errors.New("run ledger schema is newer than this binary")

// Store is the run ledger, ~/.shuttle/runs.db.
type Store struct {
	// OnTerminal receives each committed completion/failure, and each
	// delivery auth incident, before the write returns. Configure it before
	// sharing the store. Notification failures
	// must be logged by the callback, never undo a terminal ledger write.
	OnTerminal func(Event)
	db         *sql.DB
	// now is the clock every write stamps from. A field so tests can drive
	// slot maths, streaks and incident windows without sleeping.
	now func() time.Time
}

// Open opens (creating if absent) the ledger at path. One writer only:
// SetMaxOpenConns(1) plus WAL is the arrangement §6 specifies, and the
// scheduler and the run goroutines share this one *Store.
func Open(path string) (*Store, error) {
	if err := EnsureHome(filepath.Dir(path)); err != nil {
		return nil, err
	}
	// sqlite creates the database and its WAL sidecars at the process umask,
	// and it materialises -wal and -shm on the first write rather than at open.
	// Creating all three here first is what makes the mode deterministic:
	// chmod after the fact would race that write.
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := privateFile(p); err != nil {
			return nil, err
		}
	}
	// _txlock=immediate makes the driver open every transaction with BEGIN
	// IMMEDIATE, taking sqlite's write lock up front. database/sql's default
	// is deferred, which takes the lock on the first write and so leaves
	// ClaimGated's gate reading outside it — a check-then-act race between the
	// daemon and a concurrent `shuttle run` (§12). Every transaction this
	// package opens writes, so none pays for a lock it does not need.
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, now: time.Now}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// SetClock replaces the clock every write stamps from. It is the seam this
// package's own tests already drove through the field; exported because the
// scheduler's tests drive the same clock from another package, and a
// claimed_at stamped from the wall clock would make their limit windows pass
// by accident. Production leaves it at time.Now.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

// EnsureHome creates the state root if it is absent. MkdirAll applies its mode
// only to what it creates, so a root that already exists is tightened by the
// mask rather than by the create. Exported because `shuttle install` lays the
// state root down before any ledger exists (§9.3), and homeMode is the one
// place that decides what mode it gets.
func EnsureHome(dir string) error {
	if err := os.MkdirAll(dir, homeMode); err != nil {
		return err
	}
	return restrict(dir)
}

// privateFile creates one of the ledger's files ahead of sqlite, so it is
// created at 0600 rather than at whatever the process umask allows.
func privateFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, ledgerMode)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return restrict(path)
}

// restrict clears the group and other bits of an existing entry. Masking
// rather than setting a mode means this only ever narrows access: a stricter
// mode an operator chose deliberately survives.
func restrict(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return os.Chmod(path, perm&^0o077)
	}
	return nil
}

func migrate(db *sql.DB) error {
	// schema_meta may not exist on a fresh database; create it before reading
	// the version so the check is uniform across fresh and existing files.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("create schema_meta: %w", err)
	}
	var current int
	var v sql.NullString
	row := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`)
	if err := row.Scan(&v); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read schema version: %w", err)
	}
	if v.Valid {
		n, err := strconv.Atoi(v.String)
		if err != nil {
			return fmt.Errorf("parse schema version: %w", err)
		}
		current = n
	}
	// Must precede both the schema apply and the marker write: on an unknown
	// old or too-new database neither may run. Version 1 is the one supported
	// migration; schemaSQL's IF NOT EXISTS statements add the V2 tables
	// without rewriting the phase-1 ledger.
	if err := checkVersion(current); err != nil {
		return err
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	_, err := db.Exec(
		`INSERT OR REPLACE INTO schema_meta(key, value) VALUES (?, ?)`,
		"schema_version", strconv.Itoa(schemaVersion),
	)
	return err
}

// checkVersion is the two-way guard. A current of 0 is a fresh database and
// version 1 is the supported migration source.
func checkVersion(current int) error {
	if current != 0 && current < schemaVersion-1 {
		return fmt.Errorf("%w: on disk %d, this binary writes %d", ErrSchemaOutdated, current, schemaVersion)
	}
	if current > schemaVersion {
		return fmt.Errorf("%w: on disk %d, this binary writes %d", ErrSchemaTooNew, current, schemaVersion)
	}
	return nil
}

// timestamp renders a write time in the ledger's format: RFC3339 in UTC.
// Ordering is done on these columns as text, so a local offset would sort the
// repeated hour of a DST fall-back backwards — and the scheduler is
// DST-literal in a zone that has one. scheduled_for is the exception: a slot
// keeps the offset of its own timezone (§5).
func (s *Store) timestamp() string {
	return s.now().UTC().Format(time.RFC3339)
}

// isSlotTaken reports whether err is the unique-index violation on
// (agent, scheduled_for, trigger_kind). The index is the arbiter for a slot,
// so this is matched on the sqlite result code rather than the message.
func isSlotTaken(err error) bool {
	var serr *sqlite.Error
	return errors.As(err, &serr) && serr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

// nullString maps the empty string to NULL, so an unset optional column is
// distinguishable from one deliberately set to "".
func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
