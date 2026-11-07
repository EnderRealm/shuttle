package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testClock is fixed so every write is assertable; a test that needs time to
// move reassigns Store.now.
var testClock = time.Date(2026, 9, 2, 4, 30, 12, 0, time.UTC)

// newStore opens a ledger on a real file: SetMaxOpenConns(1) over a WAL file is
// what production does, and a shared in-memory database is not the same thing.
func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	s.now = func() time.Time { return testClock }
	return s
}

// slot parses an RFC3339 instant with its offset, the form §5 stores.
func slot(t *testing.T, value string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse slot %q: %v", value, err)
	}
	return ts
}

func TestOpenStampsSchemaVersion(t *testing.T) {
	s := newStore(t)
	var v string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if v != "8" {
		t.Fatalf("marker = %q, want \"8\"", v)
	}
}

func TestOpenRefusesNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.db.Exec(
		`INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)`,
		schemaVersion+1,
	); err != nil {
		t.Fatalf("stamp newer version: %v", err)
	}
	s.Close()

	if _, err := Open(path); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("open newer db: err = %v, want ErrSchemaTooNew", err)
	}
	// The guard must not have stamped its own version over the marker: that is
	// the corruption it exists to prevent.
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("reopen raw: %v", err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if v != schemaVersion+1 {
		t.Fatalf("marker = %d, want %d", v, schemaVersion+1)
	}
}

func TestCheckVersion(t *testing.T) {
	tests := []struct {
		name    string
		current int
		want    error
	}{
		{"fresh database", 0, nil},
		{"current", schemaVersion, nil},
		{"supported migration from 4", 4, nil},
		{"supported migration from 3", 3, nil},
		{"supported migration from 2", 2, nil},
		{"outdated", 1, ErrSchemaOutdated},
		{"too new", schemaVersion + 1, ErrSchemaTooNew},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Version 0 is an unstamped database, not an old one.
			if tt.current == 0 && tt.want == ErrSchemaOutdated {
				t.Skip("unreachable until schemaVersion > 1")
			}
			if err := checkVersion(tt.current); !errors.Is(err, tt.want) {
				t.Fatalf("checkVersion(%d) = %v, want %v", tt.current, err, tt.want)
			}
		})
	}
}

func TestOpenCreatesAPrivateLedger(t *testing.T) {
	home := filepath.Join(t.TempDir(), "shuttle")
	s, err := Open(filepath.Join(home, "runs.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	// The WAL sidecars carry the same rows as the database, and they exist
	// only once something has been written through them.
	if _, _, err := s.Claim("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if mode := permOf(t, home); mode != homeMode {
		t.Fatalf("%s mode = %04o, want %04o", home, mode, homeMode)
	}
	for _, name := range []string{"runs.db", "runs.db-wal", "runs.db-shm"} {
		if mode := permOf(t, filepath.Join(home, name)); mode != ledgerMode {
			t.Fatalf("%s mode = %04o, want %04o", name, mode, ledgerMode)
		}
	}
}

func TestOpenTightensAWorldReadableLedger(t *testing.T) {
	home := filepath.Join(t.TempDir(), "shuttle")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// MkdirAll and OpenFile both apply the process umask, so the loose modes
	// this test needs are set explicitly.
	if err := os.Chmod(home, 0o755); err != nil {
		t.Fatalf("chmod home: %v", err)
	}
	path := filepath.Join(home, "runs.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write db: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod db: %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if mode := permOf(t, home); mode != homeMode {
		t.Fatalf("home mode = %04o, want %04o", mode, homeMode)
	}
	if mode := permOf(t, path); mode != ledgerMode {
		t.Fatalf("db mode = %04o, want %04o", mode, ledgerMode)
	}
}

func TestRestrictNeverWidens(t *testing.T) {
	tests := []struct {
		mode os.FileMode
		want os.FileMode
	}{
		{0o644, 0o600},
		{0o666, 0o600},
		{0o600, 0o600},
		{0o400, 0o400},
	}
	dir := t.TempDir()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%04o", tt.mode), func(t *testing.T) {
			path := filepath.Join(dir, fmt.Sprintf("%04o", tt.mode))
			if err := os.WriteFile(path, nil, tt.mode); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := os.Chmod(path, tt.mode); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			if err := restrict(path); err != nil {
				t.Fatalf("restrict: %v", err)
			}
			if mode := permOf(t, path); mode != tt.want {
				t.Fatalf("restrict(%04o) left %04o, want %04o", tt.mode, mode, tt.want)
			}
		})
	}
}

// ClaimGated's gate reads inside its transaction, which is only free of a
// check-then-act race if the transaction holds the write lock from its first
// statement. That is what _txlock=immediate buys, and it is a DSN parameter one
// edit could drop, so it is asserted rather than assumed.
func TestOpenTakesTheWriteLockAtBegin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	// A second connection standing in for a second process. Its own short
	// busy_timeout keeps the probe from waiting out Open's five seconds.
	probe, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(50)")
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	defer probe.Close()

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	// A read: a deferred transaction would still be holding no write lock here.
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM runs`).Scan(&n); err != nil {
		t.Fatalf("read inside the transaction: %v", err)
	}
	if _, err := probe.Exec(`INSERT INTO schema_meta(key, value) VALUES ('probe', '1')`); err == nil {
		t.Fatal("a second connection wrote while a transaction was open, so Begin took no write lock")
	}
}

func permOf(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return fi.Mode().Perm()
}
