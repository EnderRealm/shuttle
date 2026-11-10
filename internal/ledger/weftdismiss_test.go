package ledger

import (
	"path/filepath"
	"testing"
	"time"
)

// A ledger stamped 9 gains weft_entries.dismissed, its entries reading as
// not dismissed and so still listed.
func TestSchemaNineMigratesToTen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var entry *WeftEntry
	if err := s.WeftTx(func(tx *WeftTx) error {
		entry, err = tx.Admit(WeftAdmission{Kind: WeftEntryLaunch, Ticket: "demo/migrated-0010", Project: "demo",
			RepositoryID: "demo-00000000", Repository: "/repos/demo", TokenID: "token", RequestKey: "k1"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`ALTER TABLE weft_entries DROP COLUMN dismissed`,
		`UPDATE schema_meta SET value='9' WHERE key='schema_version'`} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	if s, err = Open(path); err != nil {
		t.Fatalf("migrate 9 to 10: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "10" {
		t.Fatalf("version = %q %v", version, err)
	}
	q, err := s.WeftQueue(time.Now().Add(-time.Hour))
	if err != nil || len(q.Entries) != 1 || q.Entries[0].ID != entry.ID || q.Entries[0].Dismissed {
		t.Fatalf("migrated queue = %+v %v", q, err)
	}
}
