package ledger

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func openTerminal(t *testing.T, s *Store, o WeftTerminalOpen) (*WeftTerminal, error) {
	t.Helper()
	var row *WeftTerminal
	err := s.WeftTx(func(tx *WeftTx) error {
		var err error
		row, err = tx.OpenTerminal(o)
		return err
	})
	return row, err
}

func reservation(t *testing.T, s *Store, repositoryID string) (kind, holder string) {
	t.Helper()
	err := s.db.QueryRow(`SELECT holder_kind,holder_id FROM weft_reservations WHERE repository_id=?`, repositoryID).Scan(&kind, &holder)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return kind, holder
}

// Sessions in one repository share its one terminal row: the row names the
// session that took it until that one closes, and goes with the last share.
func TestTerminalSessionsShareOneReservation(t *testing.T) {
	s := newStore(t)
	repo := WeftTerminalOpen{Kind: "shell", TargetKind: WeftTargetProject, TargetID: "weft", Path: "/Users/steve/code/weft",
		RepositoryID: "weft-47327fe5", Repository: "/Users/steve/code/weft", Project: "weft", Cols: 120, Rows: 36, Title: "weft"}
	first, err := openTerminal(t, s, repo)
	if err != nil {
		t.Fatal(err)
	}
	second, err := openTerminal(t, s, repo)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != WeftTerminalStarting || first.Revision != 1 || first.Resource()["repository_id"] != "weft-47327fe5" {
		t.Fatalf("opened = %+v", first)
	}
	if kind, holder := reservation(t, s, "weft-47327fe5"); kind != WeftHolderTerminal || holder != first.ID {
		t.Fatalf("reservation = %s %s", kind, holder)
	}
	r, err := weftRepository(s.db, "weft-47327fe5")
	if err != nil || len(r.Terminals) != 2 {
		t.Fatalf("repository = %+v %v", r, err)
	}
	if _, err := s.CloseWeftTerminal(first.ID, WeftClosedByOperator, nil, WeftTerminalState{Cols: 120, Rows: 36, Title: "weft"}, "", 0); err != nil {
		t.Fatal(err)
	}
	if kind, holder := reservation(t, s, "weft-47327fe5"); kind != WeftHolderTerminal || holder != second.ID {
		t.Fatalf("after the first closed = %s %s", kind, holder)
	}
	code := 0
	closed, err := s.CloseWeftTerminal(second.ID, WeftClosedByExit, &code, WeftTerminalState{Cols: 80, Rows: 24, Offset: 9, Title: "zsh"}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if kind, _ := reservation(t, s, "weft-47327fe5"); kind != "" {
		t.Fatalf("the last close kept %s", kind)
	}
	resource := closed.Resource()
	if resource["state"] != "closed" || resource["closed_by"] != "exit" || resource["exit_code"] != int64(0) ||
		resource["offset"] != int64(9) || resource["cols"] != 80 || resource["owner"] != nil {
		t.Fatalf("closed = %v", resource)
	}
	if _, err := s.UpdateWeftTerminal(second.ID, WeftTerminalState{State: WeftTerminalLive}); err == nil {
		t.Fatal("a closed session took a change")
	}
	unregistered := repo
	unregistered.RepositoryID, unregistered.Repository, unregistered.Project = "", "", ""
	folder, err := openTerminal(t, s, unregistered)
	if err != nil || folder.RepositoryID.Valid {
		t.Fatalf("unregistered = %+v %v", folder, err)
	}
	var reservations int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_reservations`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("an unregistered session reserved: %d %v", reservations, err)
	}
}

// A terminal is refused, recording nothing, while a run, a settlement or
// maintenance holds the repository.
func TestTerminalRefusedUnderOtherHolders(t *testing.T) {
	s := newStore(t)
	for _, holder := range []struct{ kind, id, message string }{
		{WeftHolderSettlement, "r-20261002T091458Z-4be1d7a2", "held by the settlement of run r-20261002T091458Z-4be1d7a2"},
		{WeftHolderMaintenance, "e-20261002T095017Z-9a0b1c2d", "held by maintenance e-20261002T095017Z-9a0b1c2d"},
		{WeftHolderRun, "r-20261002T091458Z-00000000", "held by run r-20261002T091458Z-00000000, which is running"},
	} {
		if _, err := s.db.Exec(`DELETE FROM weft_reservations`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO weft_reservations VALUES('weft-47327fe5','/Users/steve/code/weft',?,?,'2026-10-02T09:14:58.000Z')`,
			holder.kind, holder.id); err != nil {
			t.Fatal(err)
		}
		_, err := openTerminal(t, s, WeftTerminalOpen{Kind: "shell", TargetKind: WeftTargetProject, TargetID: "weft",
			Path: "/Users/steve/code/weft", RepositoryID: "weft-47327fe5", Repository: "/Users/steve/code/weft", Project: "weft"})
		var refusal *WeftRefusal
		if !errors.As(err, &refusal) || refusal.Code != "repository_reserved" || !strings.Contains(refusal.Message, holder.message) {
			t.Fatalf("under %s = %v", holder.kind, err)
		}
	}
	var sessions int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_terminals`).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("refusals recorded %d sessions (%v)", sessions, err)
	}
}

// A session whose process group could not be established gone keeps its
// share through step 4 of the restart, until a later pass establishes it.
func TestUnestablishedTerminalKeepsItsShare(t *testing.T) {
	s := newStore(t)
	open := WeftTerminalOpen{Kind: "shell", TargetKind: WeftTargetProject, TargetID: "warp", Path: "/Users/steve/code/warp",
		RepositoryID: "warp-afbbf000", Repository: "/Users/steve/code/warp", Project: "warp"}
	stranded, err := openTerminal(t, s, open)
	if err != nil {
		t.Fatal(err)
	}
	gone, err := openTerminal(t, s, open)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := s.WeftTerminalsToInterrupt()
	if err != nil || len(pending) != 2 {
		t.Fatalf("to interrupt = %d %v", len(pending), err)
	}
	if _, err := s.InterruptWeftTerminal(stranded.ID, "unresolved: leader gone; process group 1234 holds 1 live member(s)"); err != nil {
		t.Fatal(err)
	}
	interrupted, err := s.InterruptWeftTerminal(gone.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if interrupted.State != WeftTerminalInterrupted || interrupted.ClosedBy.String != WeftClosedByRestart {
		t.Fatalf("interrupted = %+v", interrupted)
	}
	if kind, holder := reservation(t, s, "warp-afbbf000"); kind != WeftHolderTerminal || holder != stranded.ID {
		t.Fatalf("reservation = %s %s", kind, holder)
	}
	if _, _, _, err := s.RestartWeftQueue(WeftDispatch{}, func(string, string) (string, error) { return "", nil }); err != nil {
		t.Fatal(err)
	}
	if kind, _ := reservation(t, s, "warp-afbbf000"); kind != WeftHolderTerminal {
		t.Fatal("step 4 released the share of a group not established gone")
	}
	pending, err = s.WeftTerminalsToInterrupt()
	if err != nil || len(pending) != 1 || pending[0].ID != stranded.ID {
		t.Fatalf("still to establish = %+v %v", pending, err)
	}
	if _, err := s.InterruptWeftTerminal(stranded.ID, ""); err != nil {
		t.Fatal(err)
	}
	if kind, _ := reservation(t, s, "warp-afbbf000"); kind != "" {
		t.Fatalf("the established group's share is still held as %s", kind)
	}
}

// Schema 5 carries to 6 with the terminal columns, and on to 8.
func TestOpenMigratesSchemaFiveToSix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"project", "width", "height", "title", "owner", "attachments", "output_offset",
		"retained_from", "process_start", "group_unresolved"} {
		if _, err := s.db.Exec(`ALTER TABLE weft_terminals DROP COLUMN ` + column); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE weft_catalog DROP COLUMN host_digest`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`ALTER TABLE weft_segments DROP COLUMN agent_user`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE schema_meta SET value='5' WHERE key='schema_version'`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatalf("migrate 5 to 8: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "8" {
		t.Fatalf("version = %q %v", version, err)
	}
	if _, err := s.WeftTerminals(testClock); err != nil {
		t.Fatalf("terminals after the migration: %v", err)
	}
}
