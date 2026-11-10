package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
)

// catalogRevision is the project's recorded catalog_revision.
func catalogRevision(t *testing.T, s *Store, project string) int64 {
	t.Helper()
	var revision int64
	if err := s.db.QueryRow(`SELECT revision FROM weft_catalog WHERE project=?`, project).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	return revision
}

// catalogEventsAfter is every catalog event written after seq.
func catalogEventsAfter(t *testing.T, s *Store, seq int64) []catalogEventPayload {
	t.Helper()
	events, err := s.WeftEventsAfter(seq)
	if err != nil {
		t.Fatal(err)
	}
	var out []catalogEventPayload
	for _, e := range events {
		if e.Type != "catalog" {
			continue
		}
		var p catalogEventPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// catalogStep applies one write and holds the project's revision to it: a
// write that moves an eligibility input raises the revision by one with one
// catalog event at it, committed with the write; any other leaves both alone.
func catalogStep(t *testing.T, s *Store, project, name string, raises bool, write func() error) {
	t.Helper()
	before := catalogRevision(t, s, project)
	seq, err := s.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	if err := write(); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	after := catalogRevision(t, s, project)
	events := catalogEventsAfter(t, s, seq)
	if !raises {
		if after != before || len(events) != 0 {
			t.Fatalf("%s: revision %d -> %d with catalog events %+v; want it unchanged", name, before, after, events)
		}
		return
	}
	if after != before+1 || len(events) != 1 || events[0].Project != project || events[0].CatalogRevision != after {
		t.Fatalf("%s: revision %d -> %d with catalog events %+v; want one raise with its event", name, before, after, events)
	}
}

// Every host-held input to a ticket's eligibility — an entry admitted and
// leaving, a run entering and leaving queued, running, parked and settling,
// ending, and archived — raises the project's catalog_revision in the
// transaction that moves it.
func TestCatalogRevisionOnRunPhase(t *testing.T) {
	s := newStore(t)
	root := t.TempDir()
	repo := filepath.Join(root, "demo")
	if err := mkdir(repo); err != nil {
		t.Fatal(err)
	}
	runs := filepath.Join(root, "runs")
	d := WeftDispatch{SharedRoot: filepath.Join(root, "weft"), RunDirRoot: runs, AgentUsers: []string{"weftagent1"},
		Check: func(*WeftEntry) (string, string) { return "", "" }}
	const ticket = "demo/phase-0001"
	if _, _, err := s.SyncWeftCatalog(map[string]WeftBinding{"demo": {Digest: "tk", RepositoryID: sandboxID(repo), Repository: repo}}); err != nil {
		t.Fatal(err)
	}
	admission := func(key string) WeftAdmission {
		return WeftAdmission{Kind: WeftEntryLaunch, Ticket: ticket, Project: "demo",
			RepositoryID: sandboxID(repo), Repository: repo, TokenID: "token", RequestKey: key}
	}

	var entry *WeftEntry
	catalogStep(t, s, "demo", "entry admitted", true, func() error {
		return s.WeftTx(func(t *WeftTx) (err error) {
			entry, err = t.Admit(admission("launch"))
			return err
		})
	})
	catalogStep(t, s, "demo", "entry cancelled", true, func() error {
		return s.WeftTx(func(t *WeftTx) error {
			_, err := t.CancelEntry(entry.ID, entry.Revision)
			return err
		})
	})
	var run *WeftRun
	catalogStep(t, s, "demo", "queued", true, func() (err error) {
		run, err = s.ClaimWeft(WeftLaunch{Ticket: ticket, Project: "demo", Repo: repo, Branch: ticket,
			Worktree: filepath.Join(root, "worktree"), RunDirRoot: runs})
		return err
	})
	catalogStep(t, s, "demo", "process recorded", false, func() error {
		return s.MarkWeftRunning(run.ID, 1, 12345, "weft-worker/v2")
	})
	catalogStep(t, s, "demo", "running", true, func() error {
		return s.RecordWeftLandingHello(run.ID, 1, "20261026T043913Z-00000001", "base", "criteria", true)
	})
	var question string
	catalogStep(t, s, "demo", "parked", true, func() (err error) {
		question, err = s.RecordWeftParked(run.ID, 1, "work", 1, 1, "Which shape?", nil, "head", json.RawMessage(`{"state":"rendered"}`))
		return err
	})
	catalogStep(t, s, "demo", "parked segment reaped", false, func() error {
		return s.CompleteWeftSegment(run.ID, 1)
	})
	catalogStep(t, s, "demo", "answered", true, func() error {
		_, err := s.ClaimWeftAnswer(run.ID, question, "The existing one.", runs)
		return err
	})
	catalogStep(t, s, "demo", "running again", true, func() error {
		if err := s.MarkWeftRunning(run.ID, 2, 12346, "weft-worker/v2"); err != nil {
			return err
		}
		return s.RecordWeftLandingHello(run.ID, 2, "20261026T043913Z-00000002", "base", "criteria", true)
	})
	catalogStep(t, s, "demo", "settling", true, func() error {
		return s.RecordWeftGraded(run.ID, 2, "finished", false, "", "", "", "", "head", json.RawMessage(`{"state":"rendered"}`))
	})
	catalogStep(t, s, "demo", "ended", true, func() error {
		return s.RecordWeftLanding(run.ID, 2, WeftLanding{State: WeftSettlementNotLanded, Step: WeftLandSteps[0],
			Reason: WeftLandRefused, Detail: "main moved"})
	})
	catalogStep(t, s, "demo", "ended segment reaped", false, func() error {
		return s.CompleteWeftSegment(run.ID, 2)
	})
	// A landing that did not land pauses the repository; continuing it is
	// the repository's state, not the ticket's.
	catalogStep(t, s, "demo", "repository continued", false, func() error {
		return s.WeftTx(func(t *WeftTx) error {
			r, err := t.Repository(sandboxID(repo))
			if err != nil {
				return err
			}
			_, err = t.ContinueRepository(r.RepositoryID, r.Revision)
			return err
		})
	})
	ended, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var fresh *WeftEntry
	catalogStep(t, s, "demo", "fresh admitted", true, func() error {
		return s.WeftTx(func(t *WeftTx) (err error) {
			a := admission("fresh")
			fresh, err = t.AdmitFresh(ended.ID, ended.Revision, ended.Worktree, ended.Head.String, a)
			return err
		})
	})
	catalogStep(t, s, "demo", "fresh archiving", true, func() error {
		return s.WeftTx(func(t *WeftTx) error { return t.Dispatch(d) })
	})
	if fresh == nil {
		t.Fatal("no fresh entry")
	}
	catalogStep(t, s, "demo", "archived", true, func() error {
		if err := s.MarkWeftRunning(run.ID, 3, 12347, "weft-archive/v1"); err != nil {
			return err
		}
		spawn, _, err := s.SettleWeftArchive(run.ID, 3, &WeftArchiveReport{Done: true, Head: "head"}, "", "", d, nil)
		if err == nil && len(spawn) != 1 {
			t.Fatalf("archive spawned %v", spawn)
		}
		return err
	})
	archived, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if archived.Disposition != WeftDispositionArchived || archived.Phase != WeftPhaseEnded {
		t.Fatalf("archived run = %+v", archived)
	}
}

// A tk-only change raises the revision once, and run writes that move no
// ticket's eligibility — the process, a stage, liveness, output, a summary
// — leave the revision and the catalog stream alone.
func TestCatalogRevisionNoChurn(t *testing.T) {
	s := newStore(t)
	sync := func(digest string) {
		t.Helper()
		if _, _, err := s.SyncWeftCatalog(map[string]WeftBinding{"demo": {Digest: digest}}); err != nil {
			t.Fatal(err)
		}
	}
	sync("tk-1")
	catalogStep(t, s, "demo", "tk edit", true, func() error { sync("tk-2"); return nil })
	catalogStep(t, s, "demo", "tk unchanged", false, func() error { sync("tk-2"); return nil })

	run := claimVisible(t, s, "demo/quiet-0001")
	if err := s.RecordWeftHello(run.ID, 1, "20261026T043913Z-00000001", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	attempt := WeftAttempt{Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1}
	for _, step := range []struct {
		name  string
		write func() error
	}{
		{"process recorded", func() error { return s.MarkWeftRunning(run.ID, 1, 12345, "weft-worker/v2") }},
		{"stage", func() error {
			return s.RecordWeftStage(WeftStage{RunID: run.ID, Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1,
				State: "running", At: "2026-10-26T04:39:13.000Z"})
		}},
		{"liveness", func() error {
			return s.RecordWeftLiveness(run.ID, WeftLiveness{WeftAttempt: attempt, Stalled: true,
				LastEventAt: "2026-10-26T04:39:13.000Z", ObservedAt: "2026-10-26T04:49:13.000Z"})
		}},
		{"output", func() error {
			_, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: attempt, Chunk: 1,
				At: "2026-10-26T04:39:14.000Z", Text: "working\n"})
			return err
		}},
		{"summary", func() error { return s.RecordWeftSummarized(run.ID, json.RawMessage(`{"state":"rendered"}`)) }},
	} {
		catalogStep(t, s, "demo", step.name, false, step.write)
	}
}

// Schema 6 carries to 7 with each recorded project's host digest taken in
// the migration, without a raise, so the first write after the upgrade that
// moves an eligibility input — a queued run starting — raises the revision.
func TestOpenMigratesSchemaSixToSeven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SyncWeftCatalog(map[string]WeftBinding{"demo": {Digest: "tk"}}); err != nil {
		t.Fatal(err)
	}
	run := claimVisible(t, s, "demo/migrated-0001")
	for _, stmt := range []string{`ALTER TABLE weft_catalog DROP COLUMN host_digest`,
		`ALTER TABLE weft_segments DROP COLUMN agent_user`,
		`ALTER TABLE weft_runs DROP COLUMN host_cleanup_hold`,
		`ALTER TABLE weft_entries DROP COLUMN dismissed`,
		`ALTER TABLE weft_questions DROP COLUMN questions`, `ALTER TABLE weft_answers DROP COLUMN answers`,
		`UPDATE schema_meta SET value='6' WHERE key='schema_version'`} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	revision := catalogRevision(t, s, "demo")
	seq, err := s.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if s, err = Open(path); err != nil {
		t.Fatalf("migrate 6 to 8: %v", err)
	}
	defer s.Close()
	var version string
	var digest sql.NullString
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "11" {
		t.Fatalf("version = %q %v", version, err)
	}
	if err := s.db.QueryRow(`SELECT host_digest FROM weft_catalog WHERE project='demo'`).Scan(&digest); err != nil || !digest.Valid {
		t.Fatalf("migrated host digest = %v %v", digest, err)
	}
	if got, events := catalogRevision(t, s, "demo"), catalogEventsAfter(t, s, seq); got != revision || len(events) != 0 {
		t.Fatalf("migration moved revision %d -> %d with catalog events %+v", revision, got, events)
	}
	catalogStep(t, s, "demo", "running after the upgrade", true, func() error {
		return s.RecordWeftHello(run.ID, 1, "20261026T043913Z-00000001", "base", "criteria")
	})
}
