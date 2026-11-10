package ledger

import (
	"encoding/json"
	"testing"
)

// retainedHostedRun is agentUserHost's one launch dispatched and its segment
// reaped: a hosted run, retained, with no process and no landing to settle,
// and its repository free — a run a host-started cleanup qualifies for once
// its ticket is finished.
func retainedHostedRun(t *testing.T) (*Store, WeftDispatch, *WeftRun) {
	t.Helper()
	s, d, entries := agentUserHost(t, 1, "weftagent1")
	if _, _, err := s.DispatchWeft(d); err != nil {
		t.Fatal(err)
	}
	var runID string
	for id := range agentUsersOf(t, s, entries) {
		runID = id
	}
	if err := s.MarkWeftRunning(runID, 1, 12345, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftGraded(runID, 1, "failed", false, "work", "error", "", "", "head",
		json.RawMessage(`{"state":"rendered"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteWeftSegment(runID, 1); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetWeftRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Origin != WeftOriginHosted || run.Disposition != WeftDispositionRetained || run.Phase != WeftPhaseEnded {
		t.Fatalf("run = %+v, want a hosted run ended and retained", run)
	}
	d.Finished = func(string) bool { return true }
	return s, d, run
}

// cleanFinishedPasses runs n catalog-change evaluations of every retained run.
func cleanFinishedPasses(t *testing.T, s *Store, d WeftDispatch, n int) {
	t.Helper()
	for range n {
		if err := s.WeftTx(func(tx *WeftTx) error { return tx.CleanFinished(nil, d) }); err != nil {
			t.Fatal(err)
		}
	}
}

func cleanupEntries(t *testing.T, s *Store, runID string) []*WeftEntry {
	t.Helper()
	got, err := loadWeftEntries(s.db, `run_id=? AND kind=?`, runID, WeftEntryCleanup)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// A run whose host-started cleanup was invalidated binding_changed keeps that
// one entry across catalog changes while its project stays bound elsewhere;
// once the binding it was admitted for holds again, it is admitted again.
func TestHostStartedCleanupInvalidatedForBindingIsNotReadmitted(t *testing.T) {
	s, d, run := retainedHostedRun(t)
	bound := false
	d.Check = func(e *WeftEntry) (string, string) {
		if e.Kind == WeftEntryCleanup && !bound {
			return WeftInvalidBinding, "project p0 is now bound to no checkout"
		}
		return "", ""
	}
	cleanFinishedPasses(t, s, d, 4)
	got := cleanupEntries(t, s, run.ID)
	if len(got) != 1 {
		t.Fatalf("after four passes the run has %d cleanup entries %+v, want one", len(got), got)
	}
	if e := got[0]; e.State != WeftEntryInvalidated || e.InvalidatedReason.String != WeftInvalidBinding {
		t.Fatalf("cleanup entry = %+v, want invalidated %s", e, WeftInvalidBinding)
	}
	if _, _, err := s.DispatchWeft(d); err != nil {
		t.Fatal(err)
	}
	if got := cleanupEntries(t, s, run.ID); len(got) != 1 {
		t.Fatalf("after a dispatch pass the run has %d cleanup entries, want one", len(got))
	}

	bound = true
	cleanFinishedPasses(t, s, d, 2)
	got = cleanupEntries(t, s, run.ID)
	if len(got) != 2 || got[1].State != WeftEntryDispatched {
		t.Fatalf("with the binding restored the run's cleanup entries are %+v, want one more, dispatched", got)
	}
}

// A run whose ticket finished and whose binding holds is admitted for a
// host-started cleanup once however many passes follow, and it dispatches.
func TestHostStartedCleanupStillAdmitsACleanableRunOnce(t *testing.T) {
	s, d, run := retainedHostedRun(t)
	var spawn []string
	for range 3 {
		if err := s.WeftTx(func(tx *WeftTx) error {
			err := tx.CleanFinished(nil, d)
			spawn = append(spawn, tx.Spawn...)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	got := cleanupEntries(t, s, run.ID)
	if len(got) != 1 {
		t.Fatalf("after three passes the run has %d cleanup entries %+v, want one", len(got), got)
	}
	host, err := hostStarted(s.db, got[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if e := got[0]; !host || e.State != WeftEntryDispatched {
		t.Fatalf("cleanup entry = %+v (host-started %v), want a host-started entry dispatched", e, host)
	}
	if len(spawn) != 1 || spawn[0] != run.ID {
		t.Fatalf("spawned %v, want %s once", spawn, run.ID)
	}
}
