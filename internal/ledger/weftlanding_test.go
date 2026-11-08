package ledger

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// A ledger stamped 4 by the build before landing migrates to 5 with its
// weft-remote/v1 runs kept as history: the settlement that says v1 never
// lands, their worktree, base and summary as recorded, and segments that
// were never launched to land. Nothing makes them live: a reconcile is
// refused, and the restart pass closes their in-flight segment as v1's
// unknown rather than as a lost landing.
func TestSchemaFourMigratesToFiveKeepingV1RunsHistorical(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	schemaTwo(t, path, "")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateWeftV2(tx, testClock); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{weftVisibilitySQL,
		`UPDATE weft_runs SET summary='{"state":"unavailable","reason":"v1 fixture"}' WHERE id='r-20260913T142233Z-6b2f0c1a'`,
		`UPDATE schema_meta SET value='4' WHERE key='schema_version'`} {
		if _, err := tx.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("migrate 4 to 5: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "9" {
		t.Fatalf("version = %q %v", version, err)
	}
	finished, err := s.GetWeftRun("r-20260913T142233Z-6b2f0c1a")
	if err != nil {
		t.Fatal(err)
	}
	if finished.RecordedUnder != WeftRecordedV1 || finished.SettlementState != WeftSettlementNone ||
		finished.SettlementDetail.String != WeftV1SettlementDetail || finished.Worktree != "/w/a" || finished.Base.String != "base" ||
		string(finished.Summary) != `{"state":"unavailable","reason":"v1 fixture"}` || len(finished.Steps) != 0 ||
		finished.Disposition != WeftDispositionRetained {
		t.Fatalf("migrated v1 run = %+v", finished)
	}
	var landing int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_segments WHERE land<>0`).Scan(&landing); err != nil || landing != 0 {
		t.Fatalf("migrated segments marked landing: %d %v", landing, err)
	}
	if _, _, err := s.SyncWeftCatalog(map[string]WeftBinding{
		"weft": {Digest: "d", RepositoryID: "weft-47327fe5", Repository: "/Users/steve/code/weft"},
	}); err != nil {
		t.Fatalf("catalog binding after 4 to 5: %v", err)
	}

	err = s.WeftTx(func(t *WeftTx) error {
		_, err := t.AdmitReconcile(finished.ID, finished.Revision, WeftAdmission{TokenID: "token", RequestKey: "k"})
		return err
	})
	var refusal *WeftRefusal
	if !errors.As(err, &refusal) || refusal.Code != "not_reconcilable" {
		t.Fatalf("reconcile of a v1 run = %v", err)
	}

	running, err := s.GetWeftRun("r-20260914T090000Z-0c1d2e3f")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkUnknown(running.Segments[0].LedgerRunID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileWeftUnknown(running.Segments[0].LedgerRunID); err != nil {
		t.Fatal(err)
	}
	closed, err := s.GetWeftRun(running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Phase != WeftPhaseEnded || closed.Outcome.String != "failed" || closed.SettlementState != WeftSettlementNone ||
		len(closed.Segments) != 1 || closed.Segments[0].Failure.String != ClassUnknown {
		t.Fatalf("v1 segment after the restart pass = %+v", closed)
	}
	var reservations int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_reservations`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("reservations after the restart pass = %d %v", reservations, err)
	}
}

// landingRun is a run whose first segment, launched with --land, said hello.
func landingRun(t *testing.T, s *Store, ticket string) *WeftRun {
	t.Helper()
	run, err := s.ClaimWeft(WeftLaunch{Ticket: ticket, Project: "demo", Repo: "/repos/" + ticket,
		Branch: ticket, Worktree: "/tmp/weft/" + ticket, RunDirRoot: filepath.Join(t.TempDir(), "runs")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkWeftRunning(run.ID, 1, 12345, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftLandingHello(run.ID, 1, "20260902T043012Z-abcd1234", "base", "criteria", true); err != nil {
		t.Fatal(err)
	}
	return run
}

// A landing segment lost before its grade is closed by its reconcile as it
// was lost: an operator's forced stop stays a stop with the operator's
// summary, the class the host recorded at reap is kept, and only a segment
// the restart pass closed with none reads unknown.
func TestUngradedReconcileKeepsStopAndFailureClass(t *testing.T) {
	s := newStore(t)
	for _, tt := range []struct {
		name, class, outcome string
		stop, restart        bool
	}{
		{"forced stop", ClassStuck, "stopped", true, false},
		{"died", ClassDied, "failed", false, false},
		{"daemon restart", ClassUnknown, "failed", false, true},
	} {
		run := landingRun(t, s, "demo/"+strings.ReplaceAll(tt.name, " ", "-"))
		if tt.stop {
			if err := s.MarkWeftStopped(run.ID); err != nil {
				t.Fatal(err)
			}
		}
		if tt.restart {
			if err := s.MarkUnknown(run.Segments[0].LedgerRunID); err != nil {
				t.Fatal(err)
			}
			if err := s.ReconcileWeftUnknown(run.Segments[0].LedgerRunID); err != nil {
				t.Fatal(err)
			}
		} else if reconcile, err := s.FailWeftSegment(run.ID, 1, tt.class, "worker exited with no terminal event"); err != nil || !reconcile {
			t.Fatalf("%s: lost landing = %v %v", tt.name, reconcile, err)
		}
		if err := s.MarkWeftRunning(run.ID, 2, 12346, "weft-worker/v2"); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordWeftObserved(run.ID, 2, "", WeftObserved{"not_done", "not_done", "not_done"}, "2026-10-03T09:00:00.000Z"); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordWeftReconciled(run.ID, 2, "", WeftLanding{State: WeftSettlementNone}); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetWeftRun(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Outcome.String != tt.outcome || got.Stopped != tt.stop || got.Segments[0].Failure.String != tt.class ||
			got.SettlementState != WeftSettlementNone || got.Phase != WeftPhaseEnded ||
			(tt.stop && string(got.Summary) != weftStoppedSummary) {
			t.Fatalf("%s: reconciled = %+v", tt.name, got)
		}
	}
}

// Only a finished grade can end landed or not landed: a segment graded
// halted that reports a landing is refused, and its settlement stands.
func TestLandingReportedOnAnUnfinishedGradeIsRefused(t *testing.T) {
	s := newStore(t)
	run := landingRun(t, s, "demo/halted")
	if err := s.RecordWeftGraded(run.ID, 1, "halted", false, "review", "rejected", "two findings stand", "", "head", nil); err != nil {
		t.Fatal(err)
	}
	err := s.RecordWeftLanding(run.ID, 1, WeftLanding{State: WeftSettlementNotLanded, Step: WeftStepFastForward,
		Reason: WeftLandRefused, Detail: "dirty", Branch: "main", Commit: "head"})
	if err == nil || !strings.Contains(err.Error(), "settlement is not_applicable") {
		t.Fatalf("not_landed on a halted grade = %v", err)
	}
	if err := s.RecordWeftLanding(run.ID, 1, WeftLanding{State: WeftSettlementNone}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SettlementState != WeftSettlementNone || got.SettlementReason.Valid || got.Segments[0].Terminal.String != "ended" {
		t.Fatalf("halted run = %+v", got)
	}
}

// A landing segment lost before its grade whose reconcile is lost too (T2u):
// the host cannot tell whether the segment graded, so the run is closed as
// the segment was lost — a forced stop stays a stop, else failed with the
// class kept — and held unreconciled at the fast-forward with its repository
// paused. It is not resumable, and with no graded head a reconcile retry
// could only observe, so it is refused.
func TestUngradedLandingWithLostReconcileIsHeldUnreconciled(t *testing.T) {
	for _, tt := range []struct {
		name, class, outcome string
		stop                 bool
	}{
		{"forced stop", ClassStuck, "stopped", true},
		{"died", ClassDied, "failed", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t)
			run := landingRun(t, s, "demo/lost-twice")
			if tt.stop {
				if err := s.MarkWeftStopped(run.ID); err != nil {
					t.Fatal(err)
				}
			}
			if reconcile, err := s.FailWeftSegment(run.ID, 1, tt.class, "worker exited with no terminal event"); err != nil || !reconcile {
				t.Fatalf("lost landing = %v %v", reconcile, err)
			}
			if err := s.MarkWeftRunning(run.ID, 2, 12346, "weft-worker/v2"); err != nil {
				t.Fatal(err)
			}
			if reconcile, err := s.FailWeftSegment(run.ID, 2, ClassDied, "reconcile exited with no terminal event"); err != nil || reconcile {
				t.Fatalf("lost reconcile = %v %v", reconcile, err)
			}
			got, err := s.GetWeftRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != WeftPhaseEnded || got.Outcome.String != tt.outcome || got.Stopped != tt.stop ||
				got.Segments[0].Failure.String != tt.class || (tt.stop && string(got.Summary) != weftStoppedSummary) ||
				got.SettlementState != WeftSettlementNotLanded || got.SettlementReason.String != WeftLandUnreconciled ||
				got.SettlementStep.String != WeftStepFastForward || len(got.Steps) != 0 ||
				got.ActionRequired.String != WeftActionLandingUnsettled {
				t.Fatalf("lost reconcile of an ungraded landing = %+v", got)
			}
			err = s.WeftTx(func(tx *WeftTx) error {
				repo, err := tx.Repository(got.RepositoryID)
				if err != nil {
					return err
				}
				if repo.Dispatch != WeftDispatchPaused || repo.PausedRun.String != run.ID {
					t.Fatalf("repository = %+v", repo)
				}
				_, err = tx.AdmitResume(run.ID, got.Revision, func(string) bool { return true }, WeftAdmission{TokenID: "token", RequestKey: "resume"})
				var refusal *WeftRefusal
				if !errors.As(err, &refusal) || refusal.Code != "run_not_resumable" {
					t.Fatalf("resume = %v", err)
				}
				_, err = tx.AdmitReconcile(run.ID, got.Revision, WeftAdmission{TokenID: "token", RequestKey: "retry"})
				if !errors.As(err, &refusal) || refusal.Code != "not_reconcilable" {
					t.Fatalf("reconcile retry = %v", err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The ack rule holds the commit and base itself, whatever the stream let
// through: a reconcile of a landing the host recorded no grade for takes no
// step even when it observed a finished grade, and a landing segment's step
// names the recorded graded head on the run's base (§3 checks 1 and 2′).
func TestLandIntentNeedsTheRecordedGradedHead(t *testing.T) {
	s := newStore(t)
	ungraded := landingRun(t, s, "demo/ungraded")
	if _, err := s.FailWeftSegment(ungraded.ID, 1, ClassDied, "worker exited with no terminal event"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkWeftRunning(ungraded.ID, 2, 12346, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftObserved(ungraded.ID, 2, "finished", WeftObserved{"not_done", "not_done", "not_done"}, "2026-10-03T09:00:00.000Z"); err != nil {
		t.Fatal(err)
	}
	for _, at := range [][2]string{{"", ""}, {"head", "base"}} {
		if acked, err := s.RecordWeftLandIntent(ungraded.ID, 2, WeftStepFastForward, "main", at[0], at[1], "2026-10-03T09:00:01.000Z"); err == nil || acked {
			t.Fatalf("intent for %q on %q with no recorded grade = %v %v", at[0], at[1], acked, err)
		}
	}

	graded := landingRun(t, s, "demo/graded")
	if err := s.RecordWeftGraded(graded.ID, 1, "finished", false, "", "", "", "", "head", nil); err != nil {
		t.Fatal(err)
	}
	for _, at := range [][2]string{{"", ""}, {"other", "base"}, {"head", "other"}} {
		if acked, err := s.RecordWeftLandIntent(graded.ID, 1, WeftStepFastForward, "main", at[0], at[1], "2026-10-03T09:00:01.000Z"); err == nil || acked {
			t.Fatalf("intent for %q on %q = %v %v", at[0], at[1], acked, err)
		}
	}
	if acked, err := s.RecordWeftLandIntent(graded.ID, 1, WeftStepFastForward, "main", "head", "base", "2026-10-03T09:00:01.000Z"); err != nil || !acked {
		t.Fatalf("intent for the graded head = %v %v", acked, err)
	}
	for id, want := range map[string]int{ungraded.ID: 0, graded.ID: 1} {
		run, err := s.GetWeftRun(id)
		if err != nil {
			t.Fatal(err)
		}
		if len(run.Steps) != want {
			t.Fatalf("%s steps = %+v", id, run.Steps)
		}
	}
}

// A landing segment graded other than finished and then lost before its
// ended — killed, or closed by a restart — took no step and has nothing to
// land (row 13t): it is settled as reaped, keeps its grade with the class
// recorded, claims no reconcile, holds nothing and pauses nothing, and is
// resumable exactly as §4 reads that grade: a failed the worker graded never
// is, whatever class its lost segment then took.
func TestUnfinishedGradeLostBeforeEndedIsNotReconciled(t *testing.T) {
	for _, tt := range []struct {
		name, outcome, class string
		restart, resumable   bool
	}{
		{"halted then killed", "halted", ClassDied, false, false},
		{"stopped then killed", "stopped", ClassStuck, false, true},
		{"failed then killed", "failed", ClassDied, false, false},
		{"failed then restart", "failed", ClassUnknown, true, false},
		{"halted then restart", "halted", ClassUnknown, true, false},
		{"stopped then restart", "stopped", ClassUnknown, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t)
			run := landingRun(t, s, "demo/unfinished")
			if err := s.RecordWeftGraded(run.ID, 1, tt.outcome, tt.outcome == "stopped", "review", "rejected", "", "", "head", nil); err != nil {
				t.Fatal(err)
			}
			if tt.restart {
				if err := s.MarkUnknown(run.Segments[0].LedgerRunID); err != nil {
					t.Fatal(err)
				}
				if err := s.ReconcileWeftUnknown(run.Segments[0].LedgerRunID); err != nil {
					t.Fatal(err)
				}
			} else if reconcile, err := s.FailWeftSegment(run.ID, 1, tt.class, "worker exited with no terminal event"); err != nil || reconcile {
				t.Fatalf("lost graded segment = %v %v", reconcile, err)
			}
			got, err := s.GetWeftRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != WeftPhaseEnded || got.Outcome.String != tt.outcome || got.SettlementState != WeftSettlementNone ||
				got.SettlementReason.Valid || got.ActionRequired.Valid || len(got.Steps) != 0 || len(got.Segments) != 1 ||
				got.Segments[0].Failure.String != tt.class {
				t.Fatalf("lost %s segment = %+v", tt.outcome, got)
			}
			var reservations, entries int
			if err := s.db.QueryRow(`SELECT count(*) FROM weft_reservations`).Scan(&reservations); err != nil || reservations != 0 {
				t.Fatalf("reservations = %d %v", reservations, err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM weft_entries WHERE kind=?`, WeftEntryReconcile).Scan(&entries); err != nil || entries != 0 {
				t.Fatalf("reconcile entries = %d %v", entries, err)
			}
			err = s.WeftTx(func(tx *WeftTx) error {
				repo, err := tx.Repository(got.RepositoryID)
				if err != nil {
					return err
				}
				if repo.Dispatch == WeftDispatchPaused {
					t.Fatalf("repository = %+v", repo)
				}
				resumable, err := weftResumableTx(tx.tx, got)
				if err != nil {
					return err
				}
				if resumable != tt.resumable {
					t.Fatalf("resumable = %v, want %v", resumable, tt.resumable)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A reconcile's reading never overrides a finished grade the host recorded
// (row 15g): after the fast-forward was done, an observation or reconciled
// of any other outcome is refused, and the done row stays.
func TestReconcileReadingCannotUndoAFinishedGrade(t *testing.T) {
	s := newStore(t)
	run := landingRun(t, s, "demo/finished")
	if err := s.RecordWeftGraded(run.ID, 1, "finished", false, "", "", "", "", "head", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordWeftLandIntent(run.ID, 1, WeftStepFastForward, "main", "head", "base", "2026-10-03T09:00:00.000Z"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftLandResult(run.ID, 1, WeftStepFastForward, "done", "", "2026-10-03T09:00:01.000Z"); err != nil {
		t.Fatal(err)
	}
	if reconcile, err := s.FailWeftSegment(run.ID, 1, ClassDied, "worker exited with no terminal event"); err != nil || !reconcile {
		t.Fatalf("lost landing = %v %v", reconcile, err)
	}
	if err := s.MarkWeftRunning(run.ID, 2, 12346, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"", "halted", "failed", "stopped"} {
		if err := s.RecordWeftObserved(run.ID, 2, outcome, WeftObserved{"done", "not_done", "not_done"}, "2026-10-03T09:01:00.000Z"); err == nil {
			t.Fatalf("observed %q on a finished grade was recorded", outcome)
		}
		if err := s.RecordWeftReconciled(run.ID, 2, outcome, WeftLanding{State: WeftSettlementNone}); err == nil {
			t.Fatalf("reconciled %q on a finished grade was recorded", outcome)
		}
	}
	got, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome.String != "finished" || got.SettlementState != WeftSettlementReconciling || len(got.Steps) != 1 ||
		got.Steps[0].Result.String != "done" || got.Steps[0].Observed.Valid || got.Segments[1].Terminal.Valid {
		t.Fatalf("finished run after refused readings = %+v", got)
	}
}

// A reconcile's reading of a grade is the lost segment's grade (row 19): a
// landing lost before the host recorded its grade, whose reconcile reads
// failed from runs.jsonl, is not resumable after reap — neither admitted nor
// claimed — although the lost segment carries a host class; one that reads
// stopped is.
func TestReconciledGradeIsTheLostSegmentsGrade(t *testing.T) {
	errRollback := errors.New("rollback")
	for _, tt := range []struct {
		outcome   string
		resumable bool
	}{
		{"failed", false},
		{"stopped", true},
	} {
		t.Run(tt.outcome, func(t *testing.T) {
			s := newStore(t)
			run := landingRun(t, s, "demo/read-"+tt.outcome)
			if reconcile, err := s.FailWeftSegment(run.ID, 1, ClassDied, "worker exited with no terminal event"); err != nil || !reconcile {
				t.Fatalf("lost landing = %v %v", reconcile, err)
			}
			if err := s.MarkWeftRunning(run.ID, 2, 12346, "weft-worker/v2"); err != nil {
				t.Fatal(err)
			}
			if err := s.RecordWeftObserved(run.ID, 2, tt.outcome, WeftObserved{"not_done", "not_done", "not_done"}, "2026-10-03T09:00:00.000Z"); err != nil {
				t.Fatal(err)
			}
			if err := s.RecordWeftReconciled(run.ID, 2, tt.outcome, WeftLanding{State: WeftSettlementNone}); err != nil {
				t.Fatal(err)
			}
			if err := s.CompleteWeftSegment(run.ID, 2); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetWeftRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != WeftPhaseEnded || got.Outcome.String != tt.outcome || got.SettlementState != WeftSettlementNone ||
				got.Segments[0].Failure.String != ClassDied || got.ActionRequired.Valid {
				t.Fatalf("reconciled %s = %+v", tt.outcome, got)
			}
			err = s.WeftTx(func(tx *WeftTx) error {
				_, err := tx.AdmitResume(run.ID, got.Revision, func(string) bool { return true }, WeftAdmission{TokenID: "token", RequestKey: "resume"})
				var refusal *WeftRefusal
				switch {
				case tt.resumable && err != nil:
					t.Fatalf("resume of a run read %s = %v", tt.outcome, err)
				case !tt.resumable && (!errors.As(err, &refusal) || refusal.Code != "run_not_resumable"):
					t.Fatalf("resume of a run read %s = %v", tt.outcome, err)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatal(err)
			}
			_, err = s.ClaimWeftResume(run.ID, filepath.Join(t.TempDir(), "runs"))
			if tt.resumable != (err == nil) || (!tt.resumable && !errors.Is(err, ErrWeftRunNotResumable)) {
				t.Fatalf("claim of a resume of a run read %s = %v", tt.outcome, err)
			}
		})
	}
}

// A landing segment lost on a run that has claimed all its segments can
// claim no reconcile: its landing is held unreconciled at once, as a lost
// reconcile's is, with landing_unsettled and its repository paused at this
// reap, and its row released.
func TestLostLandingAtTheSegmentLimitIsHeldUnreconciled(t *testing.T) {
	s := newStore(t)
	run := landingRun(t, s, "demo/at-the-limit")
	if err := s.RecordWeftGraded(run.ID, 1, "finished", false, "", "", "", "", "head", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE weft_segments SET segment=? WHERE run_id=? AND segment=1`, WeftSegmentLimit, run.ID); err != nil {
		t.Fatal(err)
	}
	if reconcile, err := s.FailWeftSegment(run.ID, WeftSegmentLimit, ClassDied, "worker exited with no terminal event"); err != nil || reconcile {
		t.Fatalf("lost landing at the limit = %v %v", reconcile, err)
	}
	got, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != WeftPhaseEnded || got.Outcome.String != "finished" || got.SettlementState != WeftSettlementNotLanded ||
		got.SettlementReason.String != WeftLandUnreconciled || got.SettlementStep.String != WeftStepFastForward ||
		!strings.Contains(got.SettlementDetail.String, "claimed all") || got.ActionRequired.String != WeftActionLandingUnsettled ||
		got.SettlementCommit.String != "head" ||
		len(got.Segments) != 1 {
		t.Fatalf("lost landing at the limit = %+v", got)
	}
	var reservations, entries int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_reservations`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("reservations = %d %v", reservations, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_entries WHERE kind=?`, WeftEntryReconcile).Scan(&entries); err != nil || entries != 0 {
		t.Fatalf("reconcile entries = %d %v", entries, err)
	}
	err = s.WeftTx(func(tx *WeftTx) error {
		repo, err := tx.Repository(got.RepositoryID)
		if err != nil {
			return err
		}
		if repo.Dispatch != WeftDispatchPaused || repo.PausedRun.String != run.ID {
			t.Fatalf("repository = %+v", repo)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A reconcile lost after reading a grade other than finished settles as row
// 19 does (row 22u): the reading is the run's outcome and its lost
// segment's grade, nothing lands, and nothing is held or paused. A grade the
// worker wrote halted or failed is never resumable.
func TestLostReconcileReadingAnUnfinishedGradeSettlesAsReconciled(t *testing.T) {
	for _, outcome := range []string{"halted", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			s := newStore(t)
			run := landingRun(t, s, "demo/lost-"+outcome)
			if reconcile, err := s.FailWeftSegment(run.ID, 1, ClassDied, "worker exited with no terminal event"); err != nil || !reconcile {
				t.Fatalf("lost landing = %v %v", reconcile, err)
			}
			if err := s.MarkWeftRunning(run.ID, 2, 12346, "weft-worker/v2"); err != nil {
				t.Fatal(err)
			}
			if err := s.RecordWeftObserved(run.ID, 2, outcome, WeftObserved{"not_done", "not_done", "not_done"}, "2026-10-03T09:00:00.000Z"); err != nil {
				t.Fatal(err)
			}
			if reconcile, err := s.FailWeftSegment(run.ID, 2, ClassDied, "reconcile exited with no terminal event"); err != nil || reconcile {
				t.Fatalf("lost reconcile = %v %v", reconcile, err)
			}
			got, err := s.GetWeftRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != WeftPhaseEnded || got.Outcome.String != outcome || got.SettlementState != WeftSettlementNone ||
				got.SettlementReason.Valid || got.ActionRequired.Valid || len(got.Steps) != 0 {
				t.Fatalf("lost reconcile that read %s = %+v", outcome, got)
			}
			var reservations int
			if err := s.db.QueryRow(`SELECT count(*) FROM weft_reservations`).Scan(&reservations); err != nil || reservations != 0 {
				t.Fatalf("reservations = %d %v", reservations, err)
			}
			err = s.WeftTx(func(tx *WeftTx) error {
				repo, err := tx.Repository(got.RepositoryID)
				if err != nil {
					return err
				}
				if repo.Dispatch == WeftDispatchPaused {
					t.Fatalf("repository = %+v", repo)
				}
				resumable, err := weftResumableTx(tx.tx, got)
				if err != nil || resumable {
					t.Fatalf("resumable = %v %v", resumable, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A not_landed settlement names the reviewed head the host recorded, never
// the worker's word: a report naming another commit is refused, and one
// naming none still reads the graded head.
func TestNotLandedNamesTheReviewedHead(t *testing.T) {
	s := newStore(t)
	run := landingRun(t, s, "demo/reviewed")
	if err := s.RecordWeftGraded(run.ID, 1, "finished", false, "", "", "", "", "head", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordWeftLandIntent(run.ID, 1, WeftStepFastForward, "main", "head", "base", "2026-10-03T09:00:00.000Z"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftLandResult(run.ID, 1, WeftStepFastForward, "refused", "dirty", "2026-10-03T09:00:01.000Z"); err != nil {
		t.Fatal(err)
	}
	other := WeftLanding{State: WeftSettlementNotLanded, Step: WeftStepFastForward, Reason: WeftLandRefused, Commit: "other"}
	if err := s.RecordWeftLanding(run.ID, 1, other); err == nil || !strings.Contains(err.Error(), "reviewed head") {
		t.Fatalf("not_landed of another commit = %v", err)
	}
	none := WeftLanding{State: WeftSettlementNotLanded, Step: WeftStepFastForward, Reason: WeftLandRefused}
	if err := s.RecordWeftLanding(run.ID, 1, none); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SettlementState != WeftSettlementNotLanded || got.SettlementCommit.String != "head" {
		t.Fatalf("not_landed naming no commit = %+v", got)
	}
}
