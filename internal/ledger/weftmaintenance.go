package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
)

// WeftDispositionCleaned is a run whose worktree a cleanup removed (§6.8).
const WeftDispositionCleaned = "cleaned"

// WeftArchiveRef is the ref a fresh entry's archive step writes for the run
// it supersedes (§6.7).
func WeftArchiveRef(runID string) string { return "refs/weft/archive/" + runID }

// weftActiveTx reports whether the run has a process or is about to: a phase
// with a worker, or a segment the host has not reaped.
func weftActiveTx(tx *sql.Tx, run *WeftRun) (bool, error) {
	if slices.Contains([]string{WeftPhaseQueued, WeftPhaseRunning, WeftPhaseSettling}, run.Phase) {
		return true, nil
	}
	var live bool
	err := tx.QueryRow(`SELECT `+weftInFlightSQL, run.ID).Scan(&live)
	return live, err
}

// unsettled is a landing in motion: steps intended, or a reconcile reading
// what a lost segment did. Neither cleanup nor fresh work may race it.
func unsettled(run *WeftRun) bool {
	return run.SettlementState == WeftSettlementInProgress || run.SettlementState == WeftSettlementReconciling
}

// commandRunTx reads the run a command names and holds it to the revision
// the client was shown. An archived run is history and takes no command
// (§14.3): refusal answers it with code.
func (t *WeftTx) commandRunTx(runID string, revision int64, code string) (*WeftRun, error) {
	run, err := getWeftRunTx(t.tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, refuse("run_not_found", "%s is not a run on this host", runID)
	}
	if err != nil {
		return nil, err
	}
	if run.Origin == WeftOriginArchive {
		return nil, refuse(code, "run %s is legacy history imported from archive %s; it is never live and takes no command",
			runID, run.ArchiveID.String)
	}
	if run.Revision != revision {
		return nil, refuse("revision_conflict", "run %s is at revision %d and the request was for %d", runID, run.Revision, revision)
	}
	return run, nil
}

// AdmitReconcile queues the operator's retry of a landing that did not land
// (§7.4): accepted for one that failed, was stopped or could not be
// reconciled — a refused precondition refuses again — and never for a run
// fresh work archived: its head is history, not work to land.
func (t *WeftTx) AdmitReconcile(runID string, revision int64, a WeftAdmission) (*WeftEntry, error) {
	run, err := t.commandRunTx(runID, revision, "not_reconcilable")
	if err != nil {
		return nil, err
	}
	if archived(run) {
		return nil, refuse("not_reconcilable", "run %s is archived, superseded by %s; its landing is history", runID, run.SupersededBy.String)
	}
	if run.SettlementState != WeftSettlementNotLanded ||
		!slices.Contains([]string{WeftLandFailed, WeftLandStopped, WeftLandUnreconciled}, run.SettlementReason.String) {
		return nil, refuse("not_reconcilable", "run %s's landing is %s%s; only one not_landed for failed, stopped or unreconciled is retried",
			runID, run.SettlementState, reasonSuffix(run.SettlementReason))
	}
	// A run lost before the host recorded its graded head can only be
	// observed: no reconcile of it is given a commit to land.
	if run.Outcome.String != "finished" || run.Head.String == "" || run.Base.String == "" {
		return nil, refuse("not_reconcilable",
			"run %s has no graded head recorded, so a reconcile can only observe it; start fresh work instead", runID)
	}
	var queued string
	err = t.tx.QueryRow(`SELECT id FROM weft_entries WHERE run_id=? AND kind=? AND state=?`,
		runID, WeftEntryReconcile, WeftEntryWaiting).Scan(&queued)
	switch {
	case err == nil:
		return nil, refuse("not_reconcilable", "run %s's reconcile is already queued as %s", runID, queued)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	// The operator's retry is a new attempt, so it lifts a stop of the last.
	if run.Stopped {
		stamp := t.Stamp()
		if _, err := t.tx.Exec(`UPDATE weft_runs SET stopped=0,updated_at=?,revision=revision+1 WHERE id=?`,
			stamp, runID); err != nil {
			return nil, err
		}
		if err := appendRunEventTx(t.tx, runID, stamp); err != nil {
			return nil, err
		}
	}
	a.Kind, a.Ticket, a.Project, a.RunID = WeftEntryReconcile, run.Ticket, run.Project, runID
	a.RepositoryID, a.Repository = run.RepositoryID, run.Repo
	return t.insertEntry(a)
}

// archived is a run fresh work archived and superseded (§6.7).
func archived(run *WeftRun) bool {
	return run.Disposition == WeftDispositionArchived || run.SupersededBy.Valid
}

func reasonSuffix(reason sql.NullString) string {
	if !reason.Valid {
		return ""
	}
	return " (" + reason.String + ")"
}

// AdmitCleanup queues the dismissal of a run's worktree (§6.8), the worktree
// the confirmation showed: refused while the run has a process or a resume
// queued, while its landing is in motion, or once its worktree is gone.
func (t *WeftTx) AdmitCleanup(runID string, revision int64, worktree string, worktreeExists func(string) bool, a WeftAdmission) (*WeftEntry, error) {
	run, err := t.commandRunTx(runID, revision, "run_over")
	if err != nil {
		return nil, err
	}
	if err := t.requireIdle(run); err != nil {
		return nil, err
	}
	if run.Disposition != WeftDispositionRetained {
		return nil, refuse("worktree_missing", "run %s's worktree %s is already %s", runID, run.Worktree, run.Disposition)
	}
	if !worktreeExists(run.Worktree) {
		return nil, refuse("worktree_missing", "run %s's worktree %s is missing", runID, run.Worktree)
	}
	if worktree != run.Worktree {
		return nil, refuse("revision_conflict", "run %s's worktree is %s, not %s as the confirmation showed", runID, run.Worktree, worktree)
	}
	a.Kind, a.Ticket, a.Project, a.RunID = WeftEntryCleanup, run.Ticket, run.Project, runID
	a.RepositoryID, a.Repository = run.RepositoryID, run.Repo
	return t.insertEntry(a)
}

// refuseUnderCleanup refuses a resume or answer of a run whose worktree a
// cleanup is queued, or running, to remove.
func (t *WeftTx) refuseUnderCleanup(runID string) error {
	var id, state string
	err := t.tx.QueryRow(`SELECT id,state FROM weft_entries WHERE run_id=? AND kind=? AND state IN (?,?)
		ORDER BY position DESC LIMIT 1`, runID, WeftEntryCleanup, WeftEntryWaiting, WeftEntryDispatched).Scan(&id, &state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if state == WeftEntryDispatched {
		var live bool
		if err := t.tx.QueryRow(`SELECT `+weftInFlightSQL, runID).Scan(&live); err != nil || !live {
			return err
		}
	}
	return refuse("run_active", "run %s's worktree is queued for cleanup by entry %s", runID, id)
}

// requireIdle refuses a cleanup or fresh command while the run has a
// process, an answer or resume queued, or a landing in motion.
func (t *WeftTx) requireIdle(run *WeftRun) error {
	active, err := weftActiveTx(t.tx, run)
	if err != nil {
		return err
	}
	if active {
		return refuse("run_active", "run %s is %s and has a process", run.ID, WeftV2Phase(run.Phase))
	}
	var queued, kind string
	err = t.tx.QueryRow(`SELECT id,kind FROM weft_entries WHERE run_id=? AND state=? AND kind IN (?,?)`,
		run.ID, WeftEntryWaiting, WeftEntryAnswer, WeftEntryResume).Scan(&queued, &kind)
	switch {
	case err == nil:
		return refuse("run_active", "run %s has %s entry %s queued", run.ID, kind, queued)
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	if unsettled(run) {
		return refuse("settlement_unsettled", "run %s's landing is %s at %s", run.ID, run.SettlementState, run.SettlementStep.String)
	}
	return nil
}

// AdmitFresh queues fresh work over a retained run (§6.7): its head and
// worktree as the confirmation showed them are archived, and a new run of
// the ticket starts from current main. A run with no recorded head — one
// refused branch_exists — is archived at the head the confirmation
// authorized, which the archive worker verifies against the branch; the
// caller has held head to a commit's form. The entry records the head it
// archives.
func (t *WeftTx) AdmitFresh(runID string, revision int64, worktree, head string, a WeftAdmission) (*WeftEntry, error) {
	run, err := t.commandRunTx(runID, revision, "run_over")
	if err != nil {
		return nil, err
	}
	if run.Disposition == WeftDispositionArchived {
		return nil, refuse("run_over", "run %s is already archived, superseded by %s", runID, run.SupersededBy.String)
	}
	if err := t.requireIdle(run); err != nil {
		return nil, err
	}
	if err := t.guard(run.Ticket, runID); err != nil {
		return nil, err
	}
	if worktree != run.Worktree || (run.Head.Valid && head != run.Head.String) {
		return nil, refuse("revision_conflict", "run %s retains worktree %s at head %s, not %s at %s as the confirmation showed",
			runID, run.Worktree, run.Head.String, worktree, head)
	}
	a.Kind, a.Ticket, a.Project, a.Supersedes = WeftEntryFresh, run.Ticket, run.Project, runID
	a.RepositoryID, a.Repository = run.RepositoryID, run.Repo
	e, err := t.insertEntry(a)
	if err != nil {
		return nil, err
	}
	if _, err := t.tx.Exec(`UPDATE weft_entries SET archive_head=? WHERE id=?`, head, e.ID); err != nil {
		return nil, err
	}
	return e, nil
}

// weftArchiveHead is the head a fresh entry archives its run at.
func weftArchiveHead(q querier, entryID string) (string, error) {
	var head sql.NullString
	err := q.QueryRow(`SELECT archive_head FROM weft_entries WHERE id=?`, entryID).Scan(&head)
	return head.String, err
}

// WeftArchiveHead is the head the run's archiving fresh entry archives it
// at: the archive worker's --head, and what its done must name.
func (s *Store) WeftArchiveHead(runID string) (string, error) {
	var head sql.NullString
	err := s.db.QueryRow(`SELECT archive_head FROM weft_entries WHERE supersedes=? AND state=?`, runID, WeftEntryArchiving).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("run %s has no fresh entry archiving it", runID)
	}
	return head.String, err
}

// revalidateMaintenance is dispatch's recheck of a reconcile, cleanup or
// fresh entry against the run it acts on as that run is now.
func (t *WeftTx) revalidateMaintenance(e *WeftEntry) (string, string, error) {
	runID := e.RunID.String
	if e.Kind == WeftEntryFresh {
		runID = e.Supersedes.String
	}
	run, err := getWeftRunTx(t.tx, runID)
	if err != nil {
		return "", "", err
	}
	active, err := weftActiveTx(t.tx, run)
	if err != nil {
		return "", "", err
	}
	if e.Kind == WeftEntryCleanup {
		// A resume or answer queued while the cleanup waited needs the
		// worktree the cleanup would remove.
		var queued, kind string
		err := t.tx.QueryRow(`SELECT id,kind FROM weft_entries WHERE run_id=? AND state=? AND kind IN (?,?)`,
			run.ID, WeftEntryWaiting, WeftEntryAnswer, WeftEntryResume).Scan(&queued, &kind)
		switch {
		case err == nil:
			return WeftInvalidRun, fmt.Sprintf("run %s has %s entry %s queued", run.ID, kind, queued), nil
		case !errors.Is(err, sql.ErrNoRows):
			return "", "", err
		}
	}
	switch {
	case e.Kind == WeftEntryReconcile && archived(run):
		return WeftInvalidRun, fmt.Sprintf("run %s is archived, superseded by %s", run.ID, run.SupersededBy.String), nil
	case e.Kind == WeftEntryReconcile && run.SettlementState != WeftSettlementNotLanded:
		return WeftInvalidRun, fmt.Sprintf("run %s's landing is %s and no longer needs a reconcile", run.ID, run.SettlementState), nil
	case e.Kind == WeftEntryReconcile:
		return "", "", nil
	case active:
		return WeftInvalidRun, fmt.Sprintf("run %s is %s and has a process", run.ID, WeftV2Phase(run.Phase)), nil
	case unsettled(run):
		return WeftInvalidRun, fmt.Sprintf("run %s's landing is %s", run.ID, run.SettlementState), nil
	case e.Kind == WeftEntryCleanup && run.Disposition != WeftDispositionRetained:
		return WeftInvalidRun, fmt.Sprintf("run %s's worktree is already %s", run.ID, run.Disposition), nil
	case e.Kind == WeftEntryFresh && run.Disposition == WeftDispositionArchived:
		return WeftInvalidRun, fmt.Sprintf("run %s is already archived, superseded by %s", run.ID, run.SupersededBy.String), nil
	}
	return "", "", nil
}

// dispatchMaintenance takes the repository as maintenance for the entry and
// claims its worker's segment (§6.2): a reconcile reading the run's landing
// again, a cleanup of its worktree, or a fresh entry's archive of the run it
// supersedes — which also claims the slot the new run will carry and moves
// the entry to archiving. It answers the run whose segment was claimed and
// whether a slot was taken.
func (t *WeftTx) dispatchMaintenance(e *WeftEntry, d WeftDispatch, stamp string) (string, bool, error) {
	now := t.store.now()
	runID, kind, state := e.RunID.String, e.Kind, WeftEntryDispatched
	if e.Kind == WeftEntryFresh {
		runID, kind, state = e.Supersedes.String, WeftSegmentArchive, WeftEntryArchiving
	}
	run, err := getWeftRunTx(t.tx, runID)
	if err != nil {
		return "", false, err
	}
	// Claimed first: a run at its segment limit takes nothing.
	if _, err := claimMaintenanceSegmentTx(t.tx, runID, kind, d.RunDirRoot, now); err != nil {
		return "", false, err
	}
	if err := acquireRepository(t.tx, e.RepositoryID, e.Repository, WeftHolderMaintenance, e.ID, stamp); err != nil {
		return "", false, err
	}
	if e.Kind == WeftEntryReconcile {
		if _, err := t.tx.Exec(`UPDATE weft_runs SET phase=?,settlement_state=?,settlement_reason=NULL,settlement_detail=?,
			updated_at=?,revision=revision+1 WHERE id=?`, WeftPhaseSettling, WeftSettlementReconciling,
			fmt.Sprintf("entry %s is reconciling the landing of %s", e.ID, run.Head.String), stamp, runID); err != nil {
			return "", false, err
		}
		if err := appendSettlementEventTx(t.tx, runID, stamp); err != nil {
			return "", false, err
		}
	}
	entryRun := sql.NullString{String: runID, Valid: e.Kind != WeftEntryFresh}
	admitted := t.admitted[e.ID]
	delete(t.admitted, e.ID)
	bump := 1
	if admitted {
		stamp, bump = e.UpdatedAt, 0
	}
	if _, err := t.tx.Exec(`UPDATE weft_entries SET state=?,run_id=?,waiting_reason=NULL,holder_kind=NULL,holder_id=NULL,
		updated_at=?,revision=revision+? WHERE id=?`, state, entryRun, stamp, bump, e.ID); err != nil {
		return "", false, err
	}
	return runID, e.Kind == WeftEntryFresh, nil
}

// refuseCleanupTx invalidates the run's dispatched cleanup entry
// cleanup_refused: its worker removed nothing, and the run is unchanged.
func refuseCleanupTx(tx *sql.Tx, runID, detail, stamp string) error {
	var id string
	err := tx.QueryRow(`SELECT id FROM weft_entries WHERE run_id=? AND kind=? AND state=? ORDER BY position DESC LIMIT 1`,
		runID, WeftEntryCleanup, WeftEntryDispatched).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE weft_entries SET state=?,invalidated_reason=?,invalidated_detail=?,updated_at=?,
		revision=revision+1 WHERE id=?`, WeftEntryInvalidated, WeftInvalidCleanup, detail, stamp, id); err != nil {
		return err
	}
	return queueStepTx(tx, stamp, false, id)
}

// RecordWeftMaintained records a cleanup or archive worker's terminal event
// (§11.3). A cleanup done leaves the run cleaned — its history and branch
// stay; one refused invalidates its entry and changes nothing else. An
// archive's result is settled at reap, by SettleWeftArchive.
func (s *Store) RecordWeftMaintained(runID string, segment int, op, result, detail string) error {
	return s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='maintained' WHERE run_id=? AND segment=?`, runID, segment); err != nil {
			return err
		}
		if op != WeftSegmentCleanup {
			return nil
		}
		if result != "done" {
			return refuseCleanupTx(tx, runID, detail, stamp)
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET disposition=?,updated_at=?,revision=revision+1 WHERE id=?`,
			WeftDispositionCleaned, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
}

// WeftArchiveReport is an archive worker's maintained event: done at head,
// or refused with the worker's reason.
type WeftArchiveReport struct {
	Done         bool
	Head, Detail string
}

// SettleWeftArchive settles a reaped archive segment (§6.7). Reported done,
// the maintenance → run transaction mints the new run; refused, the fresh
// entry is invalidated archive_refused and its row and slot released. A
// worker gone without reporting is settled as §12 step 5 settles one: by the
// archive ref archiveHead reads. failClass is the host's failure class for
// the segment's row, or "" when it ended after its terminal event.
func (s *Store) SettleWeftArchive(runID string, segment int, report *WeftArchiveReport, failClass, failDetail string,
	d WeftDispatch, archiveHead func(repo, ref string) (string, error)) (spawn []string, changes []WeftChange, err error) {
	var event Event
	err = s.WeftTx(func(t *WeftTx) error {
		err := t.settleArchive(runID, segment, report, failClass, failDetail, d, archiveHead, &event)
		spawn, changes = t.Spawn, t.Changes
		return err
	})
	if err == nil && s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return spawn, changes, err
}

func (t *WeftTx) settleArchive(runID string, segment int, report *WeftArchiveReport, failClass, failDetail string,
	d WeftDispatch, archiveHead func(repo, ref string) (string, error), event *Event) error {
	stamp := t.Stamp()
	seg, err := segmentStateTx(t.tx, runID, segment)
	if err != nil {
		return err
	}
	if failClass != "" {
		if *event, err = failSegmentRowTx(t.tx, runID, seg, failClass, failDetail, stamp); err != nil {
			return err
		}
	} else {
		*event = Event{Agent: WeftAgent, RunID: seg.ledgerID}
		if err := completeSegmentTx(t.tx, seg.ledgerID, stamp); err != nil {
			return err
		}
	}
	archiving, err := loadWeftEntries(t.tx, `state=? AND supersedes=?`, WeftEntryArchiving, runID)
	if err != nil || len(archiving) == 0 {
		return err
	}
	e := archiving[0]
	switch {
	case report != nil && !report.Done:
		return t.refuseArchive(e, report.Detail, stamp)
	case report != nil:
		return t.settleArchiving(e, d, report.Head)
	}
	ref := WeftArchiveRef(runID)
	head, err := archiveHead(e.Repository, ref)
	if err != nil {
		return fmt.Errorf("entry %s: read %s in %s: %w", e.ID, ref, e.Repository, err)
	}
	return t.settleArchiving(e, d, head)
}

// RefuseWeftMaintenance settles a cleanup or archive segment the host
// refused before launching it — the worktree it would act on is not the one
// the operator authorized. It is a refusal, not a failure: its phase-1 row
// closes skipped, counting toward no failure streak and opening no
// incident; its entry is invalidated cleanup_refused or archive_refused and
// its row, and an archive's slot, released. Nothing else changes.
func (s *Store) RefuseWeftMaintenance(runID string, segment int, detail string) error {
	return s.WeftTx(func(t *WeftTx) error {
		stamp := t.Stamp()
		seg, err := segmentStateTx(t.tx, runID, segment)
		if err != nil {
			return err
		}
		if _, err := requireStatus(t.tx, seg.ledgerID, StatusSkipped, StatusClaimed); err != nil {
			return err
		}
		if _, err := t.tx.Exec(`UPDATE runs SET status=?,skip_reason=?,ended_at=? WHERE id=?`,
			StatusSkipped, SkipRefused, stamp, seg.ledgerID); err != nil {
			return err
		}
		switch seg.kind {
		case WeftSegmentCleanup:
			if err := refuseCleanupTx(t.tx, runID, detail, stamp); err != nil {
				return err
			}
			return releaseMaintenanceTx(t.tx, runID, stamp)
		case WeftSegmentArchive:
			archiving, err := loadWeftEntries(t.tx, `state=? AND supersedes=?`, WeftEntryArchiving, runID)
			if err != nil || len(archiving) == 0 {
				return err
			}
			return t.refuseArchive(archiving[0], detail, stamp)
		}
		return fmt.Errorf("run %s segment %d: a %s segment is not refused before launch", runID, segment, seg.kind)
	})
}

// refuseArchive is a refused archive (§6.7): the fresh entry is invalidated
// archive_refused with the worker's reason, its maintenance row and slot are
// released, and nothing else changes.
func (t *WeftTx) refuseArchive(e *WeftEntry, detail, stamp string) error {
	if _, err := t.tx.Exec(`UPDATE weft_entries SET state=?,invalidated_reason=?,invalidated_detail=?,updated_at=?,
		revision=revision+1 WHERE id=?`, WeftEntryInvalidated, WeftInvalidArchive, detail, stamp, e.ID); err != nil {
		return err
	}
	if err := releaseRepositoriesTx(t.tx, stamp, `holder_kind=? AND holder_id=?`, WeftHolderMaintenance, e.ID); err != nil {
		return err
	}
	t.change("invalidated", e, e.Supersedes.String, WeftInvalidArchive)
	return queueStepTx(t.tx, stamp, true, e.ID)
}
