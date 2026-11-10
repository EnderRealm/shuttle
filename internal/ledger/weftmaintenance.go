package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// WeftDispositionCleaned is a run whose worktree a cleanup removed (§6.8).
const WeftDispositionCleaned = "cleaned"

// WeftHostToken is the token_id of a cleanup entry the host admitted itself
// (§6.8). It has no weft_requests row to name, so its request_key is its own
// id.
const WeftHostToken = "host"

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
// fresh entry against the run it acts on as that run is now. A host-started
// cleanup also re-tests its trigger (§6.8): its ticket through finished, then
// the rest of what made the run qualify. finished is nil on a host that does
// not serve cleanup, where the ticket is not re-tested.
func (t *WeftTx) revalidateMaintenance(e *WeftEntry, finished func(string) bool) (string, string, error) {
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
	case e.Kind != WeftEntryCleanup || finished == nil:
		return "", "", nil
	}
	host, err := hostStarted(t.tx, e.ID)
	if err != nil || !host {
		return "", "", err
	}
	if !finished(run.Ticket) {
		return WeftInvalidTicket, fmt.Sprintf("%s no longer reads done or closed in tk", run.Ticket), nil
	}
	blocked, err := t.hostCleanupBlocked(run, e.ID)
	if err != nil || blocked == "" {
		return "", "", err
	}
	return WeftInvalidRun, blocked, nil
}

// hostStarted is whether the entry is one the host admitted itself.
func hostStarted(q querier, entryID string) (bool, error) {
	var token string
	err := q.QueryRow(`SELECT token_id FROM weft_entries WHERE id=?`, entryID).Scan(&token)
	return token == WeftHostToken, err
}

// hostCleanupBlocked is why the run does not qualify for a host-started
// cleanup (§6.8), its ticket aside, or "" when it does: a hosted run, still
// retained, with no process, a settlement nothing will move — not_applicable
// or landed, or pending on a parked run, which has no worker to intend a step
// — no entry naming it waiting or archiving but except, and no hold.
func (t *WeftTx) hostCleanupBlocked(run *WeftRun, except string) (string, error) {
	var hold bool
	if err := t.tx.QueryRow(`SELECT host_cleanup_hold FROM weft_runs WHERE id=?`, run.ID).Scan(&hold); err != nil {
		return "", err
	}
	active, err := weftActiveTx(t.tx, run)
	if err != nil {
		return "", err
	}
	settled := run.SettlementState == WeftSettlementNone || run.SettlementState == WeftSettlementLanded ||
		(run.SettlementState == WeftSettlementPending && run.Phase == WeftPhaseParked)
	switch {
	case run.Origin != WeftOriginHosted:
		return fmt.Sprintf("run %s is %s history", run.ID, run.Origin), nil
	case run.Disposition != WeftDispositionRetained:
		return fmt.Sprintf("run %s's worktree is already %s", run.ID, run.Disposition), nil
	case hold:
		return fmt.Sprintf("a cleanup of run %s was refused or cancelled, so only a dismiss cleans it", run.ID), nil
	case active:
		return fmt.Sprintf("run %s is %s and has a process", run.ID, WeftV2Phase(run.Phase)), nil
	case !settled:
		return fmt.Sprintf("run %s's landing is %s%s", run.ID, run.SettlementState, reasonSuffix(run.SettlementReason)), nil
	}
	var entry, kind, state string
	err = t.tx.QueryRow(`SELECT id,kind,state FROM weft_entries WHERE (run_id=? OR (kind=? AND supersedes=?))
		AND state IN (?,?) AND id<>? ORDER BY position LIMIT 1`, run.ID, WeftEntryFresh, run.ID,
		WeftEntryWaiting, WeftEntryArchiving, except).Scan(&entry, &kind, &state)
	switch {
	case err == nil:
		return fmt.Sprintf("run %s has %s entry %s %s", run.ID, kind, entry, state), nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", err
	}
	return "", nil
}

// CleanFinished is §6.8's evaluation of every retained hosted run of
// projects — of every project when projects is nil — for a host-started
// cleanup: on a catalog change, after its revalidation, and at restart.
func (t *WeftTx) CleanFinished(projects []string, d WeftDispatch) error {
	if d.Finished == nil || (projects != nil && len(projects) == 0) {
		return nil
	}
	query := `SELECT id FROM weft_runs WHERE origin=? AND disposition=? AND host_cleanup_hold=0`
	args := []any{WeftOriginHosted, WeftDispositionRetained}
	if projects != nil {
		query += ` AND project IN (?` + strings.Repeat(`,?`, len(projects)-1) + `)`
		for _, p := range projects {
			args = append(args, p)
		}
	}
	rows, err := t.tx.Query(query+` ORDER BY created_at,id`, args...)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return t.cleanFinished(ids, d)
}

// CleanFinishedRun evaluates one run for a host-started cleanup (§6.8), in a
// commit that may have made it qualify: an entry naming it settled.
func (t *WeftTx) CleanFinishedRun(runID string, d WeftDispatch) error {
	return t.cleanFinished([]string{runID}, d)
}

// cleanFinished admits, for each of runs that qualifies and whose ticket
// d.Finished reads done or closed, the cleanup entry a dismiss would — as
// the host admits its own, with no request behind it — and dispatches it in
// this transaction. A nil d.Finished is a host that does not serve cleanup,
// which cleans nothing itself.
func (t *WeftTx) cleanFinished(runIDs []string, d WeftDispatch) error {
	if d.Finished == nil {
		return nil
	}
	for _, id := range runIDs {
		run, err := getWeftRunTx(t.tx, id)
		if err != nil {
			return err
		}
		blocked, err := t.hostCleanupBlocked(run, "")
		if err != nil {
			return err
		}
		if blocked != "" || !d.Finished(run.Ticket) {
			continue
		}
		unbound, err := t.hostCleanupUnbound(run, d)
		if err != nil {
			return err
		}
		if unbound {
			continue
		}
		a := WeftAdmission{Kind: WeftEntryCleanup, Ticket: run.Ticket, Project: run.Project, RunID: run.ID,
			RepositoryID: run.RepositoryID, Repository: run.Repo, TokenID: WeftHostToken}
		e, err := t.insertEntryNoting(a, "host-started: "+run.Ticket+" is done or closed in tk")
		if err != nil {
			return err
		}
		paused, _, _, err := weftHostPaused(t.tx)
		if err != nil {
			return err
		}
		c, err := weftCapacity(t.tx)
		if err != nil {
			return err
		}
		if _, err := t.dispatchEntry(e, d, paused, &c); err != nil {
			return err
		}
	}
	return nil
}

// hostCleanupUnbound is whether the run's last host-started cleanup was
// invalidated binding_changed and d.Check still answers that for the cleanup
// it would admit now. A cleanup that cannot proceed stays retained with its
// reason and is not retried (§6.8): admitted again, it would only be
// invalidated again, one more entry on every catalog change.
func (t *WeftTx) hostCleanupUnbound(run *WeftRun, d WeftDispatch) (bool, error) {
	var state string
	var reason sql.NullString
	err := t.tx.QueryRow(`SELECT state,invalidated_reason FROM weft_entries WHERE run_id=? AND kind=? AND token_id=?
		ORDER BY position DESC LIMIT 1`, run.ID, WeftEntryCleanup, WeftHostToken).Scan(&state, &reason)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	case state != WeftEntryInvalidated || reason.String != WeftInvalidBinding:
		return false, nil
	}
	again, _ := d.Check(&WeftEntry{Kind: WeftEntryCleanup, Ticket: run.Ticket, Project: run.Project,
		RepositoryID: run.RepositoryID, Repository: run.Repo, RunID: sql.NullString{String: run.ID, Valid: true}})
	return again == WeftInvalidBinding, nil
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
// cleanup_refused: its worker removed nothing, and the run is unchanged but
// for its host cleanup hold, which no payload carries (§6.8). held is whether
// there was an entry to refuse, and so a hold set.
func refuseCleanupTx(tx *sql.Tx, runID, detail, stamp string) (held bool, err error) {
	var id string
	err = tx.QueryRow(`SELECT id FROM weft_entries WHERE run_id=? AND kind=? AND state=? ORDER BY position DESC LIMIT 1`,
		runID, WeftEntryCleanup, WeftEntryDispatched).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(`UPDATE weft_entries SET state=?,invalidated_reason=?,invalidated_detail=?,updated_at=?,
		revision=revision+1 WHERE id=?`, WeftEntryInvalidated, WeftInvalidCleanup, detail, stamp, id); err != nil {
		return false, err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET host_cleanup_hold=1 WHERE id=?`, runID); err != nil {
		return false, err
	}
	return true, queueStepTx(tx, stamp, false, id)
}

// RecordWeftMaintained records a cleanup or archive worker's terminal event
// (§11.3). A cleanup done leaves the run cleaned — its history and branch
// stay; one refused invalidates its entry and holds the run from host
// cleanup, changing nothing else. A parked run the host cleaned is ended as
// a stop ends one, its pending question withdrawn: nothing can answer it once
// its worktree is gone (§6.8). An archive's result is settled at reap, by
// SettleWeftArchive. held is whether a refusal set the run's hold.
func (s *Store) RecordWeftMaintained(runID string, segment int, op, result, detail string) (held bool, err error) {
	err = s.withTx(func(tx *sql.Tx) error {
		stamp := weftTimestamp(s.now())
		if _, err := tx.Exec(`UPDATE weft_segments SET terminal='maintained' WHERE run_id=? AND segment=?`, runID, segment); err != nil {
			return err
		}
		if op != WeftSegmentCleanup {
			return nil
		}
		if result != "done" {
			held, err = refuseCleanupTx(tx, runID, detail, stamp)
			return err
		}
		var token string
		err := tx.QueryRow(`SELECT token_id FROM weft_entries WHERE run_id=? AND kind=? AND state=? ORDER BY position DESC LIMIT 1`,
			runID, WeftEntryCleanup, WeftEntryDispatched).Scan(&token)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		run, err := getWeftRunTx(tx, runID)
		if err != nil {
			return err
		}
		if token == WeftHostToken && run.Phase == WeftPhaseParked {
			if err := stopWeftParkedTx(tx, runID, stamp); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE weft_runs SET disposition=?,updated_at=?,revision=revision+1 WHERE id=?`,
			WeftDispositionCleaned, stamp, runID); err != nil {
			return err
		}
		return appendRunEventTx(tx, runID, stamp)
	})
	return held, err
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
		// The refused fresh entry was the run's to settle: the run is
		// retained as it was, and may now qualify for host cleanup (§6.8).
		if err := t.refuseArchive(e, report.Detail, stamp); err != nil {
			return err
		}
		return t.CleanFinishedRun(runID, d)
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
// its row, and an archive's slot, released. Nothing else changes but a
// cleanup's run's hold, and held says whether it was set.
func (s *Store) RefuseWeftMaintenance(runID string, segment int, detail string) (held bool, err error) {
	err = s.WeftTx(func(t *WeftTx) error {
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
			if held, err = refuseCleanupTx(t.tx, runID, detail, stamp); err != nil {
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
	return held, err
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
