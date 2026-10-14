package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"shuttle/internal/sandbox"
)

// Legacy history (weft-service/v2 §14.3).
const (
	WeftArchiveVersion  = "weft-archive/v1"
	WeftOriginArchive   = "archive"
	WeftRecordedArchive = WeftArchiveVersion
	// WeftArchivedSegment is the segment an archived run reads as: a legacy
	// run was one run of the app, and the host gave it no segment rows.
	WeftArchivedSegment = 1
)

// WeftArchive is a weft-archive/v1 document as the host imports it.
type WeftArchive struct {
	ArchiveID string
	Origin    json.RawMessage
	Runs      []WeftArchiveRun
}

// WeftArchiveRun is one legacy run, as recorded: its references are
// historical data, copied and never recomputed. Repo is the one it recorded,
// or the repository its project is bound to here when it recorded none.
type WeftArchiveRun struct {
	LegacyRunID, Ticket, Project, Repo, Branch, Worktree, Base, StartedAt string
	EndedAt, Outcome                                                      sql.NullString
	HaltStage, HaltReason, HaltDetail                                     sql.NullString
	// Land is the legacy land outcome, or nil when it recorded none.
	Land    *WeftArchiveLand
	Stages  []WeftStage
	Summary json.RawMessage
}

// WeftArchiveLand is the legacy app's land outcome.
type WeftArchiveLand struct {
	Landed                 bool
	Branch, Commit, Detail sql.NullString
}

// WeftArchiveImport is an import as archive-import-response.json reports it.
type WeftArchiveImport struct {
	ArchiveID, ImportedAt string
	Origin                json.RawMessage
	Imported, Existing    int
	RunIDs                []string
}

func (i *WeftArchiveImport) Resource() map[string]any {
	return map[string]any{
		"archive_id": i.ArchiveID, "origin": i.Origin, "imported_at": i.ImportedAt,
		"runs_imported": i.Imported, "runs_existing": i.Existing, "run_ids": i.RunIDs,
	}
}

// WeftArchivedRunID is the host's id for a legacy run.
func WeftArchivedRunID(legacy string) string { return "x-" + legacy }

// ImportArchive imports a legacy archive (§14.3) as archived runs: never live
// and never resumable — no segment, no reservation, no question, no answer.
// It is idempotent by archive id and content: the same archive answers the
// recorded import, other content under the same id is archive_conflict, and
// a run another archive already imported counts as existing.
func (t *WeftTx) ImportArchive(doc *WeftArchive, sha string) (*WeftArchiveImport, error) {
	imp := &WeftArchiveImport{ArchiveID: doc.ArchiveID, RunIDs: []string{}}
	var recordedSHA, origin string
	err := t.tx.QueryRow(`SELECT sha256,origin,imported_at FROM weft_archives WHERE id=?`, doc.ArchiveID).
		Scan(&recordedSHA, &origin, &imp.ImportedAt)
	switch {
	case err == nil:
		if recordedSHA != sha {
			return nil, refuse("archive_conflict", "archive %s was imported at %s with content %s; this body's content is %s",
				doc.ArchiveID, imp.ImportedAt, recordedSHA, sha)
		}
		imp.Origin = json.RawMessage(origin)
		return imp, t.countImport(imp, doc)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	imp.ImportedAt, imp.Origin = t.Stamp(), doc.Origin
	if _, err := t.tx.Exec(`INSERT INTO weft_archives(id,sha256,origin,imported_at) VALUES(?,?,?,?)`,
		doc.ArchiveID, sha, string(doc.Origin), imp.ImportedAt); err != nil {
		return nil, err
	}
	for _, r := range doc.Runs {
		id := WeftArchivedRunID(r.LegacyRunID)
		var exists bool
		if err := t.tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM weft_runs WHERE id=?)`, id).Scan(&exists); err != nil {
			return nil, err
		}
		if exists {
			continue
		}
		if err := t.importRun(id, doc.ArchiveID, r, imp.ImportedAt); err != nil {
			return nil, fmt.Errorf("import %s: %w", id, err)
		}
	}
	t.Changes = append(t.Changes, WeftChange{Action: "imported", Kind: "archive", Detail: doc.ArchiveID})
	return imp, t.countImport(imp, doc)
}

// countImport fills the import's counts from what the ledger holds, so a
// replay answers exactly what the first import did.
func (t *WeftTx) countImport(imp *WeftArchiveImport, doc *WeftArchive) error {
	for _, r := range doc.Runs {
		id := WeftArchivedRunID(r.LegacyRunID)
		imp.RunIDs = append(imp.RunIDs, id)
		var mine bool
		if err := t.tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM weft_runs WHERE id=? AND archive_id=?)`, id, doc.ArchiveID).
			Scan(&mine); err != nil {
			return err
		}
		if mine {
			imp.Imported++
		} else {
			imp.Existing++
		}
	}
	return nil
}

// importRun writes one legacy run as archived history. Its settlement says
// what the legacy app recorded of its landing: a landed run carries the three
// steps as observed done, recorded at the run's end, since the app kept no
// intent; anything else lands nothing on this host.
func (t *WeftTx) importRun(id, archiveID string, r WeftArchiveRun, importedAt string) error {
	settlement := WeftLanding{State: WeftSettlementNone,
		Detail: "recorded under " + WeftArchiveVersion + ": the legacy run recorded no landing"}
	if l := r.Land; l != nil {
		switch {
		case l.Landed && l.Branch.Valid && l.Commit.Valid:
			settlement = WeftLanding{State: WeftSettlementLanded, Branch: l.Branch.String, Commit: l.Commit.String}
		case l.Landed:
			settlement.Detail = "recorded under " + WeftArchiveVersion + ": the legacy run landed without naming its commit"
		case l.Detail.Valid:
			settlement.Detail = "recorded under " + WeftArchiveVersion + ": the legacy run did not land: " + l.Detail.String
		default:
			settlement.Detail = "recorded under " + WeftArchiveVersion + ": the legacy run did not land"
		}
	}
	state, _, _, detail, branch, commit := settlementOf(settlement)
	summary := nullJSON(r.Summary)
	if _, err := t.tx.Exec(`INSERT INTO weft_runs(id,ticket,project,repo,branch,worktree,base,phase,outcome,
		halt_stage,halt_reason,halt_detail,stopped,summary,created_at,updated_at,ended_at,repository_id,origin,
		recorded_under,archive_id,settlement_state,settlement_detail,settlement_branch,settlement_commit,disposition)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, r.Ticket, r.Project, r.Repo, r.Branch, r.Worktree, nullString(r.Base), WeftPhaseEnded, r.Outcome,
		r.HaltStage, r.HaltReason, r.HaltDetail, r.Outcome.String == "stopped", summary, r.StartedAt, importedAt,
		r.EndedAt, sandbox.RepositoryID(r.Repo), WeftOriginArchive, WeftRecordedArchive, archiveID, state, detail,
		branch, commit, WeftDispositionRetained); err != nil {
		return err
	}
	for _, st := range r.Stages {
		if _, err := t.tx.Exec(`INSERT INTO weft_stages(
			run_id,segment,stage,occurrence,attempt,backend,state,session,detail,exit_code,turns,spend,processing_ms,tokens,at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(run_id,segment,stage,occurrence,attempt) DO UPDATE SET
			backend=excluded.backend,state=excluded.state,session=excluded.session,detail=excluded.detail,
			exit_code=excluded.exit_code,turns=excluded.turns,spend=excluded.spend,
			processing_ms=excluded.processing_ms,tokens=excluded.tokens,at=excluded.at`,
			id, WeftArchivedSegment, st.Stage, st.Occurrence, st.Attempt, st.Backend, st.State, st.Session, st.Detail,
			st.ExitCode, st.Turns, nullJSON(st.Spend), st.Processing, nullJSON(st.Tokens), st.At); err != nil {
			return err
		}
	}
	if state == WeftSettlementLanded {
		at := firstNonempty(r.EndedAt.String, r.StartedAt)
		for _, step := range WeftLandSteps {
			if _, err := t.tx.Exec(`INSERT INTO weft_land_steps(run_id,step,intent_at,observed,detail) VALUES(?,?,?,'done',?)`,
				id, step, at, "recorded by "+WeftArchiveVersion+" archive "+archiveID); err != nil {
				return err
			}
		}
	}
	return appendRunEventTx(t.tx, id, importedAt)
}
