package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Reservation holders and repository dispatch (weft-service/v2 §6.2, §7.5).
const (
	WeftHolderRun         = "run"
	WeftHolderSettlement  = "settlement"
	WeftHolderMaintenance = "maintenance"
	WeftHolderTerminal    = "terminal"

	WeftDispatchOpen   = "open"
	WeftDispatchPaused = "paused"
	WeftPauseNotLanded = "not_landed"
)

// WeftRepository is one canonical repository: its holder, the projects bound
// to it and whether dispatch to it is paused (repositories.json).
type WeftRepository struct {
	RepositoryID, Repository                        string
	Projects                                        []string
	Reservation                                     *WeftReservation
	Terminals                                       []string
	Dispatch                                        string
	PausedRun, PausedReason, PausedDetail, PausedAt sql.NullString
	Revision                                        int64
}

// Resource is the repository as every response and repository event carries it.
func (r *WeftRepository) Resource() map[string]any {
	var reservation, paused any
	if h := r.Reservation; h != nil {
		reservation = map[string]any{"kind": h.HolderKind, "id": h.HolderID, "since": h.AcquiredAt}
	}
	if r.Dispatch == WeftDispatchPaused {
		paused = map[string]any{"run_id": r.PausedRun.String, "reason": r.PausedReason.String,
			"detail": r.PausedDetail.String, "since": r.PausedAt.String}
	}
	return map[string]any{
		"repository_id": r.RepositoryID, "repository": r.Repository, "projects": r.Projects,
		"reservation": reservation, "terminals": r.Terminals, "dispatch": r.Dispatch,
		"paused": paused, "revision": r.Revision,
	}
}

// weftRepository reads one repository from what the host holds for it: its
// dispatch row, its reservation and the projects bound to it. One the host
// holds nothing for is sql.ErrNoRows.
func weftRepository(q querier, id string) (*WeftRepository, error) {
	r := &WeftRepository{RepositoryID: id, Dispatch: WeftDispatchOpen, Projects: []string{}, Terminals: []string{}}
	known := false
	err := q.QueryRow(`SELECT repository,dispatch,paused_run,paused_reason,paused_detail,paused_at,revision
		FROM weft_repositories WHERE repository_id=?`, id).
		Scan(&r.Repository, &r.Dispatch, &r.PausedRun, &r.PausedReason, &r.PausedDetail, &r.PausedAt, &r.Revision)
	switch {
	case err == nil:
		known = true
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	var h WeftReservation
	err = q.QueryRow(`SELECT repository_id,repository,holder_kind,holder_id,acquired_at FROM weft_reservations
		WHERE repository_id=?`, id).Scan(&h.RepositoryID, &h.Repository, &h.HolderKind, &h.HolderID, &h.AcquiredAt)
	switch {
	case err == nil:
		r.Reservation, known = &h, true
		if r.Repository == "" {
			r.Repository = h.Repository
		}
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	rows, err := q.Query(`SELECT project,repository FROM weft_catalog WHERE repository_id=? ORDER BY project`, id)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var project, repository string
		if err := rows.Scan(&project, &repository); err != nil {
			rows.Close()
			return nil, err
		}
		r.Projects, known = append(r.Projects, project), true
		if r.Repository == "" {
			r.Repository = repository
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = q.Query(`SELECT id FROM weft_terminals WHERE repository_id=? AND state IN ('starting','live','detached')
		ORDER BY created_at,id`, id)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var session string
		if err := rows.Scan(&session); err != nil {
			rows.Close()
			return nil, err
		}
		r.Terminals = append(r.Terminals, session)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if !known {
		return nil, sql.ErrNoRows
	}
	return r, nil
}

// WeftRepositories is every repository the host binds, reserves or has
// paused, by id (GET /v2/repositories).
func (s *Store) WeftRepositories() ([]*WeftRepository, error) {
	rows, err := s.db.Query(`SELECT repository_id FROM weft_repositories UNION
		SELECT repository_id FROM weft_reservations UNION
		SELECT repository_id FROM weft_catalog WHERE repository_id IS NOT NULL ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]*WeftRepository, 0, len(ids))
	for _, id := range ids {
		r, err := weftRepository(s.db, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// touchRepositoryTx raises the repository's revision, creating its row open.
func touchRepositoryTx(tx *sql.Tx, id, repository string) error {
	_, err := tx.Exec(`INSERT INTO weft_repositories(repository_id,repository,dispatch,revision) VALUES(?,?,?,1)
		ON CONFLICT(repository_id) DO UPDATE SET revision=revision+1`, id, repository, WeftDispatchOpen)
	return err
}

type repositoryEventPayload struct {
	ContractVersion string         `json:"contract_version"`
	Seq             int64          `json:"seq"`
	TS              string         `json:"ts"`
	Repository      map[string]any `json:"repository"`
}

// repositoryStepTx is one change to a repository's reservation or dispatch:
// its revision raised and its repository event written (§10).
func repositoryStepTx(tx *sql.Tx, id, repository, stamp string) error {
	if err := touchRepositoryTx(tx, id, repository); err != nil {
		return err
	}
	r, err := weftRepository(tx, id)
	if err != nil {
		return err
	}
	return appendWeftEventTx(tx, "", "repository", stamp, func(seq int64) any {
		return repositoryEventPayload{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp, Repository: r.Resource()}
	})
}

// acquireRepository takes the repository for a holder, with its repository
// event; a repository someone holds is a *RepositoryBusyError naming them.
func acquireRepository(tx *sql.Tx, repositoryID, repo, kind, holderID, at string) error {
	_, err := tx.Exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
		VALUES(?,?,?,?,?)`, repositoryID, repo, kind, holderID, at)
	if err == nil {
		return repositoryStepTx(tx, repositoryID, repo, at)
	}
	var held string
	if scanErr := tx.QueryRow(`SELECT holder_id FROM weft_reservations WHERE repository_id=?`, repositoryID).Scan(&held); scanErr == nil {
		return &RepositoryBusyError{HeldBy: held}
	}
	return err
}

// releaseRepositoriesTx deletes the reservations where names, each with its
// repository event.
func releaseRepositoriesTx(tx *sql.Tx, stamp, where string, args ...any) error {
	rows, err := tx.Query(`DELETE FROM weft_reservations WHERE `+where+` RETURNING repository_id,repository`, args...)
	if err != nil {
		return err
	}
	var released []WeftReservation
	for rows.Next() {
		var r WeftReservation
		if err := rows.Scan(&r.RepositoryID, &r.Repository); err != nil {
			rows.Close()
			return err
		}
		released = append(released, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	slices.SortFunc(released, func(a, b WeftReservation) int { return strings.Compare(a.RepositoryID, b.RepositoryID) })
	for _, r := range released {
		if err := repositoryStepTx(tx, r.RepositoryID, r.Repository, stamp); err != nil {
			return err
		}
	}
	return nil
}

// releaseRunTx releases the reservation the run holds as a run.
func releaseRunTx(tx *sql.Tx, runID, stamp string) error {
	return releaseRepositoriesTx(tx, stamp, `holder_kind=? AND holder_id=?`, WeftHolderRun, runID)
}

// pauseRepositoryTx pauses dispatch to the run's repository for its
// unresolved landing (§7.5), with the repository event.
func pauseRepositoryTx(tx *sql.Tx, run *WeftRun, stamp string) error {
	if err := touchRepositoryTx(tx, run.RepositoryID, run.Repo); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE weft_repositories SET dispatch=?,paused_run=?,paused_reason=?,paused_detail=?,paused_at=?
		WHERE repository_id=?`, WeftDispatchPaused, run.ID, WeftPauseNotLanded,
		firstNonempty(run.SettlementDetail.String, fmt.Sprintf("run %s did not land", run.ID)), stamp, run.RepositoryID); err != nil {
		return err
	}
	r, err := weftRepository(tx, run.RepositoryID)
	if err != nil {
		return err
	}
	return appendWeftEventTx(tx, "", "repository", stamp, func(seq int64) any {
		return repositoryEventPayload{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp, Repository: r.Resource()}
	})
}

// Repository is one repository as this transaction sees it.
func (t *WeftTx) Repository(id string) (*WeftRepository, error) { return weftRepository(t.tx, id) }

// ContinueRepository lifts a landing pause (§7.5): only an operator's
// continue does, and not while a settlement there is reconciling.
func (t *WeftTx) ContinueRepository(id string, revision int64) (*WeftRepository, error) {
	r, err := weftRepository(t.tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, refuse("repository_not_found", "%s is not a repository on this host", id)
	}
	if err != nil {
		return nil, err
	}
	if r.Dispatch != WeftDispatchPaused {
		return nil, refuse("repository_not_paused", "dispatch to %s (%s) is not paused", r.Repository, id)
	}
	if r.Revision != revision {
		return nil, refuse("revision_conflict", "repository %s is at revision %d and the request was for %d", id, r.Revision, revision)
	}
	var reconciling string
	err = t.tx.QueryRow(`SELECT id FROM weft_runs WHERE repository_id=? AND settlement_state=? ORDER BY created_at LIMIT 1`,
		id, WeftSettlementReconciling).Scan(&reconciling)
	switch {
	case err == nil:
		return nil, refuse("settlement_unsettled", "run %s is reconciling its landing in %s; continue once it settles", reconciling, r.Repository)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	stamp := t.Stamp()
	if _, err := t.tx.Exec(`UPDATE weft_repositories SET dispatch=?,paused_run=NULL,paused_reason=NULL,paused_detail=NULL,
		paused_at=NULL WHERE repository_id=?`, WeftDispatchOpen, id); err != nil {
		return nil, err
	}
	if err := repositoryStepTx(t.tx, id, r.Repository, stamp); err != nil {
		return nil, err
	}
	t.Changes = append(t.Changes, WeftChange{Action: "continued", RunID: r.PausedRun.String, Detail: r.Repository})
	return weftRepository(t.tx, id)
}
