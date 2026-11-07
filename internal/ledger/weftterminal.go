package ledger

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Terminal session states and who closed one (weft-service/v2 §9.4).
const (
	WeftTerminalStarting    = "starting"
	WeftTerminalLive        = "live"
	WeftTerminalDetached    = "detached"
	WeftTerminalClosed      = "closed"
	WeftTerminalInterrupted = "interrupted"

	WeftClosedByOperator = "operator"
	WeftClosedByExit     = "exit"
	WeftClosedByRestart  = "restart"

	WeftTargetProject  = "project"
	WeftTargetWorktree = "worktree"
	WeftTargetFolder   = "folder"
)

// weftTerminalOpenSQL is a session with a process the host owns.
const weftTerminalOpenSQL = `state IN ('starting','live','detached')`

// weftTerminalShareSQL is a session holding a share of its repository's
// terminal reservation: an open one, and one closed or interrupted whose
// process group could not be established gone (§6.2, §9.4).
const weftTerminalShareSQL = `(` + weftTerminalOpenSQL + ` OR group_unresolved IS NOT NULL)`

// WeftTerminal is one terminal session as the host records it.
type WeftTerminal struct {
	ID, Kind, TargetKind, TargetID, Path string
	RepositoryID, Project                sql.NullString
	State                                string
	PID                                  sql.NullInt64
	ProcessStart                         sql.NullString
	ExitCode                             sql.NullInt64
	ClosedBy, Owner                      sql.NullString
	Attachments, Cols, Rows              int
	Offset, RetainedFrom                 int64
	Title                                string
	GroupUnresolved                      sql.NullString
	CreatedAt, UpdatedAt                 string
	Revision                             int64
}

// Open reports whether the session's process is the host's: it has not
// been closed or interrupted.
func (t *WeftTerminal) Open() bool {
	return t.State == WeftTerminalStarting || t.State == WeftTerminalLive || t.State == WeftTerminalDetached
}

// Resource is the session as every response and terminal event carries it
// (terminal-response.json).
func (t *WeftTerminal) Resource() map[string]any {
	return map[string]any{
		"session_id": t.ID, "kind": t.Kind,
		"target": map[string]any{"kind": t.TargetKind, "id": t.TargetID},
		"path":   t.Path, "repository_id": nullable(t.RepositoryID), "project": nullable(t.Project),
		"state": t.State, "owner": nullable(t.Owner), "attachments": t.Attachments,
		"cols": t.Cols, "rows": t.Rows, "offset": t.Offset, "retained_from": t.RetainedFrom,
		"title": t.Title, "exit_code": nullableInt(t.ExitCode), "closed_by": nullable(t.ClosedBy),
		"created_at": t.CreatedAt, "updated_at": t.UpdatedAt, "revision": t.Revision,
	}
}

const weftTerminalColumns = `t.id,t.kind,t.target_kind,t.target_id,t.path,t.repository_id,t.project,t.state,t.pid,
	t.process_start,t.exit_code,t.closed_by,t.owner,t.attachments,t.width,t.height,t.output_offset,
	t.retained_from,t.title,t.group_unresolved,t.created_at,t.updated_at,t.revision`

func scanWeftTerminal(sc scanner) (*WeftTerminal, error) {
	var t WeftTerminal
	err := sc.Scan(&t.ID, &t.Kind, &t.TargetKind, &t.TargetID, &t.Path, &t.RepositoryID, &t.Project, &t.State, &t.PID, &t.ProcessStart, &t.ExitCode, &t.ClosedBy, &t.Owner, &t.Attachments,
		&t.Cols, &t.Rows, &t.Offset, &t.RetainedFrom, &t.Title, &t.GroupUnresolved, &t.CreatedAt,
		&t.UpdatedAt, &t.Revision)
	return &t, err
}

func weftTerminal(q querier, id string) (*WeftTerminal, error) {
	return scanWeftTerminal(q.QueryRow(`SELECT `+weftTerminalColumns+` FROM weft_terminals t WHERE t.id=?`, id))
}

func loadWeftTerminals(q querier, where string, args ...any) ([]*WeftTerminal, error) {
	rows, err := q.Query(`SELECT `+weftTerminalColumns+` FROM weft_terminals t WHERE `+where+
		` ORDER BY t.created_at DESC,t.id DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*WeftTerminal
	for rows.Next() {
		t, err := scanWeftTerminal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// WeftTerminal is one session, open or not; one never recorded is
// sql.ErrNoRows.
func (s *Store) WeftTerminal(id string) (*WeftTerminal, error) { return weftTerminal(s.db, id) }

// WeftTerminals lists every open session and those closed or interrupted
// since closedSince, newest first (terminals.json).
func (s *Store) WeftTerminals(closedSince time.Time) ([]*WeftTerminal, error) {
	return loadWeftTerminals(s.db, weftTerminalOpenSQL+` OR t.updated_at>=?`, weftTimestamp(closedSince))
}

type terminalEventPayload struct {
	ContractVersion string         `json:"contract_version"`
	Seq             int64          `json:"seq"`
	TS              string         `json:"ts"`
	Terminal        map[string]any `json:"terminal"`
}

func appendTerminalEventTx(tx *sql.Tx, id, stamp string) error {
	t, err := weftTerminal(tx, id)
	if err != nil {
		return err
	}
	return appendWeftEventTx(tx, "", "terminal", stamp, func(seq int64) any {
		return terminalEventPayload{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp, Terminal: t.Resource()}
	})
}

// WeftTerminalOpen is a session to create, its target resolved by the host:
// the canonical path and, where the path lies in a registered repository,
// that repository and its project. RunID is a worktree target's run.
type WeftTerminalOpen struct {
	Kind, TargetKind, TargetID, Path  string
	RepositoryID, Repository, Project string
	RunID                             string
	Cols, Rows                        int
	Title                             string
}

// OpenTerminal records a session in state starting (§9.3), before any child
// exists: a worktree target's run must have no process and no unsettled
// landing and still have its worktree, and a session in a repository takes
// its terminal reservation — sharing the row other sessions there hold —
// or is refused repository_reserved while a run, settlement or maintenance
// holds it. A session in no registered repository reserves nothing. A restart
// would end any session, so none opens under the restart barrier: the refusal
// is repository_reserved because clients decode strictly and that is the code
// this route already answers with for work that holds it off.
func (t *WeftTx) OpenTerminal(o WeftTerminalOpen) (*WeftTerminal, error) {
	held, err := t.store.restartBarrier(t.tx)
	if err != nil {
		return nil, err
	}
	if held {
		return nil, refuse("repository_reserved", "shuttle is restarting for an update, and a terminal opens once the update finishes")
	}
	if o.RunID != "" {
		if err := t.terminalRun(o.RunID); err != nil {
			return nil, err
		}
	}
	now := t.store.now()
	stamp := weftTimestamp(now)
	id, err := newRemoteID("t", now)
	if err != nil {
		return nil, err
	}
	if _, err := t.tx.Exec(`INSERT INTO weft_terminals(id,kind,target_kind,target_id,path,repository_id,project,state,
		width,height,title,attachments,created_at,updated_at,revision) VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?,?,1)`,
		id, o.Kind, o.TargetKind, o.TargetID, o.Path, nullString(o.RepositoryID), nullString(o.Project),
		WeftTerminalStarting, o.Cols, o.Rows, o.Title, stamp, stamp); err != nil {
		return nil, err
	}
	if o.RepositoryID != "" {
		var kind, holder, repo string
		err := t.tx.QueryRow(`SELECT holder_kind,holder_id,repository FROM weft_reservations WHERE repository_id=?`,
			o.RepositoryID).Scan(&kind, &holder, &repo)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if err := acquireRepository(t.tx, o.RepositoryID, o.Repository, WeftHolderTerminal, id, stamp); err != nil {
				return nil, err
			}
		case err != nil:
			return nil, err
		case kind == WeftHolderTerminal:
			if err := repositoryStepTx(t.tx, o.RepositoryID, repo, stamp); err != nil {
				return nil, err
			}
		default:
			return nil, t.reservedFor(kind, holder, repo)
		}
	}
	if err := appendTerminalEventTx(t.tx, id, stamp); err != nil {
		return nil, err
	}
	return weftTerminal(t.tx, id)
}

// terminalRun refuses a worktree target whose run still has a process or a
// landing in motion, or whose worktree is gone (§9.3).
func (t *WeftTx) terminalRun(runID string) error {
	run, err := getWeftRunTx(t.tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return refuse("run_not_found", "%s is not a run on this host", runID)
	}
	if err != nil {
		return err
	}
	if run.Origin == WeftOriginArchive {
		return refuse("worktree_missing", "run %s is legacy history imported from archive %s and has no hosted worktree; open a folder terminal on its worktree where it still exists",
			runID, run.ArchiveID.String)
	}
	if run.Disposition != WeftDispositionRetained {
		return refuse("worktree_missing", "run %s's worktree %s is %s", runID, run.Worktree, run.Disposition)
	}
	active, err := weftActiveTx(t.tx, run)
	if err != nil {
		return err
	}
	if active {
		return refuse("repository_reserved", "%s is held by run %s, which is %s; a writable terminal on it opens once that run has settled",
			run.Repo, runID, WeftV2Phase(run.Phase))
	}
	if unsettled(run) {
		return refuse("repository_reserved", "%s is held by run %s, whose landing is %s; a writable terminal on it opens once that run has settled",
			run.Repo, runID, run.SettlementState)
	}
	return nil
}

// reservedFor is repository_reserved naming the holder a terminal waits on
// (error-repository-reserved.json).
func (t *WeftTx) reservedFor(kind, holder, repo string) error {
	switch kind {
	case WeftHolderRun:
		phase := "running"
		if err := t.tx.QueryRow(`SELECT phase FROM weft_runs WHERE id=?`, holder).Scan(&phase); err != nil &&
			!errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return refuse("repository_reserved", "%s is held by run %s, which is %s; a writable terminal on it opens once that run has settled",
			repo, holder, WeftV2Phase(phase))
	case WeftHolderSettlement:
		return refuse("repository_reserved", "%s is held by the settlement of run %s, which is reconciling its landing; a writable terminal on it opens once that landing has settled",
			repo, holder)
	}
	return refuse("repository_reserved", "%s is held by %s %s; a writable terminal on it opens once that work has been reaped",
		repo, kind, holder)
}

// WeftTerminalState is what a live session holds that its resource carries.
type WeftTerminalState struct {
	State                string
	Owner                string
	Attachments          int
	Cols, Rows           int
	Offset, RetainedFrom int64
	Title                string
}

// errTerminalOver is a change to a session that is no longer open.
func errTerminalOver(t *WeftTerminal) error {
	return fmt.Errorf("terminal %s is %s", t.ID, t.State)
}

// UpdateWeftTerminal records a live session's state, owner and attachments
// as they changed, with its terminal event (§10).
func (s *Store) UpdateWeftTerminal(id string, v WeftTerminalState) (*WeftTerminal, error) {
	return s.changeTerminal(id, v, func(tx *sql.Tx, stamp string) error { return nil })
}

// RecordWeftTerminalProcess records the shell a session spawned: its pid,
// which leads its process group, and the kernel's start time for it.
func (s *Store) RecordWeftTerminalProcess(id string, pid int, start string, v WeftTerminalState) (*WeftTerminal, error) {
	return s.changeTerminal(id, v, func(tx *sql.Tx, stamp string) error {
		_, err := tx.Exec(`UPDATE weft_terminals SET pid=?,pgid=?,process_start=? WHERE id=?`, pid, pid, nullString(start), id)
		return err
	})
}

func (s *Store) changeTerminal(id string, v WeftTerminalState, also func(*sql.Tx, string) error) (*WeftTerminal, error) {
	var out *WeftTerminal
	err := s.withTx(func(tx *sql.Tx) error {
		t, err := weftTerminal(tx, id)
		if err != nil {
			return err
		}
		if !t.Open() {
			return errTerminalOver(t)
		}
		stamp := weftTimestamp(s.now())
		if err := also(tx, stamp); err != nil {
			return err
		}
		if err := updateTerminalTx(tx, id, v, stamp); err != nil {
			return err
		}
		if err := appendTerminalEventTx(tx, id, stamp); err != nil {
			return err
		}
		out, err = weftTerminal(tx, id)
		return err
	})
	return out, err
}

func updateTerminalTx(tx *sql.Tx, id string, v WeftTerminalState, stamp string) error {
	_, err := tx.Exec(`UPDATE weft_terminals SET state=?,owner=?,attachments=?,width=?,height=?,output_offset=?,
		retained_from=?,title=?,updated_at=?,revision=revision+1 WHERE id=?`,
		v.State, nullString(v.Owner), v.Attachments, v.Cols, v.Rows, v.Offset, v.RetainedFrom, v.Title, stamp, id)
	return err
}

// CloseWeftTerminal records a session closed — by the operator or by its
// shell's exit — once the host has reaped its process group, and releases
// its share of the repository's terminal reservation: the row goes with the
// last share, and otherwise names a session still holding it. unresolved says
// why a process group could not be established gone; the share is then kept
// until a restart establishes it. pid is a shell the session spawned and
// never recorded, 0 for none: recorded here so that restart has it to check.
func (s *Store) CloseWeftTerminal(id, closedBy string, exitCode *int, v WeftTerminalState, unresolved string, pid int) (*WeftTerminal, error) {
	var out *WeftTerminal
	err := s.withTx(func(tx *sql.Tx) error {
		t, err := weftTerminal(tx, id)
		if err != nil {
			return err
		}
		if !t.Open() {
			return errTerminalOver(t)
		}
		stamp := weftTimestamp(s.now())
		v.State, v.Owner, v.Attachments = WeftTerminalClosed, "", 0
		if err := updateTerminalTx(tx, id, v, stamp); err != nil {
			return err
		}
		recorded := sql.NullInt64{Int64: int64(pid), Valid: pid > 0}
		if _, err := tx.Exec(`UPDATE weft_terminals SET closed_by=?,exit_code=?,group_unresolved=?,
			pid=COALESCE(pid,?),pgid=COALESCE(pgid,?) WHERE id=?`,
			closedBy, nullInt64(exitCode), nullString(unresolved), recorded, recorded, id); err != nil {
			return err
		}
		if err := appendTerminalEventTx(tx, id, stamp); err != nil {
			return err
		}
		if unresolved == "" {
			if err := releaseTerminalShareTx(tx, t, stamp); err != nil {
				return err
			}
		}
		out, err = weftTerminal(tx, id)
		return err
	})
	return out, err
}

func nullInt64(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}

// releaseTerminalShareTx gives up the session's share of its repository's
// terminal reservation, with the repository event: the row is deleted with
// the last share; otherwise, if it named this session, it names the oldest
// session still sharing it.
func releaseTerminalShareTx(tx *sql.Tx, t *WeftTerminal, stamp string) error {
	if !t.RepositoryID.Valid {
		return nil
	}
	var kind, holder, repo string
	err := tx.QueryRow(`SELECT holder_kind,holder_id,repository FROM weft_reservations WHERE repository_id=?`,
		t.RepositoryID.String).Scan(&kind, &holder, &repo)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && kind != WeftHolderTerminal) {
		return nil
	}
	if err != nil {
		return err
	}
	var next string
	err = tx.QueryRow(`SELECT id FROM weft_terminals WHERE repository_id=? AND id<>? AND `+weftTerminalShareSQL+`
		ORDER BY created_at,id LIMIT 1`, t.RepositoryID.String, t.ID).Scan(&next)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return releaseRepositoriesTx(tx, stamp, `repository_id=? AND holder_kind=?`, t.RepositoryID.String, WeftHolderTerminal)
	case err != nil:
		return err
	}
	if holder == t.ID {
		if _, err := tx.Exec(`UPDATE weft_reservations SET holder_id=? WHERE repository_id=?`, next, t.RepositoryID.String); err != nil {
			return err
		}
	}
	return repositoryStepTx(tx, t.RepositoryID.String, repo, stamp)
}

// WeftTerminalsToInterrupt is every session §12 step 3 has to establish
// gone: each one not closed, and each whose process group could not be
// established gone before.
func (s *Store) WeftTerminalsToInterrupt() ([]*WeftTerminal, error) {
	return loadWeftTerminals(s.db, weftTerminalShareSQL)
}

// InterruptWeftTerminal is §12 step 3 for one session: it is interrupted,
// closed_by restart, and — once its process group is established gone —
// releases its reservation share. unresolved says why the group could not
// be established gone; the share is then kept, as v1's restart pass keeps an
// unestablishable segment's reservation, and the next restart checks again.
func (s *Store) InterruptWeftTerminal(id, unresolved string) (*WeftTerminal, error) {
	var out *WeftTerminal
	err := s.withTx(func(tx *sql.Tx) error {
		t, err := weftTerminal(tx, id)
		if err != nil {
			return err
		}
		stamp := weftTimestamp(s.now())
		if t.Open() {
			if _, err := tx.Exec(`UPDATE weft_terminals SET state=?,closed_by=?,owner=NULL,attachments=0,updated_at=?,
				revision=revision+1 WHERE id=?`, WeftTerminalInterrupted, WeftClosedByRestart, stamp, id); err != nil {
				return err
			}
			if err := appendTerminalEventTx(tx, id, stamp); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE weft_terminals SET group_unresolved=? WHERE id=?`, nullString(unresolved), id); err != nil {
			return err
		}
		if unresolved == "" {
			if err := releaseTerminalShareTx(tx, t, stamp); err != nil {
				return err
			}
		}
		out, err = weftTerminal(tx, id)
		return err
	})
	return out, err
}

// Terminal is one session as this transaction sees it.
func (t *WeftTx) Terminal(id string) (*WeftTerminal, error) { return weftTerminal(t.tx, id) }
