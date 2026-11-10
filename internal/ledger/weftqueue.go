package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"shuttle/internal/sandbox"
)

// The admission queue's vocabulary (weft-service/v2 §6.3).
const (
	WeftEntryLaunch    = "launch"
	WeftEntryFrontier  = "frontier"
	WeftEntryAnswer    = "answer"
	WeftEntryResume    = "resume"
	WeftEntryFresh     = "fresh"
	WeftEntryReconcile = "reconcile"
	WeftEntryCleanup   = "cleanup"

	WeftEntryWaiting     = "waiting"
	WeftEntryDispatched  = "dispatched"
	WeftEntryCancelled   = "cancelled"
	WeftEntryInvalidated = "invalidated"
	WeftEntryArchiving   = "archiving"

	WeftWaitHostPaused       = "host_paused"
	WeftWaitRepositoryPaused = "repository_paused"
	WeftWaitReserved         = "repository_reserved"
	WeftWaitCapacity         = "capacity"

	WeftInvalidTicket  = "ticket_ineligible"
	WeftInvalidBinding = "binding_changed"
	WeftInvalidRun     = "run_changed"
	WeftInvalidArchive = "archive_refused"
	WeftInvalidCleanup = "cleanup_refused"

	// WeftRecordedV2 is the protocol a run admitted through the queue is
	// recorded under, whatever worker contract its segments launch with.
	WeftRecordedV2 = "weft-service/v2"
	// WeftLandingNotServed is §2.4's settlement detail for a run on a host
	// that does not serve landing: its branch is left for the human.
	WeftLandingNotServed = "landing is not served by this host"
	// WeftActionStaleBase is the action a run is held for when its base went
	// stale under it (§6.7).
	WeftActionStaleBase = "stale_base"

	weftQueueRevisionKey = "queue_revision"
)

// weftRunningSQL counts the slots in use (§6.1): every dispatched pipeline
// segment the host has not yet reaped — its phase-1 row still claimed or
// running, so a parked run, which has no process, holds none — and every
// archiving entry. Reconcile, cleanup and archive workers launch no model and
// hold no slot of their own.
const weftRunningSQL = `SELECT (SELECT count(*) FROM weft_segments s JOIN runs r ON r.id=s.ledger_run_id
	WHERE s.kind IN ` + weftPipelineKinds + ` AND r.status IN ('` + StatusClaimed + `','` + StatusRunning + `'))
	+ (SELECT count(*) FROM weft_entries WHERE state='` + WeftEntryArchiving + `')`

// WeftRefusal is a command the queue refuses with one of weft-service/v2
// §4.2's codes; Message names the ids involved.
type WeftRefusal struct{ Code, Message string }

func (e *WeftRefusal) Error() string { return e.Code + ": " + e.Message }

func refuse(code, format string, args ...any) error {
	return &WeftRefusal{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WeftEntry is one row of the admission queue. WaitingDetail is derived when
// the entry is read, from the state its waiting reason names as it is then.
// Dismissed takes it out of the listing and out of no other read (§6.3).
type WeftEntry struct {
	ID, Kind, Ticket, Project, RepositoryID, Repository string
	SelectionID                                         sql.NullString
	Position                                            int64
	State                                               string
	WaitingReason, HolderKind, HolderID                 sql.NullString
	WaitingDetail                                       string
	InvalidatedReason, InvalidatedDetail                sql.NullString
	RunID, Supersedes, AnswerID                         sql.NullString
	CreatedAt, UpdatedAt                                string
	Revision                                            int64
	Dismissed                                           bool
}

// Resource is the entry as every response and queue event carries it.
func (e *WeftEntry) Resource() map[string]any {
	var waiting, invalidated any
	if e.State == WeftEntryWaiting {
		var holder any
		if e.HolderKind.Valid {
			holder = map[string]any{"kind": e.HolderKind.String, "id": e.HolderID.String}
		}
		waiting = map[string]any{"reason": e.WaitingReason.String, "holder": holder, "detail": e.WaitingDetail}
	}
	if e.State == WeftEntryInvalidated {
		invalidated = map[string]any{"reason": e.InvalidatedReason.String, "detail": e.InvalidatedDetail.String}
	}
	return map[string]any{
		"entry_id": e.ID, "kind": e.Kind, "ticket": e.Ticket, "project": e.Project,
		"repository_id": e.RepositoryID, "selection_id": nullable(e.SelectionID), "position": e.Position,
		"state": e.State, "waiting": waiting, "invalidated": invalidated, "run_id": nullable(e.RunID),
		"supersedes": nullable(e.Supersedes), "answer_id": nullable(e.AnswerID),
		"created_at": e.CreatedAt, "updated_at": e.UpdatedAt, "revision": e.Revision,
	}
}

const weftEntryColumns = `id,kind,ticket,project,repository_id,repository,selection_id,position,state,
	waiting_reason,holder_kind,holder_id,invalidated_reason,invalidated_detail,run_id,supersedes,answer_id,
	created_at,updated_at,revision,dismissed`

func scanWeftEntry(sc scanner) (*WeftEntry, error) {
	var e WeftEntry
	err := sc.Scan(&e.ID, &e.Kind, &e.Ticket, &e.Project, &e.RepositoryID, &e.Repository, &e.SelectionID,
		&e.Position, &e.State, &e.WaitingReason, &e.HolderKind, &e.HolderID, &e.InvalidatedReason,
		&e.InvalidatedDetail, &e.RunID, &e.Supersedes, &e.AnswerID, &e.CreatedAt, &e.UpdatedAt, &e.Revision, &e.Dismissed)
	return &e, err
}

// WeftCapacity is the cap with the running and waiting counts (§6.1).
type WeftCapacity struct{ Cap, Running, Waiting int }

func (c WeftCapacity) Resource() map[string]any {
	return map[string]any{"cap": c.Cap, "running": c.Running, "waiting": c.Waiting}
}

func weftCapacity(q querier) (WeftCapacity, error) {
	var c WeftCapacity
	var capacity, users sql.NullString
	err := q.QueryRow(`SELECT (SELECT value FROM weft_settings WHERE key=?), (SELECT value FROM weft_settings WHERE key=?),
		(`+weftRunningSQL+`), (SELECT count(*) FROM weft_entries WHERE state=?)`,
		weftCapacityKey, weftAgentUsersKey, WeftEntryWaiting).
		Scan(&capacity, &users, &c.Running, &c.Waiting)
	if err != nil {
		return c, err
	}
	c.Cap, err = weftCap(capacity, users)
	return c, err
}

// weftCap is the cap as stored, else WeftCapacityDefault bounded by the
// host's agent users once they are recorded. A stored cap above that count is
// refused at service start (ConfigureWeftAgentUsers) and by the capacity
// route, not bounded here.
func weftCap(capacity, users sql.NullString) (int, error) {
	if capacity.Valid {
		n, err := strconv.Atoi(capacity.String)
		if err != nil {
			return 0, fmt.Errorf("weft capacity %q: %w", capacity.String, err)
		}
		return n, nil
	}
	if !users.Valid {
		return WeftCapacityDefault, nil
	}
	n, err := strconv.Atoi(users.String)
	if err != nil {
		return 0, fmt.Errorf("weft agent users %q: %w", users.String, err)
	}
	return min(WeftCapacityDefault, n), nil
}

// ConfigureWeftAgentUsers records how many agent users the host has, which
// an unset cap defaults to at most (§6.1), and refuses a stored cap above
// that: every live segment runs as its own agent user, so such a cap could
// never be met. A cap the count moves is announced by its capacity event.
func (s *Store) ConfigureWeftAgentUsers(n int) error {
	return s.withTx(func(tx *sql.Tx) error {
		var stored sql.NullString
		err := tx.QueryRow(`SELECT value FROM weft_settings WHERE key=?`, weftCapacityKey).Scan(&stored)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if stored.Valid {
			capacity, err := strconv.Atoi(stored.String)
			if err != nil {
				return fmt.Errorf("weft capacity %q: %w", stored.String, err)
			}
			if capacity > n {
				return fmt.Errorf("the weft capacity stored in the ledger is %d, more than the %d agent user(s) in weft.agent_users: every live segment runs as its own agent user. Add agent users, or lower the capacity to %d or less with PUT /v2/queue/capacity while there are enough",
					capacity, n, n)
			}
		}
		before, err := weftCapacity(tx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO weft_settings(key,value) VALUES(?,?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`, weftAgentUsersKey, strconv.Itoa(n)); err != nil {
			return err
		}
		after, err := weftCapacity(tx)
		if err != nil || after.Cap == before.Cap {
			return err
		}
		return capacityStepTx(tx, weftTimestamp(s.now()))
	})
}

// weftHeldAgentUsersSQL is every agent user a pipeline segment holds: one
// whose phase-1 row is claimed or running. A segment settles only once its
// user's processes are gone, so a user is free again only after that.
const weftHeldAgentUsersSQL = `SELECT s.agent_user FROM weft_segments s JOIN runs r ON r.id=s.ledger_run_id
	WHERE s.agent_user IS NOT NULL AND r.status IN ('` + StatusClaimed + `','` + StatusRunning + `')`

// freeAgentUser is the first of users no unsettled segment holds, or "" when
// every one is held.
func freeAgentUser(q querier, users []string) (string, error) {
	rows, err := q.Query(weftHeldAgentUsersSQL)
	if err != nil {
		return "", err
	}
	held := map[string]bool{}
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			rows.Close()
			return "", err
		}
		held[user] = true
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	for _, user := range users {
		if !held[user] {
			return user, nil
		}
	}
	return "", nil
}

// agentUser is the user the entry's pipeline segment is claimed with.
// blocking has already made the entry wait while none is free, so none
// being free here is a ledger that holds more segments than slots.
func (t *WeftTx) agentUser(e *WeftEntry, d WeftDispatch) (string, error) {
	user, err := freeAgentUser(t.tx, d.AgentUsers)
	if err == nil && user == "" {
		err = fmt.Errorf("entry %s: every one of the %d agent users is held by an unsettled segment", e.ID, len(d.AgentUsers))
	}
	return user, err
}

func queueRevision(q querier) (int64, error) {
	var revision int64
	err := q.QueryRow(`SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM weft_settings WHERE key=?),0)`,
		weftQueueRevisionKey).Scan(&revision)
	return revision, err
}

func bumpQueueRevisionTx(tx *sql.Tx) (int64, error) {
	var revision int64
	err := tx.QueryRow(`INSERT INTO weft_settings(key,value) VALUES(?,'1')
		ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)
		RETURNING CAST(value AS INTEGER)`, weftQueueRevisionKey).Scan(&revision)
	return revision, err
}

// WeftHostPausedDetail says why the host dispatches nothing: the operator's
// (or the failure streak's) pause of the weft-worker agent, or the updater's,
// which lifts itself and offers the reader nothing to do.
func WeftHostPausedDetail(reason, since string) string {
	if reason == PauseUpdate {
		return fmt.Sprintf("shuttle is updating: dispatch paused since %s, and it resumes when the update finishes", since)
	}
	return fmt.Sprintf("%s is paused (%s) since %s; shuttle resume %s lifts it", WeftAgent, reason, since, WeftAgent)
}

func weftHostPaused(q querier) (paused bool, reason, since string, err error) {
	var r, at sql.NullString
	err = q.QueryRow(`SELECT paused,paused_reason,paused_at FROM agents WHERE name=?`, WeftAgent).Scan(&paused, &r, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", "", nil
	}
	return paused, r.String, at.String, err
}

// WeftHost is the host-wide dispatch state (weft-service/v2 §5.3): dispatch,
// open or paused, and the words a client shows for a pause.
type WeftHost struct{ Dispatch, Reason string }

// Resource is the state as the dashboard's host and the host event carry it.
func (h WeftHost) Resource() map[string]any {
	var reason any
	if h.Dispatch == WeftDispatchPaused {
		reason = h.Reason
	}
	return map[string]any{"dispatch": h.Dispatch, "reason": reason}
}

// WeftHost reads the host-wide dispatch state.
func (s *Store) WeftHost() (WeftHost, error) { return weftHost(s.db) }

func weftHost(q querier) (WeftHost, error) {
	paused, reason, since, err := weftHostPaused(q)
	if err != nil || !paused {
		return WeftHost{Dispatch: WeftDispatchOpen}, err
	}
	return WeftHost{Dispatch: WeftDispatchPaused, Reason: WeftHostPausedDetail(reason, since)}, nil
}

// weftHostStepTx runs change, a write to agent's pause, and writes the host
// event (§10) in the same transaction when it moved the host-wide state or
// its reason. Every write to a pause goes through here, whichever process
// makes it: the state stream reads the ledger, so another process's event
// reaches a connected client as the daemon's own do.
func weftHostStepTx(tx *sql.Tx, agent, stamp string, change func() error) error {
	if agent != WeftAgent {
		return change()
	}
	before, err := weftHost(tx)
	if err != nil {
		return err
	}
	if err := change(); err != nil {
		return err
	}
	after, err := weftHost(tx)
	if err != nil || after == before {
		return err
	}
	return appendWeftEventTx(tx, "", "host", stamp, func(seq int64) any {
		doc := after.Resource()
		doc["contract_version"], doc["seq"], doc["ts"] = WeftServiceVersion, seq, stamp
		return doc
	})
}

// weftWaitingDetail says why a waiting entry waits.
func weftWaitingDetail(q querier, e *WeftEntry, c WeftCapacity) (string, error) {
	switch e.WaitingReason.String {
	case WeftWaitHostPaused:
		_, reason, since, err := weftHostPaused(q)
		return WeftHostPausedDetail(reason, since), err
	case WeftWaitRepositoryPaused:
		var reason, detail sql.NullString
		err := q.QueryRow(`SELECT paused_reason,paused_detail FROM weft_repositories WHERE repository_id=?`,
			e.RepositoryID).Scan(&reason, &detail)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		return fmt.Sprintf("dispatch to %s is paused (%s): %s", e.Repository, reason.String, detail.String), nil
	case WeftWaitReserved:
		switch e.HolderKind.String {
		case "run":
			return fmt.Sprintf("%s holds %s", e.HolderID.String, e.Repository), nil
		default:
			return fmt.Sprintf("%s %s holds %s", e.HolderKind.String, e.HolderID.String, e.Repository), nil
		}
	case WeftWaitCapacity:
		return fmt.Sprintf("%d of %d slots are running", c.Running, c.Cap), nil
	}
	return "", nil
}

func loadWeftEntries(q querier, where string, args ...any) ([]*WeftEntry, error) {
	rows, err := q.Query(`SELECT `+weftEntryColumns+` FROM weft_entries WHERE `+where+` ORDER BY position`, args...)
	if err != nil {
		return nil, err
	}
	var out []*WeftEntry
	for rows.Next() {
		e, err := scanWeftEntry(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var c WeftCapacity
	loaded := false
	for _, e := range out {
		if e.State != WeftEntryWaiting {
			continue
		}
		if !loaded {
			if c, err = weftCapacity(q); err != nil {
				return nil, err
			}
			loaded = true
		}
		if e.WaitingDetail, err = weftWaitingDetail(q, e, c); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func weftEntry(q querier, id string) (*WeftEntry, error) {
	entries, err := loadWeftEntries(q, `id=?`, id)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, sql.ErrNoRows
	}
	return entries[0], nil
}

// WeftQueue is GET /v2/queue's resource.
type WeftQueue struct {
	Revision int64
	Capacity WeftCapacity
	Entries  []*WeftEntry
}

func weftQueue(q querier, settledSince time.Time) (*WeftQueue, error) {
	var out WeftQueue
	var err error
	if out.Revision, err = queueRevision(q); err != nil {
		return nil, err
	}
	if out.Capacity, err = weftCapacity(q); err != nil {
		return nil, err
	}
	out.Entries, err = loadWeftEntries(q, `(state IN (?,?) OR updated_at>=?) AND dismissed=0`,
		WeftEntryWaiting, WeftEntryArchiving, weftTimestamp(settledSince))
	return &out, err
}

// WeftQueue lists the waiting and archiving entries and those settled since
// settledSince, less any dismissed, with the queue's revision and capacity.
func (s *Store) WeftQueue(settledSince time.Time) (*WeftQueue, error) {
	return weftQueue(s.db, settledSince)
}

// WeftCapacity is the cap with its running and waiting counts.
func (s *Store) WeftCapacity() (WeftCapacity, error) {
	return weftCapacity(s.db)
}

type queueEventPayload struct {
	ContractVersion string         `json:"contract_version"`
	Seq             int64          `json:"seq"`
	TS              string         `json:"ts"`
	QueueRevision   int64          `json:"queue_revision"`
	Dismissed       bool           `json:"dismissed"`
	Entry           map[string]any `json:"entry"`
}

type capacityEventPayload struct {
	ContractVersion string         `json:"contract_version"`
	Seq             int64          `json:"seq"`
	TS              string         `json:"ts"`
	QueueRevision   int64          `json:"queue_revision"`
	Capacity        map[string]any `json:"capacity"`
}

type answerEventPayload struct {
	eventBase
	QuestionID string `json:"question_id"`
	AnswerID   string `json:"answer_id"`
	EntryID    string `json:"entry_id"`
}

// appendQueueEventTx writes the entry's queue event, and sets its dismissed
// flag to the event's: a dismissal takes the entry out of the listing, and
// any later change to it — a dispatched cleanup refused — puts it back, so
// the listing never disagrees with what the stream told a client (§6.3).
func appendQueueEventTx(tx *sql.Tx, entryID string, revision int64, stamp string, dismissed bool) error {
	if _, err := tx.Exec(`UPDATE weft_entries SET dismissed=? WHERE id=?`, dismissed, entryID); err != nil {
		return err
	}
	e, err := weftEntry(tx, entryID)
	if err != nil {
		return err
	}
	return appendWeftEventTx(tx, "", "queue", stamp, func(seq int64) any {
		return queueEventPayload{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp,
			QueueRevision: revision, Dismissed: dismissed, Entry: e.Resource()}
	})
}

func appendCapacityEventTx(tx *sql.Tx, revision int64, stamp string) error {
	c, err := weftCapacity(tx)
	if err != nil {
		return err
	}
	return appendWeftEventTx(tx, "", "capacity", stamp, func(seq int64) any {
		return capacityEventPayload{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp,
			QueueRevision: revision, Capacity: c.Resource()}
	})
}

// queueStepTx writes one queue transition's events: the queue revision raised
// once, a queue event per entry named, and — when the transition changed a
// count — the capacity event that follows it in the same transaction.
func queueStepTx(tx *sql.Tx, stamp string, capacity bool, entryIDs ...string) error {
	revision, err := bumpQueueRevisionTx(tx)
	if err != nil {
		return err
	}
	for _, id := range entryIDs {
		if err := appendQueueEventTx(tx, id, revision, stamp, false); err != nil {
			return err
		}
	}
	if !capacity {
		return nil
	}
	return appendCapacityEventTx(tx, revision, stamp)
}

// capacityStepTx is the capacity event of a transition that changed only a
// count: a segment reaped, a cap set.
func capacityStepTx(tx *sql.Tx, stamp string) error {
	return queueStepTx(tx, stamp, true)
}

// WeftChange is one thing a queue transaction did, for the host's log.
type WeftChange struct {
	Action, EntryID, RunID, Kind, Ticket, Detail string
}

// WeftTx is one queue transaction. Spawn names the runs whose segments its
// dispatches claimed: the caller starts their workers once it commits, never
// before.
type WeftTx struct {
	tx       *sql.Tx
	store    *Store
	Spawn    []string
	Changes  []WeftChange
	admitted map[string]bool
}

// WeftTx runs fn as one ledger transaction.
func (s *Store) WeftTx(fn func(*WeftTx) error) error {
	return s.withTx(func(tx *sql.Tx) error {
		return fn(&WeftTx{tx: tx, store: s, admitted: map[string]bool{}})
	})
}

// Refusable runs fn under a savepoint. A *WeftRefusal it returns undoes what
// fn wrote and is handed back, the transaction still usable for recording
// the refusal.
func (t *WeftTx) Refusable(fn func() error) error {
	if _, err := t.tx.Exec(`SAVEPOINT weft_command`); err != nil {
		return err
	}
	spawned, changed := len(t.Spawn), len(t.Changes)
	err := fn()
	var refusal *WeftRefusal
	if errors.As(err, &refusal) {
		if _, rollback := t.tx.Exec(`ROLLBACK TO weft_command`); rollback != nil {
			return rollback
		}
		t.Spawn, t.Changes = t.Spawn[:spawned], t.Changes[:changed]
	}
	if _, release := t.tx.Exec(`RELEASE weft_command`); release != nil {
		return release
	}
	return err
}

func (t *WeftTx) Request(tokenID, key string) (*WeftRequest, error) {
	var r WeftRequest
	err := t.tx.QueryRow(`SELECT token_id,key,method,path,body_sha256,status,response,created_at
		FROM weft_requests WHERE token_id=? AND key=?`, tokenID, key).
		Scan(&r.TokenID, &r.Key, &r.Method, &r.Path, &r.BodySHA, &r.Status, &r.Response, &r.CreatedAt)
	return &r, err
}

func (t *WeftTx) PutRequest(r WeftRequest) error {
	_, err := t.tx.Exec(`INSERT INTO weft_requests(token_id,key,method,path,body_sha256,status,response,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, r.TokenID, r.Key, r.Method, r.Path, r.BodySHA, r.Status, r.Response, r.CreatedAt)
	return err
}

func (t *WeftTx) AsOf() (int64, error) {
	var seq int64
	err := t.tx.QueryRow(`SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='weft_events'),0)`).Scan(&seq)
	return seq, err
}

func (t *WeftTx) Stamp() string { return weftTimestamp(t.store.now()) }

func (t *WeftTx) GetWeftRun(id string) (*WeftRun, error) { return getWeftRunFull(t.tx, id) }

func (t *WeftTx) WeftAnswer(id string) (*WeftAnswer, error) { return weftAnswer(t.tx, id) }

func (t *WeftTx) WeftQuestion(id string) (*WeftQuestion, error) { return weftQuestion(t.tx, id) }

func (t *WeftTx) LatestWeftQuestion(runID string) (*WeftQuestion, error) {
	return latestWeftQuestion(t.tx, runID)
}

func (t *WeftTx) Entry(id string) (*WeftEntry, error) { return weftEntry(t.tx, id) }

func (t *WeftTx) Queue(settledSince time.Time) (*WeftQueue, error) {
	return weftQueue(t.tx, settledSince)
}

func (t *WeftTx) change(action string, e *WeftEntry, runID, detail string) {
	t.Changes = append(t.Changes, WeftChange{
		Action: action, EntryID: e.ID, RunID: runID, Kind: e.Kind, Ticket: e.Ticket, Detail: detail,
	})
}

// WeftAdmission is what admits one entry: the ticket with the binding it was
// resolved to, the run an answer or resume continues, and the request whose
// key admitted it.
type WeftAdmission struct {
	Kind, Ticket, Project, RepositoryID, Repository string
	SelectionID, RunID, AnswerID, Supersedes        string
	TokenID, RequestKey                             string
}

// Admit applies the per-ticket guard and inserts a launch or frontier entry
// at the next position. It is left waiting with no reason: Dispatch, in the
// same transaction, either dispatches it or says what it waits on.
func (t *WeftTx) Admit(a WeftAdmission) (*WeftEntry, error) {
	if err := t.guard(a.Ticket, ""); err != nil {
		return nil, err
	}
	var prior, branch string
	err := t.tx.QueryRow(`SELECT id,branch FROM weft_runs WHERE ticket=? AND origin=? AND disposition<>?
		ORDER BY created_at DESC,id DESC LIMIT 1`, a.Ticket, WeftOriginHosted, WeftDispositionArchived).Scan(&prior, &branch)
	switch {
	case err == nil:
		return nil, refuse("prior_run", "%s has run %s, which retains branch %s; start that run fresh rather than launching over it",
			a.Ticket, prior, branch)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	return t.insertEntry(a)
}

// AdmitFrontier admits a confirmed frontier selection (§6.4): one frontier
// entry per ticket under one selection_id. A ticket the guard refuses means
// the frontier moved since the preview, and the whole selection is refused.
func (t *WeftTx) AdmitFrontier(project string, items []WeftAdmission) (string, []*WeftEntry, error) {
	selection, err := newRemoteID("fs", t.store.now())
	if err != nil {
		return "", nil, err
	}
	var entries []*WeftEntry
	for _, a := range items {
		a.Kind, a.SelectionID = WeftEntryFrontier, selection
		e, err := t.Admit(a)
		var refusal *WeftRefusal
		if errors.As(err, &refusal) {
			return "", nil, refuse("revision_conflict", "the frontier of %s changed since the preview: %s. Preview again and confirm what is shown",
				project, refusal.Message)
		}
		if err != nil {
			return "", nil, err
		}
		entries = append(entries, e)
	}
	return selection, entries, nil
}

// guard is §6.2's per-ticket guard: no other run of the ticket starting,
// running, parked or settling, and no entry of it waiting or archiving.
func (t *WeftTx) guard(ticket, runID string) error {
	var other, phase string
	err := t.tx.QueryRow(`SELECT id,phase FROM weft_runs WHERE ticket=? AND id<>? AND phase IN (?,?,?,?)
		ORDER BY created_at LIMIT 1`, ticket, runID, WeftPhaseQueued, WeftPhaseRunning, WeftPhaseParked, WeftPhaseSettling).Scan(&other, &phase)
	switch {
	case err == nil:
		return refuse("ticket_in_flight", "%s has run %s, which is %s", ticket, other, WeftV2Phase(phase))
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	var entry, kind, state string
	err = t.tx.QueryRow(`SELECT id,kind,state FROM weft_entries WHERE ticket=? AND state IN (?,?)
		AND kind IN (?,?,?,?,?) ORDER BY position LIMIT 1`, ticket, WeftEntryWaiting, WeftEntryArchiving,
		WeftEntryLaunch, WeftEntryFrontier, WeftEntryAnswer, WeftEntryResume, WeftEntryFresh).Scan(&entry, &kind, &state)
	switch {
	case err == nil:
		return refuse("ticket_in_flight", "%s has %s entry %s %s in the queue", ticket, kind, entry, state)
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	return nil
}

// EditGuard is §5.4's guard on a ticket edit: §6.2's with no run excepted,
// reaching every run whose settlement is reconciling and every waiting or
// archiving entry but a cleanup, which writes no ticket status. Each of those
// may still write the ticket's status, so a hand edit never races it.
func (t *WeftTx) EditGuard(ticket string) error {
	var run, phase, settlement string
	err := t.tx.QueryRow(`SELECT id,phase,settlement_state FROM weft_runs WHERE ticket=? AND (phase IN (?,?,?,?) OR settlement_state=?)
		ORDER BY created_at LIMIT 1`, ticket, WeftPhaseQueued, WeftPhaseRunning, WeftPhaseParked, WeftPhaseSettling,
		WeftSettlementReconciling).Scan(&run, &phase, &settlement)
	switch {
	case err == nil && settlement == WeftSettlementReconciling:
		return refuse("ticket_in_flight", "%s has run %s reconciling its landing; edit the ticket once that run is over", ticket, run)
	case err == nil:
		return refuse("ticket_in_flight", "%s has run %s in phase %s; edit the ticket once that run is over", ticket, run, WeftV2Phase(phase))
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	var entry, kind, state string
	err = t.tx.QueryRow(`SELECT id,kind,state FROM weft_entries WHERE ticket=? AND state IN (?,?) AND kind<>?
		ORDER BY position LIMIT 1`, ticket, WeftEntryWaiting, WeftEntryArchiving, WeftEntryCleanup).Scan(&entry, &kind, &state)
	switch {
	case err == nil:
		return refuse("ticket_in_flight", "%s has %s entry %s %s in the queue; edit the ticket once that entry has left it", ticket, kind, entry, state)
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	return nil
}

func (t *WeftTx) insertEntry(a WeftAdmission) (*WeftEntry, error) {
	return t.insertEntryNoting(a, "")
}

// insertEntryNoting is insertEntry with detail on its admission's log line.
func (t *WeftTx) insertEntryNoting(a WeftAdmission, detail string) (*WeftEntry, error) {
	id, err := insertEntryTx(t.tx, a, WeftEntryWaiting, t.store.now())
	if err != nil {
		return nil, err
	}
	t.admitted[id] = true
	e, err := t.Entry(id)
	if err != nil {
		return nil, err
	}
	t.change("admitted", e, a.RunID, detail)
	return e, nil
}

// RecordAnswer is §6.5's one transaction: the question moved pending →
// answered, the answer recorded once against it, and an answer entry carrying
// it admitted. Dispatch, in the same transaction, then tries the entry. A
// structured answer is answers, already validated against its question's
// form and options; a legacy one is text.
func (t *WeftTx) RecordAnswer(runID, questionID, text string, answers json.RawMessage, a WeftAdmission) (*WeftQuestion, *WeftEntry, error) {
	run, err := getWeftRunTx(t.tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, refuse("run_not_found", "%s is not a run on this host", runID)
	}
	if err != nil {
		return nil, nil, err
	}
	if run.Phase == WeftPhaseEnded {
		return nil, nil, refuse("run_over", "run %s is over (%s) and takes no answer", runID, run.Outcome.String)
	}
	if run.ActionRequired.String == WeftActionStaleBase {
		return nil, nil, staleBase(run)
	}
	if err := t.refuseUnderCleanup(runID); err != nil {
		return nil, nil, err
	}
	question, err := weftQuestion(t.tx, questionID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, refuse("question_not_found", "%s is not a question on this host", questionID)
	}
	if err != nil {
		return nil, nil, err
	}
	if question.RunID != runID || question.State != "pending" {
		return nil, nil, notPending(question, runID)
	}
	if err := t.guard(run.Ticket, runID); err != nil {
		return nil, nil, err
	}
	segment, err := nextSegmentTx(t.tx, runID)
	if err != nil {
		return nil, nil, err
	}
	if segment > WeftSegmentLimit {
		return nil, nil, refuse("segment_limit", "run %s has claimed all %d segments", runID, WeftSegmentLimit)
	}
	now := t.store.now()
	stamp := weftTimestamp(now)
	answerID, err := newRemoteID("a", now)
	if err != nil {
		return nil, nil, err
	}
	res, err := t.tx.Exec(`UPDATE weft_questions SET state='answered',answer_id=?
		WHERE id=? AND run_id=? AND state='pending'`, answerID, questionID, runID)
	if err != nil {
		return nil, nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, nil, notPending(question, runID)
	}
	if _, err := t.tx.Exec(`INSERT INTO weft_answers(id,question_id,run_id,text,answers,submitted_at,segment)
		VALUES(?,?,?,?,?,?,?)`, answerID, questionID, runID, text, nullJSON(answers), stamp, segment); err != nil {
		return nil, nil, err
	}
	a.Kind, a.Ticket, a.Project, a.RunID, a.AnswerID = WeftEntryAnswer, run.Ticket, run.Project, runID, answerID
	a.RepositoryID, a.Repository = run.RepositoryID, run.Repo
	entry, err := t.insertEntry(a)
	if err != nil {
		return nil, nil, err
	}
	if _, err := t.tx.Exec(`UPDATE weft_answers SET entry_id=? WHERE id=?`, entry.ID, answerID); err != nil {
		return nil, nil, err
	}
	if err := appendWeftEventTx(t.tx, runID, "answer", stamp, func(seq int64) any {
		return answerEventPayload{eventBase: newEventBase(seq, stamp, runID),
			QuestionID: questionID, AnswerID: answerID, EntryID: entry.ID}
	}); err != nil {
		return nil, nil, err
	}
	if question, err = weftQuestion(t.tx, questionID); err != nil {
		return nil, nil, err
	}
	if err := appendQuestionEventTx(t.tx, question, stamp); err != nil {
		return nil, nil, err
	}
	return question, entry, nil
}

// insertEntryTx inserts an entry in state at the next global position. An
// entry the host admits itself has no request behind it: a reconcile's empty
// token and key say so, and a host-started cleanup's WeftHostToken with its
// own id as key (§6.8).
func insertEntryTx(tx *sql.Tx, a WeftAdmission, state string, now time.Time) (string, error) {
	stamp := weftTimestamp(now)
	id, err := newRemoteID("e", now)
	if err != nil {
		return "", err
	}
	if a.TokenID == WeftHostToken {
		a.RequestKey = id
	}
	_, err = tx.Exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,selection_id,
		position,state,run_id,supersedes,answer_id,token_id,request_key,created_at,updated_at,revision)
		VALUES(?,?,?,?,?,?,?,(SELECT COALESCE(MAX(position),0)+1 FROM weft_entries),?,?,?,?,?,?,?,?,1)`,
		id, a.Kind, a.Ticket, a.Project, a.RepositoryID, a.Repository, nullString(a.SelectionID),
		state, nullString(a.RunID), nullString(a.Supersedes), nullString(a.AnswerID), a.TokenID, a.RequestKey, stamp, stamp)
	return id, err
}

func notPending(q *WeftQuestion, runID string) error {
	if q.RunID != runID {
		return refuse("question_not_pending", "%s is not run %s's question: it was asked by run %s", q.ID, runID, q.RunID)
	}
	if a := q.Answer; a != nil {
		queued := ""
		if a.EntryID.Valid {
			queued = ", queued as " + a.EntryID.String
		}
		return refuse("question_not_pending", "%s is not run %s's pending question: it was answered at %s by %s%s",
			q.ID, runID, a.SubmittedAt, a.ID, queued)
	}
	return refuse("question_not_pending", "%s is not run %s's pending question: it was %s", q.ID, runID, q.State)
}

func staleBase(run *WeftRun) error {
	return refuse("stale_base", "run %s is held for a stale base: %s. Start the run fresh, or resolve the branch in a terminal and dismiss the run",
		run.ID, run.ActionDetail.String)
}

// AdmitResume queues a resume of the run (§6.6): an answer entry for the
// same answer when the run is parked on an answer no segment consumed
// (§6.5), else a resume entry for a run v1 §3.8 calls resumable.
// worktreeExists reports whether a provisioned worktree is still on disk.
func (t *WeftTx) AdmitResume(runID string, revision int64, worktreeExists func(string) bool, a WeftAdmission) (*WeftEntry, error) {
	run, err := getWeftRunTx(t.tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, refuse("run_not_found", "%s is not a run on this host", runID)
	}
	if err != nil {
		return nil, err
	}
	if err := loadWeftChildren(t.tx, run); err != nil {
		return nil, err
	}
	if run.Origin == WeftOriginArchive {
		return nil, refuse("run_not_resumable", "run %s is legacy history imported from archive %s and is never resumed",
			runID, run.ArchiveID.String)
	}
	if run.Revision != revision {
		return nil, refuse("revision_conflict", "run %s is at revision %d and the request was for %d", runID, run.Revision, revision)
	}
	if run.ActionRequired.String == WeftActionStaleBase {
		return nil, staleBase(run)
	}
	if run.Disposition == WeftDispositionArchived {
		return nil, refuse("run_not_resumable", "run %s is archived, superseded by %s", runID, run.SupersededBy.String)
	}
	if err := t.refuseUnderCleanup(runID); err != nil {
		return nil, err
	}
	a.Kind = WeftEntryResume
	switch {
	case run.Phase == WeftPhaseParked:
		question, err := latestWeftQuestion(t.tx, runID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if question == nil || question.Answer == nil || question.Answer.ConsumedSegment.Valid {
			id := ""
			if question != nil {
				id = question.ID
			}
			return nil, refuse("run_not_resumable", "run %s is parked on question %s; answer it rather than resuming", runID, id)
		}
		a.Kind, a.AnswerID = WeftEntryAnswer, question.Answer.ID
	default:
		resumable, err := weftResumableTx(t.tx, run)
		if err != nil {
			return nil, err
		}
		if !resumable {
			return nil, refuse("run_not_resumable", "run %s is %s (%s) and is not resumable", runID, WeftV2Phase(run.Phase), run.Outcome.String)
		}
	}
	if err := t.guard(run.Ticket, runID); err != nil {
		return nil, err
	}
	for _, seg := range run.Segments {
		if seg.WorkerRunID.Valid && !worktreeExists(run.Worktree) {
			return nil, refuse("worktree_missing", "run %s's worktree %s is missing", runID, run.Worktree)
		}
	}
	segment, err := nextSegmentTx(t.tx, runID)
	if err != nil {
		return nil, err
	}
	if segment > WeftSegmentLimit {
		return nil, refuse("segment_limit", "run %s has claimed all %d segments", runID, WeftSegmentLimit)
	}
	a.Ticket, a.Project, a.RunID = run.Ticket, run.Project, runID
	a.RepositoryID, a.Repository = run.RepositoryID, run.Repo
	entry, err := t.insertEntry(a)
	if err != nil {
		return nil, err
	}
	if a.AnswerID != "" {
		if _, err := t.tx.Exec(`UPDATE weft_answers SET entry_id=? WHERE id=?`, entry.ID, a.AnswerID); err != nil {
			return nil, err
		}
	}
	return entry, nil
}

// StopRun stops the run, and only the run (§6.3, §6.6). A parked run is
// ended here, its pending question withdrawn and its waiting answer or resume
// entry invalidated run_changed; a live one is marked stopped — a settling
// one is left to report how far its landing got — and live is true: the
// caller then asks the process to stop.
func (t *WeftTx) StopRun(runID string, revision int64) (live bool, err error) {
	run, err := getWeftRunTx(t.tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, refuse("run_not_found", "%s is not a run on this host", runID)
	}
	if err != nil {
		return false, err
	}
	if run.Phase == WeftPhaseEnded {
		return false, refuse("run_over", "run %s is already over (%s)", runID, run.Outcome.String)
	}
	if run.Revision != revision {
		return false, refuse("revision_conflict", "run %s is at revision %d and the request was for %d", runID, run.Revision, revision)
	}
	stamp := t.Stamp()
	if run.Phase == WeftPhaseSettling {
		// A landing in flight finishes the step it is taking and takes no
		// other; its ended reads not_landed stopped, which pauses its
		// repository (§6.6). The grade it landed under stands, so the outcome
		// is left as it is. The stop lasts the attempt: a reconcile of a
		// landing lost after it, or after a restart, gets no step
		// acknowledged either.
		if _, err := t.tx.Exec(`UPDATE weft_runs SET stopped=1,updated_at=?,revision=revision+1 WHERE id=?`,
			stamp, runID); err != nil {
			return false, err
		}
		return true, appendRunEventTx(t.tx, runID, stamp)
	}
	if run.Phase != WeftPhaseParked {
		return true, markWeftStoppedTx(t.tx, runID, stamp)
	}
	if err := stopWeftParkedTx(t.tx, runID, stamp); err != nil {
		return false, err
	}
	waiting, err := loadWeftEntries(t.tx, `run_id=? AND state=? AND kind IN (?,?)`,
		runID, WeftEntryWaiting, WeftEntryAnswer, WeftEntryResume)
	if err != nil {
		return false, err
	}
	for _, e := range waiting {
		if err := t.invalidate(e, WeftInvalidRun, fmt.Sprintf("run %s was stopped while %s waited", runID, e.ID)); err != nil {
			return false, err
		}
	}
	return false, nil
}

// CancelEntry drops one waiting entry of any kind (§6.3).
func (t *WeftTx) CancelEntry(id string, revision int64) (*WeftEntry, error) {
	e, err := t.Entry(id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, refuse("entry_not_found", "%s is not a queue entry on this host", id)
	}
	if err != nil {
		return nil, err
	}
	if e.State != WeftEntryWaiting {
		return nil, refuse("entry_not_waiting", "entry %s is %s; only a waiting entry can be cancelled", id, e.State)
	}
	if e.Revision != revision {
		return nil, refuse("revision_conflict", "entry %s is at revision %d and the request was for %d", id, e.Revision, revision)
	}
	stamp := t.Stamp()
	if err := t.settleEntry(e, WeftEntryCancelled, stamp); err != nil {
		return nil, err
	}
	if err := queueStepTx(t.tx, stamp, true, id); err != nil {
		return nil, err
	}
	t.change("cancelled", e, e.RunID.String, "")
	// A cancelled cleanup is a human's refusal of it: the host never queues
	// one for the run again (§6.8).
	if e.Kind == WeftEntryCleanup {
		if _, err := t.tx.Exec(`UPDATE weft_runs SET host_cleanup_hold=1 WHERE id=?`, e.RunID.String); err != nil {
			return nil, err
		}
		t.change("held", e, e.RunID.String, "host cleanup hold set: the host will not queue a cleanup of this run again")
	}
	return t.Entry(id)
}

// ClearQueue drops every waiting launch and frontier entry; a queued answer or
// resume is a decision about one run and is cancelled on its own (§6.3).
func (t *WeftTx) ClearQueue(revision int64) error {
	current, err := queueRevision(t.tx)
	if err != nil {
		return err
	}
	if current != revision {
		return refuse("revision_conflict", "the queue is at revision %d and the request was for %d", current, revision)
	}
	entries, err := loadWeftEntries(t.tx, `state=? AND kind IN (?,?)`, WeftEntryWaiting, WeftEntryLaunch, WeftEntryFrontier)
	if err != nil || len(entries) == 0 {
		return err
	}
	stamp := t.Stamp()
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if err := t.settleEntry(e, WeftEntryCancelled, stamp); err != nil {
			return err
		}
		ids = append(ids, e.ID)
		t.change("cancelled", e, "", "cleared")
	}
	return queueStepTx(t.tx, stamp, true, ids...)
}

// DismissEntry takes one settled entry out of the listing (§6.3). It keeps
// its state and revision and stays readable by id; one already dismissed is
// answered as it stands, writing nothing.
func (t *WeftTx) DismissEntry(id string, revision int64) (*WeftEntry, error) {
	e, err := t.Entry(id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, refuse("entry_not_found", "entry %s is not on this host", id)
	}
	if err != nil {
		return nil, err
	}
	switch e.State {
	case WeftEntryWaiting:
		return nil, refuse("entry_not_settled", "entry %s is waiting, not settled: cancel it, or dismiss it once it has dispatched", id)
	case WeftEntryArchiving:
		return nil, refuse("entry_not_settled", "entry %s is archiving, not settled: dismiss it once it has dispatched", id)
	}
	if e.Revision != revision {
		return nil, refuse("revision_conflict", "entry %s is at revision %d, not %d: read the queue again and decide again", id, e.Revision, revision)
	}
	if e.Dismissed {
		return e, nil
	}
	if err := t.dismissEntries([]*WeftEntry{e}, ""); err != nil {
		return nil, err
	}
	return t.Entry(id)
}

// DismissSettled takes every settled entry the listing holds — those settled
// since settledSince and not yet dismissed — out of it (§6.3). With none
// listed it writes nothing.
func (t *WeftTx) DismissSettled(revision int64, settledSince time.Time) error {
	current, err := queueRevision(t.tx)
	if err != nil {
		return err
	}
	if current != revision {
		return refuse("revision_conflict", "the queue is at revision %d, not %d: read the queue again and decide again", current, revision)
	}
	entries, err := loadWeftEntries(t.tx, `state NOT IN (?,?) AND updated_at>=? AND dismissed=0`,
		WeftEntryWaiting, WeftEntryArchiving, weftTimestamp(settledSince))
	if err != nil || len(entries) == 0 {
		return err
	}
	return t.dismissEntries(entries, "dismiss-settled")
}

// dismissEntries is one dismissal's commit: the queue revision raised once
// and a dismissed queue event per entry. No count moves, so no capacity
// event follows.
func (t *WeftTx) dismissEntries(entries []*WeftEntry, detail string) error {
	stamp := t.Stamp()
	revision, err := bumpQueueRevisionTx(t.tx)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := appendQueueEventTx(t.tx, e.ID, revision, stamp, true); err != nil {
			return err
		}
		t.change("dismissed", e, e.RunID.String, detail)
	}
	return nil
}

func (t *WeftTx) settleEntry(e *WeftEntry, state, stamp string) error {
	_, err := t.tx.Exec(`UPDATE weft_entries SET state=?,waiting_reason=NULL,holder_kind=NULL,holder_id=NULL,
		updated_at=?,revision=revision+1 WHERE id=?`, state, stamp, e.ID)
	return err
}

// SetCapacity sets the host-wide cap (§6.1). It governs starts only: lowering
// it stops nothing, and a raise dispatches when the caller runs Dispatch.
func (t *WeftTx) SetCapacity(capacity int, revision int64) error {
	current, err := queueRevision(t.tx)
	if err != nil {
		return err
	}
	if current != revision {
		return refuse("revision_conflict", "the queue is at revision %d and the request was for %d", current, revision)
	}
	if _, err := t.tx.Exec(`INSERT INTO weft_settings(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, weftCapacityKey, strconv.Itoa(capacity)); err != nil {
		return err
	}
	return capacityStepTx(t.tx, t.Stamp())
}

// WeftDispatch is what dispatch needs from the host.
type WeftDispatch struct {
	// SharedRoot is the root a new run's worktree is derived under.
	SharedRoot, RunDirRoot string
	// AgentUsers are the host's agent users: a pipeline segment is claimed
	// with the first one no unsettled segment holds.
	AgentUsers []string
	// Land is whether the host serves landing: a run minted while it does is
	// pending its landing from the start (§7.1).
	Land bool
	// Check revalidates an entry against tk and its project's binding as they
	// are now, answering an invalidation reason and detail, or "" when the
	// entry may still dispatch.
	Check func(*WeftEntry) (reason, detail string)
	// Finished is whether a ticket reads done or closed through pkg/ticket as
	// Check's read has it: the trigger of a host-started cleanup (§6.8). Nil
	// on a host that does not serve cleanup, which cleans no run itself.
	Finished func(ticket string) bool
}

// Dispatch walks the waiting entries in position order (§6.3). One whose
// wait still applies is skipped and keeps its place, so a reserved or paused
// repository never holds back another's work, while the reservation keeps
// strict order within one repository. One that could dispatch is revalidated
// and invalidated visibly if it no longer holds; otherwise its reservation and
// slot are taken, its run minted or continued and its segment claimed, here,
// and its run named in Spawn. Nothing here waits on anything. A run an
// invalidated entry named is then evaluated for a host-started cleanup
// (§6.8) — but not for a cleanup's own, whose run it would only queue again.
func (t *WeftTx) Dispatch(d WeftDispatch) error {
	paused, _, _, err := weftHostPaused(t.tx)
	if err != nil {
		return err
	}
	c, err := weftCapacity(t.tx)
	if err != nil {
		return err
	}
	entries, err := loadWeftEntries(t.tx, `state=?`, WeftEntryWaiting)
	if err != nil {
		return err
	}
	var settled []string
	for _, e := range entries {
		invalid, err := t.dispatchEntry(e, d, paused, &c)
		if err != nil {
			return err
		}
		switch {
		case !invalid || e.Kind == WeftEntryCleanup:
		case e.Kind == WeftEntryFresh:
			settled = append(settled, e.Supersedes.String)
		case e.RunID.Valid:
			settled = append(settled, e.RunID.String)
		}
	}
	return t.cleanFinished(settled, d)
}

// dispatchEntry is one waiting entry's turn (§6.3): it waits, is
// invalidated — invalid is true — or is dispatched, its slot counted in c.
func (t *WeftTx) dispatchEntry(e *WeftEntry, d WeftDispatch, paused bool, c *WeftCapacity) (invalid bool, err error) {
	reason, holderKind, holderID, err := t.blocking(e, paused, *c, d.AgentUsers)
	if err != nil {
		return false, err
	}
	if reason != "" {
		return false, t.wait(e, reason, holderKind, holderID)
	}
	invalidated, detail, err := t.revalidate(e, d)
	if err != nil {
		return false, err
	}
	if invalidated != "" {
		return true, t.invalidate(e, invalidated, detail)
	}
	slot, err := t.dispatch(e, d)
	if err != nil {
		return false, err
	}
	if slot {
		c.Running++
	}
	return false, nil
}

// blocking is the first wait that applies to the entry (§6.3). Reconcile and
// cleanup entries ignore a landing pause and hold no slot (§6.1, §7.5);
// every other entry needs a slot and, for the pipeline segment it claims
// now or once its archive step is over, one of users free.
func (t *WeftTx) blocking(e *WeftEntry, paused bool, c WeftCapacity, users []string) (reason, holderKind, holderID string, err error) {
	if paused {
		return WeftWaitHostPaused, "", "", nil
	}
	repair := e.Kind == WeftEntryReconcile || e.Kind == WeftEntryCleanup
	var dispatch string
	err = t.tx.QueryRow(`SELECT dispatch FROM weft_repositories WHERE repository_id=?`, e.RepositoryID).Scan(&dispatch)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", "", "", err
	}
	if dispatch == WeftDispatchPaused && !repair {
		return WeftWaitRepositoryPaused, "", "", nil
	}
	err = t.tx.QueryRow(`SELECT holder_kind,holder_id FROM weft_reservations WHERE repository_id=?`, e.RepositoryID).
		Scan(&holderKind, &holderID)
	switch {
	case err == nil:
		return WeftWaitReserved, holderKind, holderID, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", "", "", err
	}
	if repair {
		return "", "", "", nil
	}
	if c.Running >= c.Cap {
		return WeftWaitCapacity, "", "", nil
	}
	user, err := freeAgentUser(t.tx, users)
	if err != nil || user == "" {
		return WeftWaitCapacity, "", "", err
	}
	return "", "", "", nil
}

// wait records what the entry waits on. A just-admitted entry writes its
// admission's queue and capacity events here, at revision 1; an older one
// writes a queue event only when its reason or holder changed.
func (t *WeftTx) wait(e *WeftEntry, reason, holderKind, holderID string) error {
	admitted := t.admitted[e.ID]
	if !admitted && e.WaitingReason.String == reason && e.HolderKind.String == holderKind && e.HolderID.String == holderID {
		return nil
	}
	delete(t.admitted, e.ID)
	stamp, bump := t.Stamp(), 1
	if admitted {
		stamp, bump = e.UpdatedAt, 0
	}
	if _, err := t.tx.Exec(`UPDATE weft_entries SET waiting_reason=?,holder_kind=?,holder_id=?,updated_at=?,
		revision=revision+? WHERE id=?`, reason, nullString(holderKind), nullString(holderID), stamp, bump, e.ID); err != nil {
		return err
	}
	t.change("waiting", e, e.RunID.String, reason+" "+holderKind+" "+holderID)
	return queueStepTx(t.tx, stamp, admitted, e.ID)
}

// revalidate is dispatch's recheck (§6.3): tk and the binding through
// d.Check, then the run an answer or resume continues still in the state the
// entry assumed.
func (t *WeftTx) revalidate(e *WeftEntry, d WeftDispatch) (string, string, error) {
	if reason, detail := d.Check(e); reason != "" {
		return reason, detail, nil
	}
	switch e.Kind {
	case WeftEntryReconcile, WeftEntryCleanup, WeftEntryFresh:
		return t.revalidateMaintenance(e, d.Finished)
	case WeftEntryAnswer, WeftEntryResume:
	default:
		return "", "", nil
	}
	run, err := getWeftRunTx(t.tx, e.RunID.String)
	if err != nil {
		return "", "", err
	}
	if run.ActionRequired.Valid {
		return WeftInvalidRun, fmt.Sprintf("run %s is held for %s", run.ID, run.ActionRequired.String), nil
	}
	if e.Kind == WeftEntryResume {
		resumable, err := weftResumableTx(t.tx, run)
		if err != nil || resumable {
			return "", "", err
		}
		return WeftInvalidRun, fmt.Sprintf("run %s is %s (%s) and no longer resumable", run.ID, WeftV2Phase(run.Phase), run.Outcome.String), nil
	}
	if run.Phase != WeftPhaseParked {
		return WeftInvalidRun, fmt.Sprintf("run %s is %s, no longer parked for answer %s", run.ID, WeftV2Phase(run.Phase), e.AnswerID.String), nil
	}
	var consumed sql.NullInt64
	err = t.tx.QueryRow(`SELECT a.consumed_segment FROM weft_answers a JOIN weft_questions q ON q.answer_id=a.id
		WHERE a.id=? AND q.state='answered'`, e.AnswerID.String).Scan(&consumed)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return WeftInvalidRun, fmt.Sprintf("answer %s is no longer its question's answer", e.AnswerID.String), nil
	case err != nil:
		return "", "", err
	case consumed.Valid:
		return WeftInvalidRun, fmt.Sprintf("answer %s was consumed by segment %d", e.AnswerID.String, consumed.Int64), nil
	}
	return "", "", nil
}

func (t *WeftTx) invalidate(e *WeftEntry, reason, detail string) error {
	delete(t.admitted, e.ID)
	stamp := t.Stamp()
	if _, err := t.tx.Exec(`UPDATE weft_entries SET state=?,waiting_reason=NULL,holder_kind=NULL,holder_id=NULL,
		invalidated_reason=?,invalidated_detail=?,updated_at=?,revision=revision+1 WHERE id=?`,
		WeftEntryInvalidated, reason, detail, stamp, e.ID); err != nil {
		return err
	}
	t.change("invalidated", e, e.RunID.String, reason+": "+detail)
	return queueStepTx(t.tx, stamp, true, e.ID)
}

// mintSettlement is the settlement a run minted now starts with (§2.4, §7.1).
func (d WeftDispatch) mintSettlement() (string, string) {
	if d.Land {
		return WeftSettlementPending, ""
	}
	return WeftSettlementNone, WeftLandingNotServed
}

// dispatch takes the entry's reservation and slot in this transaction: a
// launch or frontier entry mints its run, an answer or resume claims its
// run's next segment, a reconcile, cleanup or fresh entry its maintenance
// worker's segment, and the entry is marked dispatched with the run — or, for
// fresh, archiving. It answers whether a slot was taken.
func (t *WeftTx) dispatch(e *WeftEntry, d WeftDispatch) (bool, error) {
	now := t.store.now()
	stamp := weftTimestamp(now)
	var runID string
	switch e.Kind {
	case WeftEntryReconcile, WeftEntryCleanup, WeftEntryFresh:
		if t.admitted[e.ID] {
			stamp = e.UpdatedAt
		}
		runID, slot, err := t.dispatchMaintenance(e, d, stamp)
		if errors.Is(err, ErrWeftSegmentLimit) {
			target := e.RunID.String
			if e.Kind == WeftEntryFresh {
				target = e.Supersedes.String
			}
			return false, t.invalidate(e, WeftInvalidRun, fmt.Sprintf("run %s has claimed all %d segments", target, WeftSegmentLimit))
		}
		if err != nil {
			return false, err
		}
		revision, err := bumpQueueRevisionTx(t.tx)
		if err != nil {
			return false, err
		}
		if err := appendQueueEventTx(t.tx, e.ID, revision, stamp, false); err != nil {
			return false, err
		}
		if err := appendRunEventTx(t.tx, runID, stamp); err != nil {
			return false, err
		}
		if err := appendCapacityEventTx(t.tx, revision, stamp); err != nil {
			return false, err
		}
		t.Spawn = append(t.Spawn, runID)
		t.change("dispatched", e, runID, "")
		return slot, nil
	case WeftEntryLaunch, WeftEntryFrontier:
		worktree, err := sandbox.WorktreePath(d.SharedRoot, e.Repository, e.Ticket)
		if err != nil {
			return false, t.invalidate(e, WeftInvalidBinding, fmt.Sprintf("%s cannot be resolved: %v", e.Repository, err))
		}
		user, err := t.agentUser(e, d)
		if err != nil {
			return false, err
		}
		settlement, detail := d.mintSettlement()
		runID, err = mintWeftRunTx(t.tx, WeftLaunch{
			Ticket: e.Ticket, Project: e.Project, Repo: e.Repository, Branch: e.Ticket,
			Worktree: worktree, RunDirRoot: d.RunDirRoot, AgentUser: user,
		}, now, e.ID, WeftRecordedV2, settlement, detail)
		if err != nil {
			return false, err
		}
		if err := acquireRepository(t.tx, e.RepositoryID, e.Repository, WeftHolderRun, runID, stamp); err != nil {
			return false, err
		}
	case WeftEntryAnswer, WeftEntryResume:
		run, err := getWeftRunTx(t.tx, e.RunID.String)
		if err != nil {
			return false, err
		}
		answerID := e.AnswerID.String
		if e.Kind == WeftEntryResume {
			if answerID, err = resumeAnswerTx(t.tx, run.ID); err != nil {
				return false, err
			}
		}
		user, err := t.agentUser(e, d)
		if err != nil {
			return false, err
		}
		segment, err := claimNextSegmentTx(t.tx, run, e.Kind, answerID, user, d.RunDirRoot, now)
		if errors.Is(err, ErrWeftSegmentLimit) {
			return false, t.invalidate(e, WeftInvalidRun, fmt.Sprintf("run %s has claimed all %d segments", run.ID, WeftSegmentLimit))
		}
		if err != nil {
			return false, err
		}
		if answerID != "" {
			if _, err := t.tx.Exec(`UPDATE weft_answers SET segment=? WHERE id=?`, segment, answerID); err != nil {
				return false, err
			}
		}
		runID = run.ID
	default:
		return false, fmt.Errorf("entry %s: %s entries are not dispatched by this host", e.ID, e.Kind)
	}
	admitted := t.admitted[e.ID]
	delete(t.admitted, e.ID)
	bump := 1
	if admitted {
		stamp, bump = e.UpdatedAt, 0
	}
	if _, err := t.tx.Exec(`UPDATE weft_entries SET state=?,run_id=?,waiting_reason=NULL,holder_kind=NULL,holder_id=NULL,
		updated_at=?,revision=revision+? WHERE id=?`, WeftEntryDispatched, runID, stamp, bump, e.ID); err != nil {
		return false, err
	}
	revision, err := bumpQueueRevisionTx(t.tx)
	if err != nil {
		return false, err
	}
	if err := appendQueueEventTx(t.tx, e.ID, revision, stamp, false); err != nil {
		return false, err
	}
	if err := appendRunEventTx(t.tx, runID, stamp); err != nil {
		return false, err
	}
	if err := appendCapacityEventTx(t.tx, revision, stamp); err != nil {
		return false, err
	}
	t.Spawn = append(t.Spawn, runID)
	t.change("dispatched", e, runID, "")
	return true, nil
}

// DispatchWeft is one dispatch pass in its own transaction.
func (s *Store) DispatchWeft(d WeftDispatch) (spawn []string, changes []WeftChange, err error) {
	err = s.WeftTx(func(t *WeftTx) error {
		if err := t.Dispatch(d); err != nil {
			return err
		}
		spawn, changes = t.Spawn, t.Changes
		return nil
	})
	return spawn, changes, err
}

// RevalidateEntries rechecks the waiting entries of projects whose catalog
// changed (§6.3) against d's read, invalidating visibly those that no longer
// hold.
func (t *WeftTx) RevalidateEntries(projects []string, d WeftDispatch) error {
	for _, project := range projects {
		entries, err := loadWeftEntries(t.tx, `state=? AND project=?`, WeftEntryWaiting, project)
		if err != nil {
			return err
		}
		for _, e := range entries {
			reason, detail, err := t.revalidate(e, d)
			if err != nil {
				return err
			}
			if reason != "" {
				if err := t.invalidate(e, reason, detail); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// WeftReservation is one repository's holder (§6.2).
type WeftReservation struct {
	RepositoryID, Repository, HolderKind, HolderID, AcquiredAt string
}

// weftInFlightSQL is true while a run has a segment the host has not reaped.
const weftInFlightSQL = `EXISTS (SELECT 1 FROM weft_segments s JOIN runs r ON r.id=s.ledger_run_id
	WHERE s.run_id=? AND r.status IN ('` + StatusClaimed + `','` + StatusRunning + `'))`

// RestartWeftQueue is §12's steps 4 and 5, run after the restart pass and
// before the listener accepts or anything dispatches. Step 4 deletes every
// reservation whose holder is gone — a run with no segment left in flight, a
// terminal with no session holding a share, a maintenance entry with nothing
// in flight — keeping settlement holders for their reconcile and archiving
// entries' rows for step 5. Step 5 settles each archiving entry whose archive
// worker is gone by the ref it was to write: archiveHead answers the commit
// refs/weft/archive/<run> names in the repository, or "" when absent. At the
// superseded run's head, the maintenance → run transaction of §6.7 runs and
// its new run is named in spawn; otherwise the row and slot are released and
// the entry waits again. Nothing interrupted is relaunched: spawn names only
// those new runs and the runs whose reconcile step 2 claimed, which the
// caller starts before dispatch.
func (s *Store) RestartWeftQueue(d WeftDispatch, archiveHead func(repo, ref string) (string, error)) (released []WeftReservation, changes []WeftChange, spawn []string, err error) {
	err = s.WeftTx(func(t *WeftTx) error {
		rows, err := t.tx.Query(`SELECT repository_id,repository,holder_kind,holder_id,acquired_at FROM weft_reservations
			ORDER BY repository_id`)
		if err != nil {
			return err
		}
		var reservations []WeftReservation
		for rows.Next() {
			var r WeftReservation
			if err := rows.Scan(&r.RepositoryID, &r.Repository, &r.HolderKind, &r.HolderID, &r.AcquiredAt); err != nil {
				rows.Close()
				return err
			}
			reservations = append(reservations, r)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, r := range reservations {
			gone, err := t.holderGone(r)
			if err != nil {
				return err
			}
			if !gone {
				continue
			}
			if err := releaseRepositoriesTx(t.tx, t.Stamp(), `repository_id=? AND holder_kind=? AND holder_id=?`,
				r.RepositoryID, r.HolderKind, r.HolderID); err != nil {
				return err
			}
			released = append(released, r)
		}
		archiving, err := loadWeftEntries(t.tx, `state=?`, WeftEntryArchiving)
		if err != nil {
			return err
		}
		for _, e := range archiving {
			var live bool
			if err := t.tx.QueryRow(`SELECT `+weftInFlightSQL, e.Supersedes.String).Scan(&live); err != nil {
				return err
			}
			if live {
				continue
			}
			ref := WeftArchiveRef(e.Supersedes.String)
			head, err := archiveHead(e.Repository, ref)
			if err != nil {
				return fmt.Errorf("entry %s: read %s in %s: %w", e.ID, ref, e.Repository, err)
			}
			if err := t.settleArchiving(e, d, head); err != nil {
				return err
			}
		}
		rows, err = t.tx.Query(`SELECT s.run_id FROM weft_segments s JOIN runs r ON r.id=s.ledger_run_id
			WHERE s.kind=? AND r.status=? ORDER BY r.claimed_at`, WeftSegmentReconcile, StatusClaimed)
		if err != nil {
			return err
		}
		for rows.Next() {
			var runID string
			if err := rows.Scan(&runID); err != nil {
				rows.Close()
				return err
			}
			t.Spawn = append(t.Spawn, runID)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		changes, spawn = t.Changes, t.Spawn
		return nil
	})
	return released, changes, spawn, err
}

func (t *WeftTx) holderGone(r WeftReservation) (bool, error) {
	var live bool
	switch r.HolderKind {
	case "run":
		err := t.tx.QueryRow(`SELECT `+weftInFlightSQL, r.HolderID).Scan(&live)
		return !live, err
	case WeftHolderTerminal:
		err := t.tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM weft_terminals WHERE repository_id=?
			AND `+weftTerminalShareSQL+`)`, r.RepositoryID).Scan(&live)
		return !live, err
	case WeftHolderSettlement:
		// Kept for the reconcile step 2 dispatched under it, which releases it.
		return false, nil
	case "maintenance":
		var state string
		var run sql.NullString
		err := t.tx.QueryRow(`SELECT state,COALESCE(run_id,supersedes) FROM weft_entries WHERE id=?`, r.HolderID).Scan(&state, &run)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil || state == WeftEntryArchiving {
			return false, err
		}
		err = t.tx.QueryRow(`SELECT `+weftInFlightSQL, run.String).Scan(&live)
		return !live, err
	}
	return false, nil
}

// settleArchiving is §6.7's settlement of a fresh entry whose archive step
// is over, by the head the archive ref names ("" when absent). At the
// head the entry archives at, ONE transaction mints the new run, rewrites the
// maintenance row in place to the run holder, marks the entry dispatched and
// ends the old run archived; the slot is carried, so no capacity event is
// written. Otherwise the row and slot are released and the entry waits again.
func (t *WeftTx) settleArchiving(e *WeftEntry, d WeftDispatch, head string) error {
	old := e.Supersedes.String
	ref := WeftArchiveRef(old)
	now := t.store.now()
	stamp := weftTimestamp(now)
	want, err := weftArchiveHead(t.tx, e.ID)
	if err != nil {
		return err
	}
	if head == "" || head != want {
		if err := releaseRepositoriesTx(t.tx, stamp, `holder_kind=? AND holder_id=?`, WeftHolderMaintenance, e.ID); err != nil {
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
		reason, holderKind, holderID, err := t.blocking(e, paused, c, d.AgentUsers)
		if err != nil {
			return err
		}
		if reason == "" {
			reason = WeftWaitCapacity
		}
		if _, err := t.tx.Exec(`UPDATE weft_entries SET state=?,waiting_reason=?,holder_kind=?,holder_id=?,updated_at=?,
			revision=revision+1 WHERE id=?`, WeftEntryWaiting, reason, nullString(holderKind), nullString(holderID), stamp, e.ID); err != nil {
			return err
		}
		t.change("archive_absent", e, old, ref+" does not name "+want)
		return queueStepTx(t.tx, stamp, true, e.ID)
	}
	worktree, err := sandbox.WorktreePath(d.SharedRoot, e.Repository, e.Ticket)
	if err != nil {
		return err
	}
	user, err := t.agentUser(e, d)
	if err != nil {
		return err
	}
	settlement, detail := d.mintSettlement()
	runID, err := mintWeftRunTx(t.tx, WeftLaunch{
		Ticket: e.Ticket, Project: e.Project, Repo: e.Repository, Branch: e.Ticket,
		Worktree: worktree, RunDirRoot: d.RunDirRoot, AgentUser: user,
	}, now, e.ID, WeftRecordedV2, settlement, detail)
	if err != nil {
		return err
	}
	res, err := t.tx.Exec(`UPDATE weft_reservations SET holder_kind=?,holder_id=?,acquired_at=? WHERE repository_id=?
		AND holder_kind=? AND holder_id=?`, WeftHolderRun, runID, stamp, e.RepositoryID, WeftHolderMaintenance, e.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		err = repositoryStepTx(t.tx, e.RepositoryID, e.Repository, stamp)
	} else {
		err = acquireRepository(t.tx, e.RepositoryID, e.Repository, WeftHolderRun, runID, stamp)
	}
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(`UPDATE weft_runs SET supersedes=? WHERE id=?`, old, runID); err != nil {
		return err
	}
	pending, err := pendingWeftQuestion(t.tx, old)
	switch {
	case err == nil:
		if _, err := t.tx.Exec(`UPDATE weft_questions SET state='withdrawn' WHERE id=?`, pending.ID); err != nil {
			return err
		}
		withdrawn, err := getQuestionTx(t.tx, pending.ID)
		if err != nil {
			return err
		}
		if err := appendQuestionEventTx(t.tx, withdrawn, stamp); err != nil {
			return err
		}
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	if _, err := t.tx.Exec(`UPDATE weft_runs SET phase=?,outcome='stopped',stopped=1,disposition=?,superseded_by=?,
		archive_ref=?,action_required=NULL,action_detail=NULL,ended_at=COALESCE(ended_at,?),updated_at=?,
		revision=revision+1 WHERE id=?`,
		WeftPhaseEnded, WeftDispositionArchived, runID, ref, stamp, stamp, old); err != nil {
		return err
	}
	if _, err := t.tx.Exec(`UPDATE weft_entries SET state=?,run_id=?,updated_at=?,revision=revision+1 WHERE id=?`,
		WeftEntryDispatched, runID, stamp, e.ID); err != nil {
		return err
	}
	// The slot is carried from the archive step to the run: no capacity event.
	if err := queueStepTx(t.tx, stamp, false, e.ID); err != nil {
		return err
	}
	if err := appendRunEventTx(t.tx, runID, stamp); err != nil {
		return err
	}
	if _, err := settleUngradedTx(t.tx, old); err != nil {
		return err
	}
	if err := appendRunEventTx(t.tx, old, stamp); err != nil {
		return err
	}
	// A retry of the archived run's landing waiting here would reconcile
	// superseded work.
	retries, err := loadWeftEntries(t.tx, `state=? AND kind=? AND run_id=?`, WeftEntryWaiting, WeftEntryReconcile, old)
	if err != nil {
		return err
	}
	for _, retry := range retries {
		if err := t.invalidate(retry, WeftInvalidRun, fmt.Sprintf("run %s was archived, superseded by %s", old, runID)); err != nil {
			return err
		}
	}
	t.Spawn = append(t.Spawn, runID)
	t.change("dispatched", e, runID, "archived "+old+" at "+ref)
	return nil
}
