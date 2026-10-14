package ledger

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"shuttle/internal/sandbox"
)

// The weft-service/v2 values a run carries (weft-service/v2 §6.9, §7.1).
const (
	// WeftServiceVersion is the contract every state event payload carries.
	WeftServiceVersion = "weft-service/v2"

	WeftRecordedV1          = "weft-remote/v1"
	WeftOriginHosted        = "hosted"
	WeftDispositionRetained = "retained"
	WeftDispositionArchived = "archived"
	WeftSettlementNone      = "not_applicable"

	// WeftV1SettlementDetail is §14.1's settlement detail for every run
	// recorded under weft-remote/v1: the v1 protocol has no landing.
	WeftV1SettlementDetail = "recorded under weft-remote/v1, which never lands"

	// WeftCapacityDefault is §6.1's host-wide cap while none is persisted.
	WeftCapacityDefault = 3

	weftCapacityKey = "capacity"
	// weftEventFloorKey names the last seq allocated under weft-remote/v1, as
	// the migration found it. Seqs continue across the migration (§14.2), so
	// every event at or below it is a v1 payload and every one above a v2 one.
	weftEventFloorKey = "v1_event_floor"
)

// migrateWeftV2 is §14.1 step 3 inside migrate's transaction: the v2 tables,
// the repository locks moved to reservations, every existing run labelled as
// recorded under weft-remote/v1, and host_id minted once.
func migrateWeftV2(tx *sql.Tx, now time.Time) error {
	if _, err := tx.Exec(weftV2SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE weft_runs SET origin=?,recorded_under=?,disposition=?,
		settlement_state=?,settlement_detail=?`, WeftOriginHosted, WeftRecordedV1,
		WeftDispositionRetained, WeftSettlementNone, WeftV1SettlementDetail); err != nil {
		return err
	}
	repos := map[string]string{}
	rows, err := tx.Query(`SELECT id,repo FROM weft_runs`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, repo string
		if err := rows.Scan(&id, &repo); err != nil {
			rows.Close()
			return err
		}
		repos[id] = repo
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for id, repo := range repos {
		if _, err := tx.Exec(`UPDATE weft_runs SET repository_id=? WHERE id=?`, sandbox.RepositoryID(repo), id); err != nil {
			return err
		}
	}
	type lock struct{ run, acquired string }
	var locks []lock
	rows, err = tx.Query(`SELECT held_by_run,acquired_at FROM weft_repository_locks`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var l lock
		if err := rows.Scan(&l.run, &l.acquired); err != nil {
			rows.Close()
			return err
		}
		locks = append(locks, l)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, l := range locks {
		repo, ok := repos[l.run]
		if !ok {
			return fmt.Errorf("repository lock held by %s, which is not a run", l.run)
		}
		// A plain INSERT: two locks naming one repository would be a v1
		// reservation breach, and the migration refuses it rather than
		// choosing a holder.
		if _, err := tx.Exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
			VALUES(?,?,?,?,?)`, sandbox.RepositoryID(repo), repo, "run", l.run, l.acquired); err != nil {
			return fmt.Errorf("move repository lock of %s: %w", l.run, err)
		}
	}
	if _, err := tx.Exec(`DROP TABLE weft_repository_locks`); err != nil {
		return err
	}
	// A v1 answer was consumed by the segment launched with it once that
	// segment's worker said hello; one that never did is still unconsumed.
	if _, err := tx.Exec(`UPDATE weft_answers SET consumed_segment=segment WHERE EXISTS (
		SELECT 1 FROM weft_segments s WHERE s.run_id=weft_answers.run_id
		AND s.segment=weft_answers.segment AND s.worker_run_id IS NOT NULL)`); err != nil {
		return err
	}
	hostID, err := newHostID()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO weft_host(host_id,created_at) VALUES(?,?)`, hostID, weftTimestamp(now)); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO weft_settings(key,value) VALUES(?,
		CAST(COALESCE((SELECT seq FROM sqlite_sequence WHERE name='weft_events'),0) AS TEXT))`, weftEventFloorKey)
	return err
}

// newHostID mints §4.1's h-<name>-<8 hex>, the name taken from the machine's
// host name so an operator can tell two hosts' caches apart.
func newHostID() (string, error) {
	host, _ := os.Hostname()
	host, _, _ = strings.Cut(strings.ToLower(host), ".")
	name := []byte(host)
	for i, c := range name {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			name[i] = '-'
		}
	}
	if len(name) > 40 {
		name = name[:40]
	}
	if len(name) == 0 {
		name = []byte("host")
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "h-" + string(name) + "-" + hex.EncodeToString(b[:]), nil
}

// WeftHostID is the host_id the migration minted.
func (s *Store) WeftHostID() (string, error) {
	var id string
	err := s.db.QueryRow(`SELECT host_id FROM weft_host`).Scan(&id)
	return id, err
}

// WeftV1EventFloor is the last seq written under weft-remote/v1.
func (s *Store) WeftV1EventFloor() (int64, error) {
	var floor int64
	err := s.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM weft_settings WHERE key=?`, weftEventFloorKey).Scan(&floor)
	return floor, err
}

// WeftStreamBounds is the v2 state stream's window: floor is the last seq
// written under weft-remote/v1, newest the last seq allocated, and oldest the
// first v2 event still retained, or newest+1 when none is. A v1 payload is
// never a v2 event (§14.2), so the window never reaches below the floor.
func (s *Store) WeftStreamBounds() (floor, oldest, newest int64, err error) {
	floor, err = s.WeftV1EventFloor()
	if err != nil {
		return 0, 0, 0, err
	}
	newest, err = s.WeftAsOf()
	if err != nil {
		return 0, 0, 0, err
	}
	err = s.db.QueryRow(`SELECT COALESCE(MIN(seq),?) FROM weft_events WHERE seq>?`, newest+1, floor).Scan(&oldest)
	return floor, oldest, newest, err
}

// WeftBinding is one registered project as a catalog read found it: the
// digest its tickets were read at and the repository it is bound to, empty
// when it names none.
type WeftBinding struct {
	Digest, RepositoryID, Repository string
}

// SyncWeftCatalog records the digest each registered project's tickets were
// read at. A project seen for the first time is recorded at revision 0; one
// whose digest moved, or that is no longer in bindings, has its revision
// raised. Each recorded or raised project writes its catalog event in the same
// transaction, so a revision a read reports is one the stream carries; changed
// names them. Each project's repository binding is kept as read, so a
// repository resource names its projects (§6.2).
func (s *Store) SyncWeftCatalog(bindings map[string]WeftBinding) (revisions map[string]int64, changed []string, err error) {
	revisions = map[string]int64{}
	err = s.withTx(func(tx *sql.Tx) error {
		type row struct {
			revision int64
			digest   string
		}
		recorded := map[string]row{}
		rows, err := tx.Query(`SELECT project,revision,digest FROM weft_catalog`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var name string
			var r row
			if err := rows.Scan(&name, &r.revision, &r.digest); err != nil {
				rows.Close()
				return err
			}
			recorded[name] = r
		}
		if err := rows.Close(); err != nil {
			return err
		}
		var names []string
		for name := range recorded {
			names = append(names, name)
		}
		for name := range bindings {
			if _, ok := recorded[name]; !ok {
				names = append(names, name)
			}
		}
		slices.Sort(names)
		stamp := weftTimestamp(s.now())
		for _, name := range names {
			binding := bindings[name]
			digest := binding.Digest
			prior, seen := recorded[name]
			if seen {
				if _, err := tx.Exec(`UPDATE weft_catalog SET repository_id=?,repository=? WHERE project=?`,
					nullString(binding.RepositoryID), nullString(binding.Repository), name); err != nil {
					return err
				}
			}
			switch {
			case !seen:
				if _, err := tx.Exec(`INSERT INTO weft_catalog(project,revision,digest,repository_id,repository) VALUES(?,0,?,?,?)`,
					name, digest, nullString(binding.RepositoryID), nullString(binding.Repository)); err != nil {
					return err
				}
			case prior.digest != digest:
				prior.revision++
				if _, err := tx.Exec(`UPDATE weft_catalog SET revision=?,digest=? WHERE project=?`, prior.revision, digest, name); err != nil {
					return err
				}
			default:
				revisions[name] = prior.revision
				continue
			}
			revisions[name] = prior.revision
			changed = append(changed, name)
			if err := appendCatalogEventTx(tx, name, prior.revision, stamp); err != nil {
				return err
			}
		}
		return nil
	})
	return revisions, changed, err
}

func appendCatalogEventTx(tx *sql.Tx, project string, revision int64, stamp string) error {
	return appendWeftEventTx(tx, "", "catalog", stamp, func(seq int64) any {
		return catalogEventPayload{
			ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp,
			Project: project, CatalogRevision: revision,
		}
	})
}

type catalogEventPayload struct {
	ContractVersion string `json:"contract_version"`
	Seq             int64  `json:"seq"`
	TS              string `json:"ts"`
	Project         string `json:"project"`
	CatalogRevision int64  `json:"catalog_revision"`
}

// WeftCounts are the dashboard's figures (§5.3).
type WeftCounts struct {
	Capacity, Running, Waiting                          int
	Active, Blocked, ActionRequired                     int
	QuestionsPending, RepositoriesPaused, TerminalsOpen int
}

// WeftDashboardCounts reads every dashboard figure in one statement, so they
// agree with each other. Capacity's running count is weftRunningSQL's.
func (s *Store) WeftDashboardCounts() (WeftCounts, error) {
	c := WeftCounts{Capacity: WeftCapacityDefault}
	var capacity sql.NullString
	err := s.db.QueryRow(`SELECT
		(SELECT value FROM weft_settings WHERE key=?),
		(SELECT count(*) FROM weft_runs WHERE phase IN (?,?,?)),
		(SELECT count(*) FROM weft_runs WHERE phase=?),
		(SELECT count(*) FROM weft_runs WHERE action_required IS NOT NULL),
		(SELECT count(*) FROM weft_questions WHERE state='pending'),
		(SELECT count(*) FROM weft_entries WHERE state='waiting'),
		(`+weftRunningSQL+`),
		(SELECT count(*) FROM weft_repositories WHERE dispatch='paused'),
		(SELECT count(*) FROM weft_terminals WHERE state IN ('starting','live','detached'))`,
		weftCapacityKey, WeftPhaseQueued, WeftPhaseRunning, WeftPhaseSettling, WeftPhaseParked).
		Scan(&capacity, &c.Active, &c.Blocked, &c.ActionRequired, &c.QuestionsPending, &c.Waiting,
			&c.Running, &c.RepositoriesPaused, &c.TerminalsOpen)
	if err != nil {
		return c, err
	}
	if capacity.Valid {
		if c.Capacity, err = strconv.Atoi(capacity.String); err != nil {
			return c, fmt.Errorf("weft capacity %q: %w", capacity.String, err)
		}
	}
	return c, nil
}

// WeftEntryRef is the waiting or archiving entry a ticket has in the queue.
type WeftEntryRef struct {
	ID, Kind, State, Ticket string
}

// WeftPendingEntries maps each ticket to its latest entry still waiting or
// archiving.
func (s *Store) WeftPendingEntries() (map[string]WeftEntryRef, error) {
	rows, err := s.db.Query(`SELECT id,kind,state,ticket FROM weft_entries
		WHERE state IN ('waiting','archiving') ORDER BY position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]WeftEntryRef{}
	for rows.Next() {
		var e WeftEntryRef
		if err := rows.Scan(&e.ID, &e.Kind, &e.State, &e.Ticket); err != nil {
			return nil, err
		}
		out[e.Ticket] = e
	}
	return out, rows.Err()
}

// WeftRunQuery filters history. Before names the run a page ends at: the
// page holds the runs after it, newest first.
type WeftRunQuery struct {
	Ticket, State, Origin, Before string
	Limit                         int
}

// QueryWeftRuns reads runs newest first, ties broken by id so a page boundary
// is exact.
func (s *Store) QueryWeftRuns(q WeftRunQuery) ([]*WeftRun, error) {
	query := `SELECT ` + weftRunColumns + ` FROM weft_runs`
	var where []string
	var args []any
	if q.Ticket != "" {
		where = append(where, "ticket=?")
		args = append(args, q.Ticket)
	}
	switch q.State {
	case "active":
		where = append(where, "phase IN ('queued','running','settling')")
	case "blocked":
		where = append(where, "phase='parked'")
	case "over":
		where = append(where, "phase='ended'")
	}
	if q.Origin != "" {
		where = append(where, "origin=?")
		args = append(args, q.Origin)
	}
	if q.Before != "" {
		where = append(where, "(created_at,id)<(SELECT created_at,id FROM weft_runs WHERE id=?)")
		args = append(args, q.Before)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY created_at DESC,id DESC"
	if q.Limit > 0 {
		query += " LIMIT " + strconv.Itoa(q.Limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	var out []*WeftRun
	for rows.Next() {
		run, err := scanWeftRun(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, run := range out {
		if err := loadWeftChildren(s.db, run); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// LatestWeftQuestion is the run's most recent question in any state, with its
// answer, or sql.ErrNoRows when it never asked one.
func (s *Store) LatestWeftQuestion(runID string) (*WeftQuestion, error) {
	return latestWeftQuestion(s.db, runID)
}

func latestWeftQuestion(q querier, runID string) (*WeftQuestion, error) {
	var id string
	if err := q.QueryRow(`SELECT id FROM weft_questions WHERE run_id=? ORDER BY asked_at DESC,rowid DESC LIMIT 1`,
		runID).Scan(&id); err != nil {
		return nil, err
	}
	return weftQuestion(q, id)
}
