package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// agentUserHost is a store with n repositories bound, one launch entry
// admitted for each, dispatched with users.
func agentUserHost(t *testing.T, n int, users ...string) (*Store, WeftDispatch, []*WeftEntry) {
	t.Helper()
	s := newStore(t)
	root := t.TempDir()
	bindings := map[string]WeftBinding{}
	var repos []string
	for i := range n {
		repo := filepath.Join(root, fmt.Sprintf("repo%d", i))
		if err := mkdir(repo); err != nil {
			t.Fatal(err)
		}
		repos = append(repos, repo)
		bindings[fmt.Sprintf("p%d", i)] = WeftBinding{Digest: "tk", RepositoryID: sandboxID(repo), Repository: repo}
	}
	if _, _, err := s.SyncWeftCatalog(bindings); err != nil {
		t.Fatal(err)
	}
	d := WeftDispatch{SharedRoot: filepath.Join(root, "shared"), RunDirRoot: filepath.Join(root, "runs"), AgentUsers: users,
		Check: func(*WeftEntry) (string, string) { return "", "" }}
	var entries []*WeftEntry
	if err := s.WeftTx(func(tx *WeftTx) error {
		for i, repo := range repos {
			e, err := tx.Admit(WeftAdmission{Kind: WeftEntryLaunch, Ticket: fmt.Sprintf("p%d/t-%04d", i, i), Project: fmt.Sprintf("p%d", i),
				RepositoryID: sandboxID(repo), Repository: repo, TokenID: "token", RequestKey: fmt.Sprintf("k%d", i)})
			if err != nil {
				return err
			}
			entries = append(entries, e)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return s, d, entries
}

// agentUsersOf maps each dispatched entry's run to the user its latest
// segment holds.
func agentUsersOf(t *testing.T, s *Store, entries []*WeftEntry) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, e := range entries {
		got, err := s.WeftQueue(testClock)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range got.Entries {
			if q.ID != e.ID || !q.RunID.Valid {
				continue
			}
			run, err := s.GetWeftRun(q.RunID.String)
			if err != nil {
				t.Fatal(err)
			}
			out[run.ID] = run.Segments[len(run.Segments)-1].AgentUser.String
		}
	}
	return out
}

// Concurrent segments are claimed with distinct agent users, in the order
// the host lists them. With every user held an entry waits on capacity even
// under a cap that would admit it, and once a segment settles its user is
// the one the next segment is claimed with.
func TestDispatchClaimsEachSegmentWithItsOwnAgentUser(t *testing.T) {
	s, d, entries := agentUserHost(t, 3, "weftagent1", "weftagent2")
	if err := setCapacity(t, s, 3); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.DispatchWeft(d); err != nil {
		t.Fatal(err)
	}
	users := agentUsersOf(t, s, entries)
	if len(users) != 2 {
		t.Fatalf("dispatched %v, want two runs: one per agent user", users)
	}
	held := map[string]string{}
	for runID, user := range users {
		if user != "weftagent1" && user != "weftagent2" || held[user] != "" {
			t.Fatalf("runs hold %v, want weftagent1 and weftagent2 once each", users)
		}
		held[user] = runID
	}
	waiting, err := s.WeftQueue(testClock)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range waiting.Entries {
		if e.ID == entries[2].ID && (e.State != WeftEntryWaiting || e.WaitingReason.String != WeftWaitCapacity) {
			t.Fatalf("third entry = %+v, want waiting on capacity", e)
		}
	}

	first := held["weftagent1"]
	if err := s.CompleteWeftSegment(first, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.DispatchWeft(d); err != nil {
		t.Fatal(err)
	}
	users = agentUsersOf(t, s, entries)
	if len(users) != 3 {
		t.Fatalf("dispatched %v after a settlement, want the third", users)
	}
	for runID, user := range users {
		if runID != first && runID != held["weftagent2"] && user != "weftagent1" {
			t.Fatalf("the third run holds %q, want the settled segment's weftagent1", user)
		}
	}
}

// A maintenance segment runs as the operator and holds no agent user.
func TestMaintenanceSegmentsHoldNoAgentUser(t *testing.T) {
	s, d, entries := agentUserHost(t, 1, "weftagent1")
	if _, _, err := s.DispatchWeft(d); err != nil {
		t.Fatal(err)
	}
	var runID string
	for id := range agentUsersOf(t, s, entries) {
		runID = id
	}
	if err := s.withTx(func(tx *sql.Tx) error {
		_, err := claimMaintenanceSegmentTx(tx, runID, WeftSegmentCleanup, d.RunDirRoot, testClock)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetWeftRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := run.Segments[1]; got.Kind != WeftSegmentCleanup || got.AgentUser.Valid {
		t.Fatalf("maintenance segment = %+v, want no agent user", got)
	}
	if _, ok, err := s.WeftSegmentAgent(run.Segments[1].LedgerRunID); ok || err != nil {
		t.Fatalf("maintenance row reads as an agent segment: %v %v", ok, err)
	}
	a, ok, err := s.WeftSegmentAgent(run.Segments[0].LedgerRunID)
	if !ok || err != nil || a != (WeftAgentSegment{RunID: runID, Segment: 1, AgentUser: "weftagent1"}) {
		t.Fatalf("launch row = %+v %v %v", a, ok, err)
	}
	for _, seg := range run.Segments {
		if _, ok, err := s.WeftLegacyCodingWorktree(seg.LedgerRunID); ok || err != nil {
			t.Fatalf("segment %d (%s) reads as a pre-agent-user coding segment: %v %v", seg.Segment, seg.Kind, ok, err)
		}
	}
}

// A segment that launches a model and holds no agent user was launched before
// agent users, and reads with its run's worktree; a maintenance segment holds
// no user either but launches no model, and does not.
func TestWeftLegacyCodingWorktreeNamesOnlyAModelSegmentWithNoUser(t *testing.T) {
	s := newStore(t)
	root := t.TempDir()
	for i, kind := range []string{WeftSegmentReconcile, WeftSegmentCleanup, WeftSegmentArchive} {
		repo := filepath.Join(root, kind)
		run, err := s.ClaimWeft(WeftLaunch{Ticket: fmt.Sprintf("demo/t-%d", i), Project: "demo", Repo: repo,
			Branch: kind, Worktree: filepath.Join(root, "worktrees", kind), RunDirRoot: filepath.Join(root, "runs")})
		if err != nil {
			t.Fatal(err)
		}
		worktree, ok, err := s.WeftLegacyCodingWorktree(run.Segments[0].LedgerRunID)
		if !ok || err != nil || worktree != run.Worktree {
			t.Fatalf("launch with no agent user = %q %v %v, want %q", worktree, ok, err, run.Worktree)
		}
		if err := s.withTx(func(tx *sql.Tx) error {
			_, err := claimMaintenanceSegmentTx(tx, run.ID, kind, filepath.Join(root, "runs"), testClock)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if run, err = s.GetWeftRun(run.ID); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := s.WeftLegacyCodingWorktree(run.Segments[1].LedgerRunID); ok || err != nil {
			t.Fatalf("%s segment reads as a pre-agent-user coding segment: %v %v", kind, ok, err)
		}
	}
	if _, ok, err := s.WeftLegacyCodingWorktree("no-such-row"); ok || err != nil {
		t.Fatalf("unknown row = %v %v", ok, err)
	}
}

// An unset cap is the default bounded by the agent users the host was
// started with; a stored cap above them is refused, naming both, and leaves
// the ledger as it was.
func TestConfigureWeftAgentUsersBoundsTheCap(t *testing.T) {
	s := newStore(t)
	if err := s.ConfigureWeftAgentUsers(2); err != nil {
		t.Fatal(err)
	}
	if c, err := s.WeftCapacity(); err != nil || c.Cap != 2 {
		t.Fatalf("cap with two agent users = %+v %v, want 2", c, err)
	}
	if counts, err := s.WeftDashboardCounts(); err != nil || counts.Capacity != 2 {
		t.Fatalf("dashboard capacity = %+v %v, want 2", counts, err)
	}
	if err := s.ConfigureWeftAgentUsers(5); err != nil {
		t.Fatal(err)
	}
	if c, err := s.WeftCapacity(); err != nil || c.Cap != WeftCapacityDefault {
		t.Fatalf("cap with five agent users = %+v %v, want %d", c, err, WeftCapacityDefault)
	}
	if err := setCapacity(t, s, 4); err != nil {
		t.Fatal(err)
	}
	err := s.ConfigureWeftAgentUsers(3)
	if err == nil || !strings.Contains(err.Error(), "capacity stored in the ledger is 4") ||
		!strings.Contains(err.Error(), "3 agent user(s) in weft.agent_users") {
		t.Fatalf("a stored cap of 4 over three agent users = %v", err)
	}
	if c, err := s.WeftCapacity(); err != nil || c.Cap != 4 {
		t.Fatalf("cap after the refusal = %+v %v, want 4", c, err)
	}
	if err := s.ConfigureWeftAgentUsers(4); err != nil {
		t.Fatalf("a cap equal to the agent users: %v", err)
	}
}

// A ledger stamped 7 gains weft_segments.agent_user, its segments reading
// as holding none.
func TestSchemaSevenMigratesToEight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	run := claimVisible(t, s, "demo/migrated-0008")
	for _, stmt := range []string{`ALTER TABLE weft_segments DROP COLUMN agent_user`,
		`ALTER TABLE weft_runs DROP COLUMN host_cleanup_hold`,
		`ALTER TABLE weft_entries DROP COLUMN dismissed`,
		`ALTER TABLE weft_questions DROP COLUMN questions`, `ALTER TABLE weft_answers DROP COLUMN answers`,
		`UPDATE schema_meta SET value='7' WHERE key='schema_version'`} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	if s, err = Open(path); err != nil {
		t.Fatalf("migrate 7 to 8: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "11" {
		t.Fatalf("version = %q %v", version, err)
	}
	got, err := s.GetWeftRun(run.ID)
	if err != nil || len(got.Segments) != 1 || got.Segments[0].AgentUser.Valid {
		t.Fatalf("migrated run = %+v %v", got, err)
	}
}

func setCapacity(t *testing.T, s *Store, capacity int) error {
	t.Helper()
	revision, err := queueRevision(s.db)
	if err != nil {
		t.Fatal(err)
	}
	return s.WeftTx(func(tx *WeftTx) error { return tx.SetCapacity(capacity, revision) })
}
