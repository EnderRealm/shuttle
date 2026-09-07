package loomsessions

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/provider"
)

// schemaSQL is the subset of loom's schema 4 this provider reads, copied so the
// fixture is exactly the shape the real database has (loom is unimportable).
const schemaSQL = `
CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE sessions (
    agent             TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    project           TEXT,
    cwd               TEXT,
    cwd_raw           TEXT,
    git_remote        TEXT,
    start_time        TEXT,
    end_time          TEXT,
    turn_count        INTEGER,
    tool_call_count   INTEGER,
    error_count       INTEGER,
    input_tokens      INTEGER,
    output_tokens     INTEGER,
    cache_read_tokens INTEGER,
    PRIMARY KEY (agent, session_id)
);
CREATE TABLE turns (
    agent          TEXT NOT NULL,
    session_id     TEXT NOT NULL,
    idx            INTEGER NOT NULL,
    user_message   TEXT,
    assistant_text TEXT,
    PRIMARY KEY (agent, session_id, idx)
);
CREATE TABLE commits (
    agent         TEXT NOT NULL,
    session_id    TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    committed_at  TEXT,
    git_remote    TEXT,
    cwd           TEXT,
    commit_hash   TEXT NOT NULL,
    branch        TEXT,
    subject       TEXT,
    files_changed INTEGER,
    PRIMARY KEY (agent, session_id, seq)
);
`

// stamp renders a timestamp the way loom writes one.
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// newDB builds a schema-4 summaries.db under a temp loom home and returns the
// home. version is what schema_meta records, so a test can build an outdated one.
func newDB(t *testing.T, version string, populate func(*sql.DB)) string {
	t.Helper()
	home := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(home, dbFile))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)`, version); err != nil {
		t.Fatal(err)
	}
	if populate != nil {
		populate(db)
	}
	return home
}

func testEnv(t *testing.T, home string) provider.Env {
	t.Helper()
	return provider.Env{LoomHome: home, Logger: log.New(os.Stderr, "", 0)}
}

// testWindow is the previous day of the daily agent's 2026-09-02 slot.
func testWindow(t *testing.T) provider.Window {
	t.Helper()
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	w, err := provider.Resolve(provider.WindowPreviousDay, time.Date(2026, 9, 2, 4, 30, 0, 0, loc), loc)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func run(t *testing.T, home string) document {
	t.Helper()
	data, err := Run(context.Background(), testEnv(t, home), nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return doc
}

// A missing database does not fail the run (§8.2).
func TestMissingDatabaseIsUnavailable(t *testing.T) {
	data, err := Run(context.Background(), testEnv(t, t.TempDir()), nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(data) != `{"available":false}` {
		t.Fatalf("data = %s, want {\"available\":false}", data)
	}
}

func TestSchemaOtherThanFourIsRefused(t *testing.T) {
	home := newDB(t, "3", nil)
	_, err := Run(context.Background(), testEnv(t, home), nil, testWindow(t))
	if err == nil || !strings.Contains(err.Error(), "schema version 3") {
		t.Fatalf("err = %v, want one naming the version found", err)
	}
}

// Nothing happened yesterday is a valid day (§7.1 step 3).
func TestEmptyWindowEmitsAnEmptyDocument(t *testing.T) {
	home := newDB(t, "4", nil)
	data, err := Run(context.Background(), testEnv(t, home), nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(string(data), `"repos":[]`) {
		t.Fatalf("data = %s, want an empty repos array", data)
	}
}

func TestSessionsAreGroupedByRepositoryIdentity(t *testing.T) {
	w := testWindow(t)
	inside := w.From.Add(9 * time.Hour)
	home := newDB(t, "4", func(db *sql.DB) {
		insertSession(t, db, "claude", "s1", "shuttle", "/Users/steve/code/shuttle", "", "git@github.com:steve/shuttle.git", inside, 4, 9, 1, 100, 200, 300)
		// The same repo over HTTPS: NormalizeRemote collapses the two.
		insertSession(t, db, "codex", "s2", "shuttle", "", "", "https://github.com/steve/Shuttle", inside.Add(time.Hour), 2, 3, 0, 10, 20, 30)
		// No remote at all: the identity falls through to cwd_raw.
		insertSession(t, db, "claude", "s3", "loom", "/Users/steve/code/loom", "/Users/steve/code/loom-raw", "", inside.Add(2*time.Hour), 1, 1, 0, 1, 2, 3)
		// The boundary: To belongs to the next window.
		insertSession(t, db, "claude", "s4", "shuttle", "", "", "git@github.com:steve/shuttle.git", w.To, 99, 99, 99, 99, 99, 99)
		insertSession(t, db, "claude", "s5", "shuttle", "", "", "git@github.com:steve/shuttle.git", w.From.Add(-time.Second), 99, 99, 99, 99, 99, 99)

		insertTurn(t, db, "claude", "s1", 1, "second message")
		insertTurn(t, db, "claude", "s1", 0, strings.Repeat("é", 400))
		insertCommit(t, db, "claude", "s1", 1, inside, "git@github.com:steve/shuttle.git", "", "abc123", "main", "[shuttle/x] Do the thing")
		insertCommit(t, db, "claude", "s1", 2, w.To, "git@github.com:steve/shuttle.git", "", "def456", "main", "tomorrow")
	})

	doc := run(t, home)
	if !doc.Available {
		t.Fatal("available = false, want true")
	}
	if len(doc.Repos) != 2 {
		t.Fatalf("repos = %+v, want two", doc.Repos)
	}
	shuttle := doc.Repos[1]
	if shuttle.Repo != "github.com/steve/shuttle" {
		t.Fatalf("repo = %q, want the normalised remote", shuttle.Repo)
	}
	if shuttle.Sessions != 2 || shuttle.Turns != 6 || shuttle.ToolCalls != 12 || shuttle.Errors != 1 {
		t.Errorf("counts = %+v, want the two in-window sessions summed", shuttle)
	}
	if shuttle.Tokens != (tokens{Input: 110, Output: 220, CacheRead: 330}) {
		t.Errorf("tokens = %+v, want the two in-window sessions summed", shuttle.Tokens)
	}
	if len(shuttle.Commits) != 1 || shuttle.Commits[0].Hash != "abc123" {
		t.Errorf("commits = %+v, want only the one in the window", shuttle.Commits)
	}
	intent := shuttle.SessionList[0].Intent
	if got := len([]rune(intent)); got != intentRunes {
		t.Errorf("intent = %d runes, want %d taken from the lowest-idx turn", got, intentRunes)
	}
	if doc.Repos[0].Repo != "/users/steve/code/loom-raw" {
		t.Errorf("repo = %q, want cwd_raw as the identity", doc.Repos[0].Repo)
	}
}

func insertSession(t *testing.T, db *sql.DB, agent, id, project, cwd, cwdRaw, remote string, start time.Time, turns, toolCalls, errs, in, out, cache int) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO sessions(agent, session_id, project, cwd, cwd_raw, git_remote,
			start_time, turn_count, tool_call_count, error_count, input_tokens, output_tokens, cache_read_tokens)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		agent, id, project, cwd, cwdRaw, remote, stamp(start), turns, toolCalls, errs, in, out, cache); err != nil {
		t.Fatal(err)
	}
}

func insertTurn(t *testing.T, db *sql.DB, agent, id string, idx int, message string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO turns(agent, session_id, idx, user_message) VALUES (?, ?, ?, ?)`,
		agent, id, idx, message); err != nil {
		t.Fatal(err)
	}
}

func insertCommit(t *testing.T, db *sql.DB, agent, id string, seq int, at time.Time, remote, cwd, hash, branch, subject string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO commits(agent, session_id, seq, committed_at, git_remote, cwd, commit_hash, branch, subject)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		agent, id, seq, stamp(at), remote, cwd, hash, branch, subject); err != nil {
		t.Fatal(err)
	}
}

// The precedence and the normalisation are loom's; the fallback is this
// provider's, because a group with no name tells the model nothing.
func TestIdentity(t *testing.T) {
	cases := []struct {
		candidates []string
		want       string
	}{
		{[]string{"git@github.com:steve/Shuttle.git", "/tmp/a", "/tmp/b", "shuttle"}, "github.com/steve/shuttle"},
		// A URL-form remote's userinfo is credentials, not identity.
		{[]string{"https://user:token@github.com/steve/shuttle.git"}, "github.com/steve/shuttle"},
		{[]string{"ssh://git@github.com/steve/shuttle.git"}, "github.com/steve/shuttle"},
		{[]string{"deploy@github.com:steve/shuttle.git"}, "github.com/steve/shuttle"},
		{[]string{"", "/Users/steve/code/loom", "", "loom"}, "/users/steve/code/loom"},
		{[]string{"", "", "", "shuttle"}, "shuttle"},
		{[]string{"", "", "/", "shuttle"}, "shuttle"},
		{[]string{"", "", "", ""}, unknownRepo},
	}
	for _, c := range cases {
		if got := identity(c.candidates...); got != c.want {
			t.Errorf("identity(%q) = %q, want %q", c.candidates, got, c.want)
		}
	}
}

// The identity is serialised into the bundle the model reads, so a remote's
// credentials must not survive normalisation.
func TestRemoteCredentialsAreNotEmitted(t *testing.T) {
	w := testWindow(t)
	home := newDB(t, "4", func(db *sql.DB) {
		insertSession(t, db, "claude", "s1", "shuttle", "", "", "https://user:token@github.com/steve/shuttle.git",
			w.From.Add(9*time.Hour), 1, 1, 0, 0, 0, 0)
	})
	data, err := Run(context.Background(), testEnv(t, home), nil, w)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The token itself, not the bare word: "tokens" is a field name in every
	// repo the document carries.
	for _, secret := range []string{"user:token", "user@", "token@"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("data = %s, want %q nowhere in it", data, secret)
		}
	}
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(doc.Repos) != 1 || doc.Repos[0].Repo != "github.com/steve/shuttle" {
		t.Fatalf("repos = %+v, want the host and path alone", doc.Repos)
	}
}

// loom writes RFC3339Nano, which drops a zero fraction, so the SQL bounds are
// widened and the window applied to the parsed instant: a row half a second
// inside the window is kept and one half a second past To is not.
func TestTheBoundarySecondIsFilteredOnTheParsedTimestamp(t *testing.T) {
	w := testWindow(t)
	remote := "git@github.com:steve/shuttle.git"
	home := newDB(t, "4", func(db *sql.DB) {
		insertSession(t, db, "claude", "in", "shuttle", "", "", remote, w.From.Add(500*time.Millisecond), 1, 1, 0, 0, 0, 0)
		insertSession(t, db, "claude", "out", "shuttle", "", "", remote, w.To.Add(500*time.Millisecond), 9, 9, 9, 9, 9, 9)
		insertCommit(t, db, "claude", "in", 1, w.From.Add(500*time.Millisecond), remote, "", "abc123", "main", "in")
		insertCommit(t, db, "claude", "out", 1, w.To.Add(500*time.Millisecond), remote, "", "def456", "main", "out")
	})

	doc := run(t, home)
	if len(doc.Repos) != 1 {
		t.Fatalf("repos = %+v, want one", doc.Repos)
	}
	r := doc.Repos[0]
	if r.Sessions != 1 || len(r.SessionList) != 1 || r.SessionList[0].SessionID != "in" {
		t.Errorf("sessions = %+v, want only the one inside the window", r.SessionList)
	}
	if len(r.Commits) != 1 || r.Commits[0].Hash != "abc123" {
		t.Errorf("commits = %+v, want only the one inside the window", r.Commits)
	}
}
