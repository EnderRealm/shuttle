// Package loomsessions reports the previous day's agent sessions from loom's
// summary database (§8.2).
package loomsessions

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"shuttle/internal/provider"
)

const (
	dbFile = "summaries.db"

	// minSchema is the oldest schema this provider reads. loom's v4 added the
	// commits table; a v3 database has no column to read them from, so the
	// provider refuses rather than reporting a day with no commits in it. A
	// later version is read as long as every column in readColumns survives.
	minSchema = 4

	// parentColumn marks a subagent's session (loom v8 and later). Older
	// databases lack it, and every row in them is a top-level session.
	parentColumn = "parent_session_id"

	// intentRunes bounds the session intent. The first user message is enough
	// for the model to say what a session was for, and the bundle has a 40 KB
	// budget (§7.3).
	intentRunes = 300

	unknownRepo = "unknown"
)

// readColumns is every column this provider reads, checked by name because
// loom migrates its schema without this provider's knowledge.
var readColumns = []struct {
	table   string
	columns []string
}{
	{"sessions", []string{"agent", "session_id", "project", "cwd", "cwd_raw", "git_remote", "start_time",
		"turn_count", "tool_call_count", "error_count", "input_tokens", "output_tokens", "cache_read_tokens"}},
	{"turns", []string{"agent", "session_id", "idx", "user_message"}},
	{"commits", []string{"committed_at", "git_remote", "cwd", "commit_hash", "branch", "subject"}},
}

// unavailable is the document a missing database yields (§8.2): the run
// continues, because a summary that says loom was unavailable is better than
// no summary.
var unavailable = json.RawMessage(`{"available":false}`)

type document struct {
	Available bool   `json:"available"`
	Repos     []repo `json:"repos"`
}

type repo struct {
	Repo        string    `json:"repo"`
	Sessions    int       `json:"sessions"`
	Turns       int       `json:"turns"`
	ToolCalls   int       `json:"tool_calls"`
	Errors      int       `json:"errors"`
	Tokens      tokens    `json:"tokens"`
	Commits     []commit  `json:"commits"`
	SessionList []session `json:"session_list"`
}

type tokens struct {
	Input     int `json:"input"`
	Output    int `json:"output"`
	CacheRead int `json:"cache_read"`
}

type commit struct {
	Hash        string `json:"hash"`
	Subject     string `json:"subject"`
	Branch      string `json:"branch"`
	CommittedAt string `json:"committed_at"`
}

type session struct {
	Agent     string `json:"agent"`
	SessionID string `json:"session_id"`
	StartTime string `json:"start_time"`
	Intent    string `json:"intent"`
}

// Run implements provider.Func.
func Run(ctx context.Context, env provider.Env, _ map[string]any, w provider.Window) (json.RawMessage, error) {
	path := filepath.Join(env.LoomHome, dbFile)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return unavailable, nil
		}
		return nil, err
	}
	db, err := sql.Open("sqlite", readDSN(path))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	hasParent, err := checkSchema(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	repos := map[string]*repo{}
	if err := readSessions(ctx, db, w, hasParent, repos); err != nil {
		return nil, err
	}
	if err := readCommits(ctx, db, w, repos); err != nil {
		return nil, err
	}

	doc := document{Available: true, Repos: make([]repo, 0, len(repos))}
	for _, r := range repos {
		doc.Repos = append(doc.Repos, *r)
	}
	slices.SortFunc(doc.Repos, func(a, b repo) int { return strings.Compare(a.Repo, b.Repo) })
	return json.Marshal(doc)
}

// readDSN is the DSN loom's own read paths use: mode=ro so this reader cannot
// write or create the file, and a busy timeout so a `loom summarize` holding
// the write lock delays the read rather than failing the run.
func readDSN(path string) string {
	return fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(2000)", path)
}

// Check requires the configured database and the schema this provider reads.
// Unlike Run's unavailable document, preflight must name a missing requirement.
func Check(ctx context.Context, home string) error {
	db, err := sql.Open("sqlite", readDSN(filepath.Join(home, dbFile)))
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = checkSchema(ctx, db)
	return err
}

// checkSchema refuses a database older than minSchema or missing a column the
// provider reads, and reports whether sessions carries parentColumn.
func checkSchema(ctx context.Context, db *sql.DB) (bool, error) {
	var version string
	err := db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return false, errors.New("schema_meta: no schema_version")
	}
	if err != nil {
		return false, err
	}
	if n, err := strconv.Atoi(version); err != nil || n < minSchema {
		return false, fmt.Errorf("schema version %s: this provider reads version %d or later", version, minSchema)
	}
	var hasParent bool
	for _, t := range readColumns {
		have, err := tableColumns(ctx, db, t.table)
		if err != nil {
			return false, err
		}
		for _, c := range t.columns {
			if !have[c] {
				return false, fmt.Errorf("schema version %s: %s.%s is missing", version, t.table, c)
			}
		}
		if t.table == "sessions" {
			hasParent = have[parentColumn]
		}
	}
	return hasParent, nil
}

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	have := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		have[name] = true
	}
	return have, rows.Err()
}

func readSessions(ctx context.Context, db *sql.DB, w provider.Window, hasParent bool, repos map[string]*repo) error {
	parent := "NULL"
	if hasParent {
		parent = parentColumn
	}
	// The bounds narrow the scan to the index; they are widened by a second on
	// each side because RFC3339Nano is variable width — a whole second renders
	// without a fraction, and '.' sorts below 'Z', so a string comparison alone
	// mis-orders the boundary second. The window itself is applied below, to
	// the parsed instant.
	rows, err := db.QueryContext(ctx, `SELECT agent, session_id, project, cwd, cwd_raw, git_remote,
			start_time, turn_count, tool_call_count, error_count,
			input_tokens, output_tokens, cache_read_tokens, `+parent+`
		FROM sessions WHERE start_time >= ? AND start_time < ? ORDER BY start_time`,
		bound(w.From.Add(-time.Second)), bound(w.To.Add(time.Second)))
	if err != nil {
		return err
	}
	defer rows.Close()
	type key struct{ agent, id string }
	var order []key
	for rows.Next() {
		var (
			s                               session
			project, cwd, cwdRaw, gitRemote sql.NullString
			turns, toolCalls, errCount      sql.NullInt64
			inputTok, outputTok, cacheTok   sql.NullInt64
			parentID                        sql.NullString
		)
		if err := rows.Scan(&s.Agent, &s.SessionID, &project, &cwd, &cwdRaw, &gitRemote,
			&s.StartTime, &turns, &toolCalls, &errCount, &inputTok, &outputTok, &cacheTok, &parentID); err != nil {
			return err
		}
		start, err := time.Parse(time.RFC3339Nano, s.StartTime)
		if err != nil || !w.Contains(start) {
			// A row loom wrote a timestamp this provider cannot read cannot be
			// placed in the day, so it is not reported as part of it.
			continue
		}
		r := group(repos, identity(gitRemote.String, cwdRaw.String, cwd.String, project.String))
		r.Turns += int(turns.Int64)
		r.ToolCalls += int(toolCalls.Int64)
		r.Errors += int(errCount.Int64)
		r.Tokens.Input += int(inputTok.Int64)
		r.Tokens.Output += int(outputTok.Int64)
		r.Tokens.CacheRead += int(cacheTok.Int64)
		if parentID.String != "" {
			// A subagent's usage is recorded on its own row only, so it adds to
			// the totals, but it is part of its parent's session, not one more.
			continue
		}
		r.Sessions++
		r.SessionList = append(r.SessionList, s)
		order = append(order, key{s.Agent, s.SessionID})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// The intent is read per session rather than by a join: the turns table is
	// the largest in the database and (agent, session_id) is its primary key
	// prefix, so one indexed lookup per session in the window is cheap.
	intents := make(map[key]string, len(order))
	for _, k := range order {
		intent, err := firstIntent(ctx, db, k.agent, k.id)
		if err != nil {
			return err
		}
		intents[k] = intent
	}
	for _, r := range repos {
		for i := range r.SessionList {
			r.SessionList[i].Intent = intents[key{r.SessionList[i].Agent, r.SessionList[i].SessionID}]
		}
	}
	return nil
}

func firstIntent(ctx context.Context, db *sql.DB, agent, sessionID string) (string, error) {
	var message sql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT user_message FROM turns WHERE agent = ? AND session_id = ? ORDER BY idx LIMIT 1`,
		agent, sessionID).Scan(&message)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return truncate(message.String, intentRunes), nil
}

func readCommits(ctx context.Context, db *sql.DB, w provider.Window, repos map[string]*repo) error {
	// Widened and then filtered in Go for the same reason as the sessions scan.
	rows, err := db.QueryContext(ctx, `SELECT committed_at, git_remote, cwd, commit_hash, branch, subject
		FROM commits WHERE committed_at >= ? AND committed_at < ? ORDER BY committed_at`,
		bound(w.From.Add(-time.Second)), bound(w.To.Add(time.Second)))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			c                           commit
			committedAt, gitRemote, cwd sql.NullString
			branch, subject             sql.NullString
		)
		if err := rows.Scan(&committedAt, &gitRemote, &cwd, &c.Hash, &branch, &subject); err != nil {
			return err
		}
		at, err := time.Parse(time.RFC3339Nano, committedAt.String)
		if err != nil || !w.Contains(at) {
			continue
		}
		c.CommittedAt, c.Branch, c.Subject = committedAt.String, branch.String, subject.String
		// A repo whose commits landed in the window but whose session started
		// before it still gets a group: the commit is the day's work.
		r := group(repos, identity(gitRemote.String, cwd.String))
		r.Commits = append(r.Commits, c)
	}
	return rows.Err()
}

// group returns the repo bucket for a normalised identity, with the slices
// non-nil so an empty day renders as [] rather than null.
func group(repos map[string]*repo, name string) *repo {
	r, ok := repos[name]
	if !ok {
		r = &repo{Repo: name, Commits: []commit{}, SessionList: []session{}}
		repos[name] = r
	}
	return r
}

// identity applies loom's precedence: the first candidate that normalises to
// something, so an SSH and an HTTPS clone of one repo are one group. A value
// normalising to nothing — a bare "/" cwd — falls through rather than opening a
// group with no name.
func identity(candidates ...string) string {
	for _, c := range candidates {
		if name := normalizeRemote(c); name != "" {
			return name
		}
	}
	return unknownRepo
}

// normalizeRemote is a port of loom's summaries.NormalizeRemote; loom's module
// path is the bare `loom`, so it cannot be imported (§8.3).
func normalizeRemote(remote string) string {
	s := strings.TrimSpace(remote)
	s = strings.TrimSuffix(s, ".git")
	s = strings.TrimSuffix(s, "/")
	switch {
	case strings.Contains(s, "://"):
		// A URL-form remote can carry credentials in its userinfo
		// (https://user:token@host/owner/repo), and the identity is serialised
		// into the bundle the model reads, so it is built from host and path
		// alone — userinfo, query and fragment are dropped.
		u, err := url.Parse(s)
		if err != nil {
			// Nothing identifies an unparseable URL safely; the caller falls
			// through to the next candidate rather than to a partly stripped
			// string that may still carry the userinfo.
			return ""
		}
		s = u.Host + u.Path
	case isSCP(s):
		s = s[strings.LastIndex(s, "@")+1:]
		s = strings.Replace(s, ":", "/", 1)
	}
	return strings.ToLower(s)
}

// isSCP reports whether s is git's scp-style remote form, [user@]host:path —
// a colon before any slash. A filesystem path candidate is not one, so it
// falls through untouched. LastIndex of "@" is -1 for a form without userinfo,
// which leaves the whole string.
func isSCP(s string) bool {
	colon := strings.Index(s, ":")
	if colon < 0 {
		return false
	}
	slash := strings.Index(s, "/")
	return slash < 0 || slash > colon
}

// bound renders a window edge the way loom writes its timestamp columns.
func bound(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func truncate(s string, runes int) string {
	r := []rune(s)
	if len(r) <= runes {
		return s
	}
	return string(r[:runes])
}
