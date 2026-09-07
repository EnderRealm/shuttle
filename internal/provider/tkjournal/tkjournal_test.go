package tkjournal

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/journal"

	"shuttle/internal/provider"
	"shuttle/internal/provider/tkconfig"
)

// tkHome writes a config.yaml naming one project and returns its path. State
// is read from the config's own directory, so the fixture is self-contained
// and TK_STORE_ROOT is never involved (§8.4).
func tkHome(t *testing.T, projects ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), ".ticket")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "central_root: " + filepath.Join(dir, "store") + "\nprojects:\n"
	for _, p := range projects {
		body += "    " + p + ":\n        path: /Users/steve/code/" + p + "\n"
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeJournal(t *testing.T, tkConfig, project string, lines ...string) {
	t.Helper()
	dir := tkconfig.StateDir(tkConfig, project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := ""
	for _, l := range lines {
		body += l + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, journalFile), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func marshal(t *testing.T, e journal.Entry) string {
	t.Helper()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

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

func run(t *testing.T, tkConfig string, logs *bytes.Buffer) document {
	t.Helper()
	env := provider.Env{TKConfig: tkConfig, Logger: log.New(logs, "", 0)}
	data, err := Run(context.Background(), env, nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return doc
}

func TestEntriesInTheWindowAcrossProjects(t *testing.T) {
	w := testWindow(t)
	tkConfig := tkHome(t, "shuttle", "loom")
	inside := w.From.Add(10 * time.Hour)
	writeJournal(t, tkConfig, "shuttle",
		marshal(t, journal.Entry{SHA: "aaa", Ticket: "shuttle/x-1", Repo: "shuttle", TS: inside.Format(time.RFC3339),
			Msg: "[shuttle/x-1] Do the thing", Author: "steve", Action: "close",
			FilesChanged: []string{"a.go"}, LinesAdded: 12, LinesRemoved: 3, Branch: "main", DurationSecs: 420}),
		// The boundary: an entry at To belongs to the next window.
		marshal(t, journal.Entry{SHA: "bbb", TS: w.To.Format(time.RFC3339), Action: "link"}),
		marshal(t, journal.Entry{SHA: "ccc", TS: w.From.Add(-time.Second).Format(time.RFC3339), Action: "link"}),
	)
	writeJournal(t, tkConfig, "loom",
		marshal(t, journal.Entry{SHA: "ddd", Ticket: "loom/y-2", TS: inside.Format(time.RFC3339), Action: "link"}),
	)

	doc := run(t, tkConfig, &bytes.Buffer{})
	if len(doc.Entries) != 2 {
		t.Fatalf("entries = %+v, want the two in the window", doc.Entries)
	}
	// Projects are walked in name order, so loom comes before shuttle.
	if doc.Entries[0].Project != "loom" || doc.Entries[1].Project != "shuttle" {
		t.Fatalf("projects = %q, %q, want them in name order", doc.Entries[0].Project, doc.Entries[1].Project)
	}
	got := doc.Entries[1]
	if got.SHA != "aaa" || got.Message != "[shuttle/x-1] Do the thing" || got.Action != "close" ||
		got.LinesAdded != 12 || got.LinesRemoved != 3 || got.Branch != "main" || got.DurationSeconds != 420 {
		t.Fatalf("entry = %+v, want the journal's fields", got)
	}
}

// config.yaml lists projects whose journal has never been written; that is an
// empty project, not a failure.
func TestEmptyWindowEmitsAnEmptyDocument(t *testing.T) {
	tkConfig := tkHome(t, "shuttle")
	data, err := Run(context.Background(), provider.Env{TKConfig: tkConfig, Logger: log.New(&bytes.Buffer{}, "", 0)}, nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(data) != `{"entries":[],"skipped_lines":0}` {
		t.Fatalf("data = %s, want an empty document", data)
	}
}

func TestMalformedLinesAreLoggedSkippedAndCounted(t *testing.T) {
	w := testWindow(t)
	tkConfig := tkHome(t, "shuttle")
	writeJournal(t, tkConfig, "shuttle",
		"{not json",
		marshal(t, journal.Entry{SHA: "aaa", TS: "yesterday", Action: "link"}),
		marshal(t, journal.Entry{SHA: "bbb", TS: w.From.Add(time.Hour).Format(time.RFC3339), Action: "link"}),
	)
	var logs bytes.Buffer
	doc := run(t, tkConfig, &logs)
	if len(doc.Entries) != 1 || doc.Entries[0].SHA != "bbb" {
		t.Fatalf("entries = %+v, want only the usable one", doc.Entries)
	}
	if doc.SkippedLines != 2 {
		t.Fatalf("skipped_lines = %d, want 2", doc.SkippedLines)
	}
	if logs.Len() == 0 {
		t.Fatal("the skipped lines were not logged")
	}
}
