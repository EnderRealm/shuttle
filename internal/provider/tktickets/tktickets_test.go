package tktickets

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/provider"
	"shuttle/internal/provider/tkconfig"
)

const project = "shuttle"

// tkHome writes a config.yaml under a temp $HOME and returns its path. HOME is
// set because pkg/ticket's mutation log is written under it; the provider reads
// state from the config's own directory, which is the same tree.
func tkHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".ticket")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "central_root: " + filepath.Join(home, "store") + "\nprojects:\n    " + project +
		":\n        path: " + filepath.Join(home, "code", project) + "\n"
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, "store", "tickets", project), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// create writes a ticket through pkg/ticket, then restamps the timestamps the
// store takes from the clock: the window under test is a fixed past day.
func create(t *testing.T, tkConfig string, tk *ticket.Ticket, updated time.Time) {
	t.Helper()
	cfg, err := tkconfig.Load(tkConfig)
	if err != nil {
		t.Fatal(err)
	}
	store := ticket.NewMultiStore(cfg.TicketsRoot())
	if err := store.Create(tk); err != nil {
		t.Fatal(err)
	}
	namespaced := tk.ID
	_, bare, _ := strings.Cut(namespaced, "/")
	tk.ID = bare
	tk.Updated = updated
	if tk.Status == ticket.StatusDone {
		tk.Completed = updated
	}
	data, err := ticket.Serialize(tk)
	if err != nil {
		t.Fatal(err)
	}
	tk.ID = namespaced
	if err := os.WriteFile(filepath.Join(cfg.TicketsRoot(), project, bare+".md"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeMutation appends one line of the log by hand: the provider reconstructs
// status changes from writes made on this machine, and the entry a real edit
// leaves is exactly this shape.
func writeMutation(t *testing.T, tkConfig string, m ticket.MutationEntry) {
	t.Helper()
	dir := tkconfig.StateDir(tkConfig, project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, mutationsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(m); err != nil {
		t.Fatal(err)
	}
}

// writeRawMutation appends a line the provider cannot parse, which is the
// class of failure skipped_lines counts.
func writeRawMutation(t *testing.T, tkConfig, line string) {
	t.Helper()
	dir := tkconfig.StateDir(tkConfig, project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, mutationsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
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

func run(t *testing.T, tkConfig string) (document, []byte) {
	t.Helper()
	env := provider.Env{TKConfig: tkConfig, Logger: log.New(&bytes.Buffer{}, "", 0)}
	data, err := Run(context.Background(), env, nil, testWindow(t))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return doc, data
}

func TestTicketsUpdatedInTheWindow(t *testing.T) {
	w := testWindow(t)
	inside := w.From.Add(11 * time.Hour)
	tkConfig := tkHome(t)
	create(t, tkConfig, &ticket.Ticket{
		ID:       project + "/input-providers-0c4e",
		Status:   ticket.StatusDone,
		Type:     ticket.TypeFeature,
		Priority: 2,
		Title:    "Input providers over loom, tk and git",
		Notes: []ticket.Note{
			{Timestamp: inside, Text: "the six providers landed"},
			{Timestamp: w.From.Add(-24 * time.Hour), Text: "groomed the day before"},
		},
	}, inside)
	create(t, tkConfig, &ticket.Ticket{
		ID:       project + "/untouched-1111",
		Status:   ticket.StatusReady,
		Type:     ticket.TypeFeature,
		Priority: 2,
		Title:    "Not touched yesterday",
	}, w.To.Add(time.Hour))
	writeMutation(t, tkConfig, ticket.MutationEntry{
		Timestamp: inside, TicketID: "input-providers-0c4e", Operation: ticket.MutationEdit,
		Source: ticket.SourceHuman, FieldsChanged: []string{"status", "updated"},
	})
	writeMutation(t, tkConfig, ticket.MutationEntry{
		Timestamp: inside, TicketID: "input-providers-0c4e", Operation: ticket.MutationAddNote,
		Source: ticket.SourceHuman, FieldsChanged: []string{"notes"},
	})
	writeMutation(t, tkConfig, ticket.MutationEntry{
		Timestamp: w.To, TicketID: "untouched-1111", Operation: ticket.MutationEdit,
		Source: ticket.SourceHuman, FieldsChanged: []string{"status"},
	})
	writeRawMutation(t, tkConfig, "{not a mutation entry")

	doc, raw := run(t, tkConfig)
	if len(doc.Tickets) != 1 {
		t.Fatalf("tickets = %+v, want only the one updated in the window", doc.Tickets)
	}
	got := doc.Tickets[0]
	if got.ID != project+"/input-providers-0c4e" || got.Project != project || got.Status != "done" ||
		got.Type != "feature" || got.Priority != 2 || got.Completed == "" {
		t.Fatalf("ticket = %+v, want the store's fields", got)
	}
	if len(got.Notes) != 1 || got.Notes[0].Text != "the six providers landed" {
		t.Fatalf("notes = %+v, want only the one added in the window", got.Notes)
	}
	if len(doc.StatusChanges) != 1 {
		t.Fatalf("status_changes = %+v, want only the in-window status mutation", doc.StatusChanges)
	}
	change := doc.StatusChanges[0]
	if change.Ticket != project+"/input-providers-0c4e" || change.Source != ticket.SourceHuman || change.Status != "done" {
		t.Fatalf("status change = %+v, want it paired with the current status", change)
	}
	// A line the provider could not read is a status change the day may be
	// missing, so the document says so rather than only the log.
	if doc.SkippedLines != 1 {
		t.Fatalf("skipped_lines = %d, want the one malformed line", doc.SkippedLines)
	}
	// The limitation is what the model reads, so it has to be in the document.
	if !strings.Contains(string(raw), "mutations.jsonl records only writes made on this machine") {
		t.Fatalf("document does not name the mutations.jsonl limitation:\n%s", raw)
	}
}

func TestEmptyWindowEmitsAnEmptyDocument(t *testing.T) {
	tkConfig := tkHome(t)
	doc, raw := run(t, tkConfig)
	if len(doc.Tickets) != 0 || len(doc.StatusChanges) != 0 || len(doc.Skips) != 0 {
		t.Fatalf("document = %+v, want it empty", doc)
	}
	for _, key := range []string{`"tickets":[]`, `"status_changes":[]`, `"skips":[]`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("data = %s, want %s", raw, key)
		}
	}
	if doc.Limitations == "" {
		t.Error("an empty day still carries the limitation")
	}
}

// A file the store could not read is reported rather than swallowed (§8.4).
func TestUnreadableFilesAreReportedAsSkips(t *testing.T) {
	tkConfig := tkHome(t)
	cfg, err := tkconfig.Load(tkConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.TicketsRoot(), project, "broken.md"), []byte("not a ticket"), 0o644); err != nil {
		t.Fatal(err)
	}
	doc, _ := run(t, tkConfig)
	if len(doc.Skips) != 1 || doc.Skips[0].File != "broken.md" {
		t.Fatalf("skips = %+v, want the unreadable file", doc.Skips)
	}
}
