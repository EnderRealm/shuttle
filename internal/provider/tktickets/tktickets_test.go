package tktickets

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"slices"
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
	tk.Updated = updated
	if tk.Status == ticket.StatusDone {
		tk.Completed = updated
	}
	write(t, tkConfig, tk)
}

// write puts a ticket file in its namespace's directory exactly as tk stores
// it — bare ID, every timestamp as given — without going through a store
// write, so a fixture can hold what a sync from another machine leaves: a
// child completed or reopened with no write to its epic, or a Root ticket on
// a machine where Root has no repository.
func write(t *testing.T, tkConfig string, tk *ticket.Ticket) {
	t.Helper()
	cfg, err := tkconfig.Load(tkConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespaced := tk.ID
	ns, bare := ticket.ParseNamespacedID(namespaced)
	if tk.Created.IsZero() {
		tk.Created = tk.Updated
	}
	tk.ID = bare
	data, err := ticket.Serialize(tk)
	tk.ID = namespaced
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(cfg.TicketsRoot(), ns)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, bare+".md"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// activate writes the catalog that turns on Root and cross-project parents
// for Root and the named namespaces, with each one's directory and marker, as
// tk's activation leaves the central store.
func activate(t *testing.T, tkConfig string, namespaces ...string) {
	t.Helper()
	cfg, err := tkconfig.Load(tkConfig)
	if err != nil {
		t.Fatal(err)
	}
	body := "required_features: [" + ticket.FeatureRootNamespace + ", " + ticket.FeatureCrossProjectParents + "]\nnamespaces:\n"
	for _, ns := range append([]string{tkconfig.RootNamespace}, namespaces...) {
		kind := ticket.KindProject
		if ns == tkconfig.RootNamespace {
			kind = ticket.KindRoot
		}
		body += "  " + ns + ": {kind: " + string(kind) + "}\n"
		if err := ticket.WriteNamespaceMarker(cfg.CentralRoot, ns); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(ticket.CatalogPath(cfg.CentralRoot), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeMutation appends one line of a namespace's log by hand: the provider
// reconstructs status changes from writes made on this machine, and the entry
// a real edit leaves is exactly this shape.
func writeMutation(t *testing.T, tkConfig, ns string, m ticket.MutationEntry) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	writeRawMutation(t, tkConfig, ns, string(data))
}

// writeRawMutation appends a line as given, which is how a line the provider
// cannot parse — the class of failure skipped_lines counts — gets there.
func writeRawMutation(t *testing.T, tkConfig, ns, line string) {
	t.Helper()
	dir := tkconfig.StateDir(tkConfig, ns)
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
	writeMutation(t, tkConfig, project, ticket.MutationEntry{
		Timestamp: inside, TicketID: "input-providers-0c4e", Operation: ticket.MutationEdit,
		Source: ticket.SourceHuman, FieldsChanged: []string{"status", "updated"},
	})
	writeMutation(t, tkConfig, project, ticket.MutationEntry{
		Timestamp: inside, TicketID: "input-providers-0c4e", Operation: ticket.MutationAddNote,
		Source: ticket.SourceHuman, FieldsChanged: []string{"notes"},
	})
	writeMutation(t, tkConfig, project, ticket.MutationEntry{
		Timestamp: w.To, TicketID: "untouched-1111", Operation: ticket.MutationEdit,
		Source: ticket.SourceHuman, FieldsChanged: []string{"status"},
	})
	writeRawMutation(t, tkConfig, project, "{not a mutation entry")

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

// register appends a project to config.yaml; an empty path is a project
// registered on this machine with no repository behind it.
func register(t *testing.T, tkConfig, name, path string) {
	t.Helper()
	f, err := os.OpenFile(tkConfig, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("    " + name + ":\n        path: \"" + path + "\"\n"); err != nil {
		t.Fatal(err)
	}
}

func ids(entries []entry) []string {
	out := []string{}
	for _, e := range entries {
		out = append(out, e.ID)
	}
	return out
}

func find(t *testing.T, entries []entry, id string) entry {
	t.Helper()
	for _, e := range entries {
		if e.ID == id {
			return e
		}
	}
	t.Fatalf("tickets = %v, want %s among them", ids(entries), id)
	return entry{}
}

// Root holds tickets and a mutation log but no repository: config.yaml never
// names it, and its activity still reaches the document.
func TestRootActivityIsReportedWithoutARepository(t *testing.T) {
	w := testWindow(t)
	inside := w.From.Add(9 * time.Hour)
	tkConfig := tkHome(t)
	activate(t, tkConfig, project)
	rootID := tkconfig.RootNamespace + "/unified-work-ab12"
	write(t, tkConfig, &ticket.Ticket{
		ID: rootID, Status: ticket.StatusDone, Type: ticket.TypeFeature, Priority: 2,
		Title: "Decide the unified work model", Updated: inside, Completed: inside,
		Notes: []ticket.Note{{Timestamp: inside, Text: "decided: one central graph"}},
	})
	writeMutation(t, tkConfig, tkconfig.RootNamespace, ticket.MutationEntry{
		Timestamp: inside, TicketID: "unified-work-ab12", Operation: ticket.MutationEdit,
		Source: ticket.SourceHuman, FieldsChanged: []string{"status", "updated"},
	})

	doc, _ := run(t, tkConfig)
	if !doc.Complete || len(doc.Diagnostics) != 0 {
		t.Fatalf("complete = %v, diagnostics = %v, want a complete read", doc.Complete, doc.Diagnostics)
	}
	if !slices.Contains(doc.Namespaces, tkconfig.RootNamespace) {
		t.Fatalf("namespaces = %v, want Root among them", doc.Namespaces)
	}
	got := find(t, doc.Tickets, rootID)
	if got.Project != tkconfig.RootNamespace || got.Status != "done" || got.Completed == "" || len(got.Notes) != 1 {
		t.Fatalf("root ticket = %+v, want the store's fields", got)
	}
	if len(doc.StatusChanges) != 1 || doc.StatusChanges[0].Ticket != rootID || doc.StatusChanges[0].Status != "done" {
		t.Fatalf("status_changes = %+v, want Root's logged change paired with its status", doc.StatusChanges)
	}
}

// A derived epic transition writes nothing to the epic, so an epic in one
// project whose only change is a child in another completing, or reopening,
// is found from the child and reported with the snapshot's global status and
// completion time.
func TestEpicChangedOnlyByAForeignChild(t *testing.T) {
	w := testWindow(t)
	before := w.From.Add(-48 * time.Hour)
	inside := w.From.Add(14 * time.Hour)
	epicID := project + "/rollout-e1e1"
	for _, test := range []struct {
		name          string
		child         ticket.Status
		wantStatus    string
		wantCompleted string
	}{
		{name: "completion", child: ticket.StatusDone, wantStatus: "done", wantCompleted: inside.UTC().Format(time.RFC3339)},
		{name: "reopening", child: ticket.StatusOpen, wantStatus: "open"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tkConfig := tkHome(t)
			activate(t, tkConfig, project, "loom")
			// The epic's file still reads done from before the window: nothing
			// wrote it when its foreign child moved.
			write(t, tkConfig, &ticket.Ticket{
				ID: epicID, Status: ticket.StatusDone, Type: ticket.TypeEpic, Priority: 1,
				Title: "Roll out the central graph", Updated: before, Completed: before,
			})
			write(t, tkConfig, &ticket.Ticket{
				ID: project + "/local-c2c2", Status: ticket.StatusDone, Type: ticket.TypeFeature, Priority: 2,
				Title: "Local half", Parent: epicID, Updated: before, Completed: before,
			})
			foreign := &ticket.Ticket{
				ID: "loom/remote-c1c1", Status: test.child, Type: ticket.TypeFeature, Priority: 2,
				Title: "Foreign half", Parent: epicID, Updated: inside,
			}
			if test.child == ticket.StatusDone {
				foreign.Completed = inside
			}
			write(t, tkConfig, foreign)
			// An epic none of whose children moved stays out of the document.
			write(t, tkConfig, &ticket.Ticket{
				ID: project + "/quiet-e3e3", Status: ticket.StatusOpen, Type: ticket.TypeEpic, Priority: 2,
				Title: "Untouched epic", Updated: before,
			})
			write(t, tkConfig, &ticket.Ticket{
				ID: "loom/quiet-c3c3", Status: ticket.StatusOpen, Type: ticket.TypeFeature, Priority: 2,
				Title: "Untouched child", Parent: project + "/quiet-e3e3", Updated: before,
			})

			doc, _ := run(t, tkConfig)
			if !slices.Equal(ids(doc.Tickets), []string{"loom/remote-c1c1", epicID}) {
				t.Fatalf("tickets = %v, want the foreign child and the epic it changed", ids(doc.Tickets))
			}
			epic := find(t, doc.Tickets, epicID)
			if !slices.Equal(epic.ChangedBy, []string{"loom/remote-c1c1"}) {
				t.Fatalf("changed_by = %v, want the foreign child", epic.ChangedBy)
			}
			if epic.Status != test.wantStatus || epic.Completed != test.wantCompleted {
				t.Fatalf("epic status = %q, completed = %q; want %q, %q derived from every child",
					epic.Status, epic.Completed, test.wantStatus, test.wantCompleted)
			}
		})
	}
}

// A child changed in the window is found through the mutation log as well as
// its updated time: a status change logged on this machine names the epic.
func TestEpicChangedByAChildsLoggedStatusChange(t *testing.T) {
	w := testWindow(t)
	before := w.From.Add(-48 * time.Hour)
	tkConfig := tkHome(t)
	activate(t, tkConfig, project, "loom")
	epicID := project + "/rollout-e1e1"
	write(t, tkConfig, &ticket.Ticket{
		ID: epicID, Status: ticket.StatusOpen, Type: ticket.TypeEpic, Priority: 1, Title: "Epic", Updated: before,
	})
	write(t, tkConfig, &ticket.Ticket{
		ID: "loom/remote-c1c1", Status: ticket.StatusOpen, Type: ticket.TypeFeature, Priority: 2,
		Title: "Child", Parent: epicID, Updated: before,
	})
	writeMutation(t, tkConfig, "loom", ticket.MutationEntry{
		Timestamp: w.From.Add(time.Hour), TicketID: "remote-c1c1", Operation: ticket.MutationEdit,
		Source: ticket.SourceHuman, FieldsChanged: []string{"status"},
	})
	doc, _ := run(t, tkConfig)
	epic := find(t, doc.Tickets, epicID)
	if !slices.Equal(epic.ChangedBy, []string{"loom/remote-c1c1"}) {
		t.Fatalf("changed_by = %v, want the child whose status change was logged", epic.ChangedBy)
	}
}

// A namespace the catalog expects but the store does not hold is reported as
// the incompleteness it is, and no epic certifies done off that read.
func TestIncompleteReadsAreReportedAndDegradeEpics(t *testing.T) {
	w := testWindow(t)
	inside := w.From.Add(3 * time.Hour)
	tkConfig := tkHome(t)
	activate(t, tkConfig, project, "ghost")
	cfg, err := tkconfig.Load(tkConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(cfg.TicketsRoot(), "ghost")); err != nil {
		t.Fatal(err)
	}
	epicID := project + "/rollout-e1e1"
	write(t, tkConfig, &ticket.Ticket{
		ID: epicID, Status: ticket.StatusDone, Type: ticket.TypeEpic, Priority: 1, Title: "Epic", Updated: inside, Completed: inside,
	})
	write(t, tkConfig, &ticket.Ticket{
		ID: project + "/only-c1c1", Status: ticket.StatusDone, Type: ticket.TypeFeature, Priority: 2,
		Title: "Child", Parent: epicID, Updated: inside, Completed: inside,
	})

	doc, raw := run(t, tkConfig)
	if doc.Complete {
		t.Fatalf("complete = true, want false with a catalogued namespace missing:\n%s", raw)
	}
	if len(doc.Diagnostics) == 0 || !strings.Contains(strings.Join(doc.Diagnostics, "\n"), `"ghost"`) {
		t.Fatalf("diagnostics = %v, want the missing namespace named", doc.Diagnostics)
	}
	if len(doc.Skips) != 1 || doc.Skips[0].Kind != ticket.FileSkipNamespace || doc.Skips[0].Project != "ghost" {
		t.Fatalf("skips = %+v, want the namespace skip", doc.Skips)
	}
	if slices.Contains(doc.Namespaces, "ghost") {
		t.Fatalf("namespaces = %v, want only the ones read in full", doc.Namespaces)
	}
	if epic := find(t, doc.Tickets, epicID); epic.Status == "done" || epic.Completed != "" {
		t.Fatalf("epic = %+v, want it not certified done off an incomplete read", epic)
	}
}

// One bare ID in three namespaces is three tickets, and each namespace's log
// is read once — the ones config names, the ones the store holds, and Root,
// even when config wrongly registers it — so no change is lost or counted
// twice.
func TestMixedNamespacesKeepExactIdentities(t *testing.T) {
	w := testWindow(t)
	inside := w.From.Add(6 * time.Hour)
	tkConfig := tkHome(t)
	register(t, tkConfig, "loom", "")
	register(t, tkConfig, tkconfig.RootNamespace, "/Users/steve/code/root")
	activate(t, tkConfig, project, "loom")
	statuses := map[string]ticket.Status{
		project: ticket.StatusDone, "loom": ticket.StatusOpen, tkconfig.RootNamespace: ticket.StatusReady,
	}
	for ns, status := range statuses {
		tk := &ticket.Ticket{
			ID: ns + "/same-0001", Status: status, Type: ticket.TypeFeature, Priority: 2, Title: "Same bare ID in " + ns, Updated: inside,
		}
		if status == ticket.StatusDone {
			tk.Completed = inside
		}
		write(t, tkConfig, tk)
		writeMutation(t, tkConfig, ns, ticket.MutationEntry{
			Timestamp: inside, TicketID: "same-0001", Operation: ticket.MutationEdit,
			Source: ticket.SourceHuman, FieldsChanged: []string{"status"},
		})
	}

	doc, _ := run(t, tkConfig)
	want := []string{tkconfig.RootNamespace + "/same-0001", "loom/same-0001", project + "/same-0001"}
	if !slices.Equal(ids(doc.Tickets), want) {
		t.Fatalf("tickets = %v, want %v", ids(doc.Tickets), want)
	}
	for _, e := range doc.Tickets {
		ns, _ := ticket.ParseNamespacedID(e.ID)
		if e.Project != ns || e.Status != string(statuses[ns]) {
			t.Fatalf("ticket = %+v, want project %s and status %s", e, ns, statuses[ns])
		}
	}
	if len(doc.StatusChanges) != len(want) {
		t.Fatalf("status_changes = %+v, want exactly one per namespace", doc.StatusChanges)
	}
	seen := map[string]bool{}
	for _, c := range doc.StatusChanges {
		ns, _ := ticket.ParseNamespacedID(c.Ticket)
		if seen[c.Ticket] || c.Status != string(statuses[ns]) {
			t.Fatalf("status_changes = %+v, want each qualified ID once, paired with its own namespace's status", doc.StatusChanges)
		}
		seen[c.Ticket] = true
	}
}
