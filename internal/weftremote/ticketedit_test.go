package weftremote

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/sandbox"
)

// newEditStudio is the fixtures' worked example with tk's lock and mutation
// log kept under the test's root rather than the user's home.
func newEditStudio(t *testing.T) (*studio, []fixtureTicket) {
	t.Helper()
	s := newStudio(t)
	t.Setenv("HOME", s.root)
	tickets := s.seedFixtureCatalog()
	s.seedFixtureRuns(tickets)
	s.sync()
	return s, tickets
}

func (s *studio) edit(id, key, body string) response {
	s.t.Helper()
	rec := s.request(s.t, http.MethodPost, "/v2/tickets/"+id+"/edit", key, body)
	doc, _ := decode(s.t, rec.Body.Bytes()).(map[string]any)
	return response{code: rec.Code, replayed: rec.Header().Get("Idempotent-Replayed") == "true", body: doc, raw: rec.Body.String()}
}

func editBody(status, priority any, readStatus string, readPriority any) string {
	doc, _ := json.Marshal(map[string]any{
		"contract_version": ServiceVersion, "status": status, "priority": priority,
		"read_status": readStatus, "read_priority": readPriority,
	})
	return string(doc)
}

func (s *studio) ticketFile(id string) []byte {
	s.t.Helper()
	project, slug, _ := strings.Cut(id, "/")
	doc, err := os.ReadFile(filepath.Join(s.central, "tickets", project, slug+".md"))
	if err != nil {
		s.t.Fatal(err)
	}
	return doc
}

func (s *studio) asOf() int64 {
	s.t.Helper()
	seq, err := s.store.WeftAsOf()
	if err != nil {
		s.t.Fatal(err)
	}
	return seq
}

func editResponseShape(t *testing.T) *shape {
	return learn(decode(t, fixtureBytes(t, "ticket-edit-response.json")), decode(t, fixtureBytes(t, "ticket.json")))
}

// nextCatalog reads the stream to its next catalog event, held to the shape
// events-ticket-edit.sse gives it: the project and its revision, no ticket.
func nextCatalog(t *testing.T, st *stream) map[string]any {
	t.Helper()
	for {
		ev := st.next(t)
		if ev.name != "catalog" {
			continue
		}
		doc := decode(t, []byte(ev.data)).(map[string]any)
		learn(fixtureEvents(t, "events-ticket-edit.sse")["catalog"]...).check(t, "catalog", doc)
		if doc["seq"] != decode(t, []byte(ev.id)) {
			t.Fatalf("catalog event id %s carries seq %v", ev.id, doc["seq"])
		}
		return doc
	}
}

func TestTicketEditWritesStatusAndPriority(t *testing.T) {
	s, _ := newEditStudio(t)
	server := httptest.NewServer(s.service)
	// A cleanup, so the stream's own cleanup closes it first.
	t.Cleanup(server.Close)
	events := openStream(t, server, "")
	events.next(t)

	// The fixture's ticket, with the description ticket-edit-response.json
	// shows it reading.
	var want map[string]any
	fixtureJSON(t, "ticket-edit-response.json", &want)
	wantTicket := want["ticket"].(map[string]any)
	idea := wantTicket["id"].(string)
	updated := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	writeTicketIn(t, s.central, "weft", &ticket.Ticket{
		ID: bare(idea), Title: wantTicket["title"].(string), Status: ticket.StatusBacklog, Type: ticket.TypeFeature,
		Body: wantTicket["description"].(string) + "\n", Created: updated, Updated: updated,
	})
	s.sync()
	revision := s.revision("weft")
	nextCatalog(t, events)

	// tk holds a priority on every ticket, so this host serves the one tk
	// reads and the edit carries it back; ticket-edit-request.json is
	// otherwise sent as it stands.
	before := s.get("/v2/tickets/" + idea)["ticket"].(map[string]any)
	var request map[string]any
	fixtureJSON(t, "ticket-edit-request.json", &request)
	if before["status"] != request["read_status"] {
		t.Fatalf("%s reads %v, the fixture request read %v", idea, before["status"], request["read_status"])
	}
	request["read_priority"] = before["priority"]
	body, _ := json.Marshal(request)
	r := s.edit(idea, "edit-idea", string(body))
	r.expect(t, http.StatusOK, "ticket-edit-response.json", "ticket.json")
	editResponseShape(t).check(t, "edit", r.body)
	got := r.body["ticket"].(map[string]any)
	for key, value := range wantTicket {
		if key == "updated" || key == "priority" {
			continue
		}
		if !reflect.DeepEqual(got[key], value) {
			t.Errorf("edited %s.%s = %v, ticket-edit-response.json has %v", idea, key, got[key], value)
		}
	}
	if got["priority"] != before["priority"] {
		t.Errorf("a status edit moved priority %v to %v", before["priority"], got["priority"])
	}
	if after := s.get("/v2/tickets/" + idea)["ticket"]; !reflect.DeepEqual(after, got) {
		t.Errorf("GET after the edit = %v, the edit returned %v", after, got)
	}
	event := nextCatalog(t, events)
	if event["project"] != "weft" || int64(event["catalog_revision"].(float64)) <= revision ||
		r.body["as_of"].(float64) < event["seq"].(float64) {
		t.Fatalf("catalog event %v after revision %d, edit as_of %v", event, revision, r.body["as_of"])
	}
	if again := s.edit(idea, "edit-idea", string(body)); !again.replayed || again.raw != r.raw {
		t.Fatalf("replay = %v %s", again.replayed, again.raw)
	}

	for _, c := range []struct {
		name, id          string
		status            any
		priority          any
		readStatus        string
		readPriority      int
		wantStatus        string
		wantPriority      float64
		eligibilityBefore string
	}{
		{"priority", "weft/example-ready-a0b1", nil, 1, "ready", 2, "ready", 1, "eligible"},
		{"both", "weft/example-uncriteria-3b9d", "open", 0, "ready", 3, "open", 0, "ineligible"},
	} {
		revision := s.revision("weft")
		r := s.edit(c.id, "edit-"+c.name, editBody(c.status, c.priority, c.readStatus, c.readPriority))
		r.expect(t, http.StatusOK, "ticket-edit-response.json", "ticket.json")
		editResponseShape(t).check(t, c.name, r.body)
		got := r.body["ticket"].(map[string]any)
		if got["status"] != c.wantStatus || got["priority"] != c.wantPriority {
			t.Errorf("%s edit of %s = %v %v, want %s %v", c.name, c.id, got["status"], got["priority"], c.wantStatus, c.wantPriority)
		}
		if after := s.get("/v2/tickets/" + c.id)["ticket"]; !reflect.DeepEqual(after, got) {
			t.Errorf("%s: GET after the edit = %v, the edit returned %v", c.name, after, got)
		}
		event := nextCatalog(t, events)
		if event["project"] != "weft" || int64(event["catalog_revision"].(float64)) <= revision ||
			r.body["as_of"].(float64) < event["seq"].(float64) {
			t.Fatalf("%s: catalog event %v after revision %d, edit as_of %v", c.name, event, revision, r.body["as_of"])
		}
	}

	// A priority equal to the current one changes nothing: the edit answers
	// with the ticket, writes nothing and is no catalog event.
	unchanged := "weft/example-ready-a0b1"
	file, seq := s.ticketFile(unchanged), s.asOf()
	r = s.edit(unchanged, "edit-noop", editBody(nil, 1, "ready", 1))
	r.expect(t, http.StatusOK, "ticket-edit-response.json", "ticket.json")
	if got := r.body["ticket"].(map[string]any); got["priority"] != float64(1) || got["status"] != "ready" {
		t.Errorf("no-op edit = %v", got)
	}
	if string(s.ticketFile(unchanged)) != string(file) || s.asOf() != seq || r.body["as_of"] != float64(seq) {
		t.Fatalf("a no-op edit wrote: file changed %v, stream %d -> %d, as_of %v",
			string(s.ticketFile(unchanged)) != string(file), seq, s.asOf(), r.body["as_of"])
	}

	// An epic's one settable status is closed, which tk records as an
	// abandon; its priority is edited like any ticket's.
	created := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	writeTicketIn(t, s.central, "loom", &ticket.Ticket{
		ID: "container-0001", Status: ticket.StatusOpen, Type: ticket.TypeEpic, Priority: 2, Title: "Container",
		Created: created, Updated: created,
	})
	epic := s.get("/v2/tickets/loom/container-0001")["ticket"].(map[string]any)
	r = s.edit("loom/container-0001", "edit-epic-priority", editBody(nil, 4, epic["status"].(string), 2))
	r.expect(t, http.StatusOK, "ticket-edit-response.json", "ticket.json")
	r = s.edit("loom/container-0001", "edit-epic-close", editBody("closed", nil, epic["status"].(string), 4))
	r.expect(t, http.StatusOK, "ticket-edit-response.json", "ticket.json")
	if got := r.body["ticket"].(map[string]any); got["status"] != "closed" || got["priority"] != float64(4) {
		t.Errorf("epic edit = %v", got)
	}
	stored, err := ticket.NewMultiStore(filepath.Join(s.central, "tickets")).Get("loom/container-0001")
	if err != nil || !stored.Abandoned || stored.Status != ticket.StatusClosed {
		t.Fatalf("epic as tk reads it = %+v (%v)", stored, err)
	}
}

func TestTicketEditRefusalsWriteNothing(t *testing.T) {
	s, _ := newEditStudio(t)
	// The stale read the fixture shows is of a ticket at priority 1.
	s.edit("weft/example-ready-a0b1", "seed", editBody(nil, 1, "ready", 2)).expect(t, http.StatusOK)

	fixtureMessage := func(name string) string {
		var doc map[string]any
		fixtureJSON(t, name, &doc)
		message := doc["error"].(map[string]any)["message"].(string)
		for fixtureID, ours := range s.runIDs {
			message = strings.ReplaceAll(message, fixtureID, ours)
		}
		return message
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(s.root, "runs.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reconciling := s.ticketRun["weft/example-ticket-0a1b"]
	if _, err := db.Exec(`UPDATE weft_runs SET settlement_state='reconciling' WHERE id=?`, reconciling); err != nil {
		t.Fatal(err)
	}
	shape := learn(decode(t, fixtureBytes(t, "error-ticket-edit-unknown-status.json")),
		decode(t, fixtureBytes(t, "error-ticket-edit-not-found.json")), decode(t, fixtureBytes(t, "error-ticket-edit-in-flight.json")),
		decode(t, fixtureBytes(t, "error-ticket-edit-stale.json")))

	for _, c := range []struct {
		name, id, body string
		status         int
		code           string
		// fixture is the error fixture whose message the refusal carries
		// exactly; message is a substring where no fixture shows one.
		fixture, message string
	}{
		{"neither", "weft/example-ready-a0b1", editBody(nil, nil, "ready", 1), 400, "malformed", "", "names neither"},
		{"unknown status", "weft/example-ready-a0b1", editBody("started", nil, "ready", 1), 400, "malformed",
			"error-ticket-edit-unknown-status.json", ""},
		{"priority range", "weft/example-ready-a0b1", editBody(nil, 7, "ready", 1), 400, "malformed",
			"error-ticket-edit-priority-range.json", ""},
		{"body before ticket", "weft/example-gone-0c1d", editBody(nil, 7, "ready", 1), 400, "malformed",
			"error-ticket-edit-priority-range.json", ""},
		{"not found", "weft/example-gone-0c1d", editBody("ready", nil, "backlog", 2), 404, "ticket_not_found",
			"error-ticket-edit-not-found.json", ""},
		{"running", "weft/example-second-4c2d", editBody("open", nil, "backlog", 0), 409, "ticket_in_flight",
			"error-ticket-edit-in-flight.json", ""},
		{"parked", "weft/example-fourth-6e4f", editBody(nil, 1, "open", 2), 409, "ticket_in_flight", "",
			"in phase parked"},
		{"waiting entry", "weft/example-third-5d3e", editBody(nil, 1, "ready", 3), 409, "ticket_in_flight", "",
			"launch entry e-20261002T095017Z-9a0b1c2d waiting"},
		{"reconciling", "weft/example-ticket-0a1b", editBody(nil, 1, "open", 2), 409, "ticket_in_flight", "",
			"run " + reconciling + " reconciling its landing"},
		{"stale before unchanged", "weft/example-ready-a0b1", editBody("ready", nil, "ready", 2), 409, "revision_conflict",
			"error-ticket-edit-stale.json", ""},
		{"unchanged", "weft/example-ready-a0b1", editBody("ready", nil, "ready", 1), 400, "malformed",
			"error-ticket-edit-status-unchanged.json", ""},
		{"epic", "weft/example-epic-1a2b", editBody("ready", nil, "open", 1), 400, "malformed",
			"error-ticket-edit-epic-status.json", ""},
	} {
		var file []byte
		if c.code != "ticket_not_found" && !strings.Contains(c.id, "gone") {
			file = s.ticketFile(c.id)
		}
		seq := s.asOf()
		r := s.edit(c.id, "refuse-"+c.name, c.body)
		message := r.refused(t, c.status, c.code)
		shape.check(t, c.name, r.body)
		switch {
		case c.fixture != "" && message != fixtureMessage(c.fixture):
			t.Errorf("%s: message %q, %s has %q", c.name, message, c.fixture, fixtureMessage(c.fixture))
		case c.fixture == "" && !strings.Contains(message, c.message):
			t.Errorf("%s: message %q, want it to say %q", c.name, message, c.message)
		}
		if file != nil && string(s.ticketFile(c.id)) != string(file) {
			t.Errorf("%s: the refused edit rewrote %s", c.name, c.id)
		}
		if s.asOf() != seq {
			t.Errorf("%s: the refused edit moved the stream from %d to %d", c.name, seq, s.asOf())
		}
	}

	// A cleanup entry writes no ticket status, so it refuses no edit.
	earlier := "weft/example-earlier-7f3c"
	repo, _ := sandbox.CanonicalRepository(s.projects["weft"])
	if _, err := db.Exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,run_id,position,state,
		token_id,request_key,created_at,updated_at,revision)
		VALUES('e-20261002T100000Z-0c1ea0c1','cleanup',?,'weft',?,?,?,9,'waiting',
		'token','cleanup-earlier','2026-10-02T10:00:00.000Z','2026-10-02T10:00:00.000Z',1)`,
		earlier, sandbox.RepositoryID(repo), repo, s.ticketRun[earlier]); err != nil {
		t.Fatal(err)
	}
	// The row names the entry, a shape tickets.json shows and the edit
	// fixtures, whose ticket has none, do not.
	s.edit(earlier, "edit-under-cleanup", editBody(nil, 1, "done", 2)).expect(t, http.StatusOK)
	if got := s.get("/v2/tickets/" + earlier)["ticket"].(map[string]any)["priority"]; got != float64(1) {
		t.Errorf("%s priority = %v after an edit under a cleanup entry", earlier, got)
	}
}

// An epic set closed is abandoned, which closes its children with it, so a
// child's run refuses the epic's edit as it would refuse the child's own.
func TestTicketEditEpicCloseHoldsChildRuns(t *testing.T) {
	s, _ := newEditStudio(t)
	child := "weft/example-second-4c2d"
	created := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	writeTicketIn(t, s.central, "weft", &ticket.Ticket{
		ID: "container-0002", Status: ticket.StatusOpen, Type: ticket.TypeEpic, Priority: 2, Title: "Container",
		Created: created, Updated: created,
	})
	tk, err := ticket.NewMultiStore(filepath.Join(s.central, "tickets")).Get(child)
	if err != nil {
		t.Fatal(err)
	}
	tk.ID, tk.Parent = bare(child), "container-0002"
	writeTicketIn(t, s.central, "weft", tk)
	s.sync()

	epic := s.get("/v2/tickets/weft/container-0002")["ticket"].(map[string]any)
	epicFile, childFile, seq := s.ticketFile("weft/container-0002"), s.ticketFile(child), s.asOf()
	r := s.edit("weft/container-0002", "close-epic", editBody("closed", nil, epic["status"].(string), 2))
	message := r.refused(t, http.StatusConflict, "ticket_in_flight")
	var want map[string]any
	fixtureJSON(t, "error-ticket-edit-in-flight.json", &want)
	wantMessage := want["error"].(map[string]any)["message"].(string)
	for fixtureID, ours := range s.runIDs {
		wantMessage = strings.ReplaceAll(wantMessage, fixtureID, ours)
	}
	if message != wantMessage {
		t.Errorf("message %q, error-ticket-edit-in-flight.json has %q", message, wantMessage)
	}
	if string(s.ticketFile("weft/container-0002")) != string(epicFile) || string(s.ticketFile(child)) != string(childFile) {
		t.Fatalf("the refused abandon rewrote the epic or its child")
	}
	if s.asOf() != seq {
		t.Errorf("the refused abandon moved the stream from %d to %d", seq, s.asOf())
	}
}
