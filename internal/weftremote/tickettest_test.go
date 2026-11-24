package weftremote

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

// testedTicket is the ticket the test fixtures pass, undo and fail: in
// testing, with its landed run over.
const testedTicket = "weft/example-second-4c2d"

// newTestStudio is newEditStudio's catalog as tickets-testing.json has the
// tested ticket: landed, in testing, updated when the fixture's pass read it.
func newTestStudio(t *testing.T) *studio {
	t.Helper()
	s := newStudio(t)
	t.Setenv("HOME", s.root)
	tickets := s.seedFixtureCatalog()
	for _, ft := range tickets {
		if ft.ID == testedTicket {
			ft.Run.Phase = "ended"
		}
	}
	s.seedFixtureRuns(tickets)
	s.exec(`UPDATE weft_runs SET settlement_state=? WHERE id=?`, ledger.WeftSettlementLanded, s.ticketRun[testedTicket])
	s.writeTestedTicket(nil)
	s.sync()
	return s
}

func (s *studio) exec(query string, args ...any) {
	s.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(s.root, "runs.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		s.t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(query, args...); err != nil {
		s.t.Fatal(err)
	}
}

// writeTestedTicket writes the tested ticket as the fixtures have it in
// testing, changed by edit where one is given.
func (s *studio) writeTestedTicket(edit func(*ticket.Ticket)) {
	s.t.Helper()
	var list struct {
		Tickets []fixtureTicket `json:"tickets"`
	}
	fixtureJSON(s.t, "tickets-testing.json", &list)
	detail := s.fixtureResponse("undo")
	for _, ft := range list.Tickets {
		if ft.ID != testedTicket {
			continue
		}
		updated, err := time.Parse(time.RFC3339, ft.Updated)
		if err != nil {
			s.t.Fatal(err)
		}
		tk := &ticket.Ticket{
			ID: bare(ft.ID), Title: ft.Title, Status: ticket.Status(ft.Status), Type: ticket.TicketType(ft.Type),
			Priority: *ft.Priority, Branch: *ft.Branch, Created: updated.Add(-time.Hour), Updated: updated,
			Body: detail["description"].(string) + "\n\n## Acceptance Criteria\n\n" + detail["acceptance"].(string) + "\n",
		}
		if edit != nil {
			edit(tk)
		}
		writeTicketIn(s.t, s.central, "weft", tk)
		return
	}
	s.t.Fatalf("tickets-testing.json lists no %s", testedTicket)
}

func (s *studio) tk() *ticket.MultiStore {
	return ticket.NewMultiStore(filepath.Join(s.central, "tickets"))
}

func (s *studio) stored(id string) *ticket.Ticket {
	s.t.Helper()
	t, err := s.tk().Get(id)
	if err != nil {
		s.t.Fatal(err)
	}
	return t
}

func (s *studio) row(id string) map[string]any {
	s.t.Helper()
	project, _, _ := strings.Cut(id, "/")
	for _, row := range s.get("/v2/tickets?project=" + project)["tickets"].([]any) {
		if row := row.(map[string]any); row["id"] == id {
			return row
		}
	}
	s.t.Fatalf("%s is not listed", id)
	return nil
}

func (s *studio) test(id, key, body string) response {
	s.t.Helper()
	rec := s.request(s.t, http.MethodPost, "/v2/tickets/"+id+"/test", key, body)
	doc, _ := decode(s.t, rec.Body.Bytes()).(map[string]any)
	return response{code: rec.Code, replayed: rec.Header().Get("Idempotent-Replayed") == "true", body: doc, raw: rec.Body.String()}
}

// decide sends ticket-test-request-<action>.json with the updated the ticket
// reads now, and holds the answer to ticket-test-response-<action>.json.
func (s *studio) decide(action, key string) map[string]any {
	s.t.Helper()
	r := s.test(testedTicket, key, s.testRequest(action, s.row(testedTicket)["updated"].(string)))
	s.decided(action, r)
	return r.body["ticket"].(map[string]any)
}

func (s *studio) testRequest(action, updated string) string {
	s.t.Helper()
	var request map[string]any
	fixtureJSON(s.t, "ticket-test-request-"+action+".json", &request)
	request["read_updated"] = updated
	body, _ := json.Marshal(request)
	return string(body)
}

func (s *studio) fixtureResponse(action string) map[string]any {
	s.t.Helper()
	var doc map[string]any
	fixtureJSON(s.t, "ticket-test-response-"+action+".json", &doc)
	return doc["ticket"].(map[string]any)
}

// decided holds a test's answer to its fixture and to the GET after it. The
// instants and the run are this host's own; tk notes each action, where the
// fixtures count a fail's note alone; and whether the landed run's retained
// branch still refuses the failed ticket a launch is §6.7's.
func (s *studio) decided(action string, r response) {
	t := s.t
	t.Helper()
	fixture := "ticket-test-response-" + action + ".json"
	r.expect(t, http.StatusOK, "ticket-test-response-pass.json", "ticket-test-response-undo.json", "ticket-test-response-fail.json", "ticket.json")
	got, want := r.body["ticket"].(map[string]any), s.fixtureResponse(action)
	for key, value := range want {
		switch {
		case key == "updated" || key == "run" || key == "runs" || key == "notes_total" || key == "last_test":
		case key == "eligibility" && action == testActionFail:
		case !reflect.DeepEqual(got[key], value):
			t.Errorf("%s: %s = %v, %s has %v", action, key, got[key], fixture, value)
		}
	}
	gotTest, _ := got["last_test"].(map[string]any)
	wantTest, _ := want["last_test"].(map[string]any)
	if (gotTest == nil) != (wantTest == nil) {
		t.Fatalf("%s: last_test = %v, %s has %v", action, got["last_test"], fixture, want["last_test"])
	}
	for _, key := range []string{"result", "checks", "note"} {
		if !reflect.DeepEqual(gotTest[key], wantTest[key]) {
			t.Errorf("%s: last_test.%s = %v, %s has %v", action, key, gotTest[key], fixture, wantTest[key])
		}
	}
	// Not held to updated: tk reads the clock for each, and a second may turn
	// between the two.
	if gotTest != nil {
		at, err := time.Parse("2006-01-02T15:04:05.000Z", gotTest["at"].(string))
		if err != nil || time.Since(at) < 0 || time.Since(at) > time.Minute {
			t.Errorf("%s: last_test.at = %v (%v)", action, gotTest["at"], err)
		}
	}
	if after := s.get("/v2/tickets/" + testedTicket)["ticket"]; !reflect.DeepEqual(after, got) {
		t.Errorf("GET after the %s = %v, the %s returned %v", action, after, action, got)
	}
	row := s.row(testedTicket)
	learn(decode(t, fixtureBytes(t, "tickets-testing.json")), decode(t, fixtureBytes(t, "tickets.json"))).
		check(t, "tickets", s.get("/v2/tickets?project=weft"))
	for key, value := range row {
		if !reflect.DeepEqual(got[key], value) {
			t.Errorf("%s: the listing has %s = %v, the detail %v", action, key, value, got[key])
		}
	}
}

func TestTicketTestPass(t *testing.T) {
	s := newTestStudio(t)
	before := s.row(testedTicket)
	if before["status"] != "testing" || before["last_test"] != nil {
		t.Fatalf("%s before its test = %v", testedTicket, before)
	}
	// The fixture's request as it stands: the ticket is as tickets-testing.json
	// showed it to the client.
	body := string(fixtureBytes(t, "ticket-test-request-pass.json"))
	r := s.test(testedTicket, "pass", body)
	s.decided("pass", r)

	stored := s.stored(testedTicket)
	if stored.Status != ticket.StatusDone || len(stored.Tests) != 1 || stored.Tests[0].Result != ticket.TestPassed ||
		len(stored.Notes) != 1 || len(stored.Actions) != 1 || stored.Actions[0].Outcome != string(ticket.StatusDone) {
		t.Fatalf("%s as tk reads it = %+v", testedTicket, stored)
	}

	// A replay answers with the first response and writes nothing.
	file, seq := s.ticketFile(testedTicket), s.asOf()
	again := s.test(testedTicket, "pass", body)
	if !again.replayed || again.raw != r.raw {
		t.Fatalf("replay = %v %s", again.replayed, again.raw)
	}
	// A retry of a request whose ledger commit was lost after tk's write is
	// the action tk recorded, not a second one.
	s.exec(`DELETE FROM weft_requests WHERE key='pass'`)
	retried := s.test(testedTicket, "pass", body)
	s.decided("pass", retried)
	if retried.replayed || string(s.ticketFile(testedTicket)) != string(file) || s.asOf() != seq {
		t.Fatalf("a retry wrote again: replayed %v, stream %d -> %d, file %s", retried.replayed, seq, s.asOf(), s.ticketFile(testedTicket))
	}
}

func TestTicketTestFailNeedsNoteAndChecks(t *testing.T) {
	s := newTestStudio(t)
	updated := s.row(testedTicket)["updated"].(string)
	file, seq := s.ticketFile(testedTicket), s.asOf()
	for _, c := range []struct {
		name          string
		checks, note  any
		fixture, says string
	}{
		{"no check", []int{}, "It overflows.", "error-ticket-test-fail-no-check.json", ""},
		{"no note", []int{2}, nil, "error-ticket-test-fail-no-note.json", ""},
		{"blank note", []int{2}, " \n\t", "error-ticket-test-fail-no-note.json", ""},
		{"neither", []int{}, nil, "error-ticket-test-fail-no-check.json", ""},
		{"null checks", nil, "It overflows.", "error-ticket-test-fail-no-check.json", ""},
		{"repeated check", []int{2, 2}, "It overflows.", "error-ticket-test-checks-invalid.json", ""},
		{"check zero", []int{0}, "It overflows.", "", "checks [0] are not distinct positive indexes"},
		{"check past the criteria", []int{3}, "It overflows.", "error-ticket-test-check-range.json", ""},
	} {
		body := testBody("fail", c.checks, c.note, updated)
		message := s.test(testedTicket, "fail-"+c.name, body).refused(t, http.StatusBadRequest, "malformed")
		if want := c.says + s.fixtureMessage(c.fixture); message != want {
			t.Errorf("%s: message %q, want %q", c.name, message, want)
		}
	}
	if string(s.ticketFile(testedTicket)) != string(file) || s.asOf() != seq {
		t.Fatalf("a refused fail wrote: stream %d -> %d, file %s", seq, s.asOf(), s.ticketFile(testedTicket))
	}

	body := s.testRequest("fail", updated)
	r := s.test(testedTicket, "fail", body)
	s.decided("fail", r)
	row := s.row(testedTicket)
	if row["status"] != "ready" || row["last_test"].(map[string]any)["result"] != "failed" {
		t.Fatalf("%s after its fail = %v", testedTicket, row)
	}
	var request struct{ Note string }
	fixtureJSON(t, "ticket-test-request-fail.json", &request)
	stored := s.stored(testedTicket)
	if len(stored.Notes) != 1 || stored.Notes[0].Text != request.Note || r.body["ticket"].(map[string]any)["notes_total"] != float64(1) {
		t.Fatalf("notes after the fail = %+v", stored.Notes)
	}
	// The record keeps the criterion as it read at the test.
	stored.Body = strings.Replace(stored.Body, "A long answer wraps inside the card.", "A long answer scrolls.", 1)
	if err := s.tk().Update(stored); err != nil {
		t.Fatal(err)
	}
	if got := s.row(testedTicket)["last_test"]; !reflect.DeepEqual(got, row["last_test"]) {
		t.Errorf("last_test after an edit of its criterion = %v, was %v", got, row["last_test"])
	}
	// The whole fail again, under its key: the first answer, and one note.
	if again := s.test(testedTicket, "fail", body); !again.replayed || again.raw != r.raw || len(s.stored(testedTicket).Notes) != 1 {
		t.Fatalf("replay = %v %s", again.replayed, again.raw)
	}
}

func TestTicketTestUndo(t *testing.T) {
	s := newTestStudio(t)
	s.decide("pass", "pass")
	s.decide("undo", "undo-pass")
	s.decide("fail", "fail")
	s.decide("undo", "undo-fail")
	if stored := s.stored(testedTicket); stored.Status != ticket.StatusTesting || len(stored.Tests) != 0 || len(stored.Notes) != 4 {
		t.Fatalf("%s after two undone tests = %+v", testedTicket, stored)
	}

	// Failed, landed again, passed: an undo leaves the earlier test standing.
	failed := s.decide("fail", "fail-again")
	if _, err := ticket.ApplyAction(s.tk(), ticket.ActionRequest{
		ID: testedTicket, ActionID: "land-again", Note: "Landed.", Transition: ticket.TransitionLand, Commit: fixtureHead,
	}); err != nil {
		t.Fatal(err)
	}
	s.decide("pass", "pass-again")
	r := s.test(testedTicket, "undo-again", s.testRequest("undo", s.row(testedTicket)["updated"].(string)))
	r.expect(t, http.StatusOK, "ticket-test-response-undo.json", "ticket-test-response-fail.json", "ticket.json")
	got := r.body["ticket"].(map[string]any)
	if got["status"] != "testing" || !reflect.DeepEqual(got["last_test"], failed["last_test"]) {
		t.Fatalf("undone to the test before = %v, want last_test %v", got, failed["last_test"])
	}
}

func testBody(action string, checks, note, updated any) string {
	doc, _ := json.Marshal(map[string]any{
		"contract_version": ServiceVersion, "action": action, "checks": checks, "note": note, "read_updated": updated,
	})
	return string(doc)
}

// fixtureMessage is an error fixture's message with this host's run ids, or
// "" for no fixture.
func (s *studio) fixtureMessage(name string) string {
	s.t.Helper()
	if name == "" {
		return ""
	}
	var doc map[string]any
	fixtureJSON(s.t, name, &doc)
	message := doc["error"].(map[string]any)["message"].(string)
	for fixtureID, ours := range s.runIDs {
		message = strings.ReplaceAll(message, fixtureID, ours)
	}
	return strings.ReplaceAll(message, "h-studio-7c41e09a", s.service.hostID)
}

func TestTicketTestRefusals(t *testing.T) {
	s := newTestStudio(t)
	// What the contract's stale read was overtaken by.
	const read, stale = "2026-10-02T10:47:19.000Z", "2026-10-02T10:31:21.000Z"
	updated, err := time.Parse(time.RFC3339, read)
	if err != nil {
		t.Fatal(err)
	}
	s.writeTestedTicket(func(tk *ticket.Ticket) { tk.Updated = updated })
	// Passed at one commit, landed since as another and edited done by hand:
	// no test of this landing left it there.
	passed := func(id, status, outputCommit string) {
		writeTicketIn(t, s.central, "weft", &ticket.Ticket{
			ID: bare(id), Title: "Passed", Status: ticket.Status(status), Type: ticket.TypeFeature, Priority: 2,
			Created: updated, Updated: updated, Outputs: map[string]string{ticket.OutputKeyCommit: outputCommit},
			Tests: []ticket.TestRow{{Result: ticket.TestPassed, At: "2026-10-02T10:00:00Z", By: "human", Commit: fixtureBase}},
		})
	}
	passed("weft/relanded-0001", "done", fixtureHead)
	passed("weft/reopened-0002", "open", fixtureBase)
	s.sync()
	run := s.ticketRun[testedTicket]

	for _, c := range []struct {
		name, id, body string
		// phase and settlement are the tested ticket's run's, when not over
		// and landed.
		phase, settlement string
		status            int
		code              string
		fixture, message  string
	}{
		{name: "unknown action", id: testedTicket, body: testBody("skip", []int{}, nil, read), status: 400, code: "malformed",
			fixture: "error-ticket-test-unknown-action.json"},
		{name: "pass with a check", id: testedTicket, body: testBody("pass", []int{1}, nil, read), status: 400, code: "malformed",
			fixture: "error-ticket-test-pass-fields.json"},
		{name: "pass with a note", id: testedTicket, body: testBody("pass", []int{}, "Fine.", read), status: 400, code: "malformed",
			fixture: "error-ticket-test-pass-fields.json"},
		{name: "undo with a note", id: testedTicket, body: testBody("undo", []int{}, "Oops.", read), status: 400, code: "malformed",
			message: "an undo carries no checks and no note"},
		{name: "null checks", id: testedTicket, body: testBody("pass", nil, nil, read), status: 400, code: "malformed",
			message: "checks is null"},
		{name: "no read", id: testedTicket, body: testBody("pass", []int{}, nil, nil), status: 400, code: "malformed",
			message: "read_updated is null"},
		{name: "body before ticket", id: "weft/example-gone-0c1d", body: testBody("skip", []int{}, nil, read), status: 400,
			code: "malformed", fixture: "error-ticket-test-unknown-action.json"},
		{name: "not found", id: "weft/example-gone-0c1d", body: testBody("pass", []int{}, nil, read), status: 404,
			code: "ticket_not_found", fixture: "error-ticket-test-not-found.json"},
		{name: "settling, before stale", id: testedTicket, body: testBody("pass", []int{}, nil, stale),
			phase: ledger.WeftPhaseSettling, settlement: ledger.WeftSettlementInProgress, status: 409,
			code: "ticket_in_flight", fixture: "error-ticket-test-in-flight.json"},
		{name: "reconciling", id: testedTicket, body: testBody("pass", []int{}, nil, read),
			phase: ledger.WeftPhaseEnded, settlement: ledger.WeftSettlementReconciling, status: 409, code: "ticket_in_flight",
			message: testedTicket + " has run " + run + " reconciling its landing; test the ticket once that run is over"},
		{name: "waiting entry", id: "weft/example-third-5d3e", body: testBody("undo", []int{}, nil, read), status: 409,
			code: "ticket_in_flight", message: "weft/example-third-5d3e has launch entry e-20261002T095017Z-9a0b1c2d waiting in the queue; " +
				"test the ticket once that entry has left it"},
		{name: "stale", id: testedTicket, body: testBody("pass", []int{}, nil, stale), status: 409,
			code: "revision_conflict", fixture: "error-ticket-test-stale.json"},
		{name: "stale before not testing", id: "weft/example-ready-a0b1", body: testBody("pass", []int{}, nil, stale), status: 409,
			code: "revision_conflict", message: "weft/example-ready-a0b1 was updated at " + s.row("weft/example-ready-a0b1")["updated"].(string) +
				" and the test read it at " + stale + ": read it again and decide again"},
		{name: "pass not testing", id: "weft/example-ready-a0b1", body: testBody("pass", []int{}, nil, ""), status: 409,
			code: "ticket_not_testing", fixture: "error-ticket-test-not-testing.json"},
		{name: "fail not testing, before its range", id: "weft/example-ready-a0b1", body: testBody("fail", []int{9}, "No.", ""), status: 409,
			code: "ticket_not_testing", fixture: "error-ticket-test-not-testing.json"},
		{name: "epic", id: "weft/example-epic-1a2b", body: testBody("pass", []int{}, nil, ""), status: 409,
			code: "ticket_not_testing", message: "weft/example-epic-1a2b is open, not testing"},
		{name: "undo with no test", id: "weft/example-earlier-7f3c", body: testBody("undo", []int{}, nil, ""), status: 409,
			code: "no_test_to_undo", fixture: "error-ticket-test-no-undo.json"},
		{name: "undo in testing", id: testedTicket, body: testBody("undo", []int{}, nil, read), status: 409,
			code: "no_test_to_undo", message: testedTicket + " is testing and holds no test that left it there"},
		{name: "undo after another landing", id: "weft/relanded-0001", body: testBody("undo", []int{}, nil, read), status: 409,
			code: "no_test_to_undo", message: "weft/relanded-0001 is done and holds no test that left it there"},
		{name: "undo after a status edit", id: "weft/reopened-0002", body: testBody("undo", []int{}, nil, read), status: 409,
			code: "no_test_to_undo", message: "weft/reopened-0002 is open and holds no test that left it there"},
	} {
		if c.phase != "" {
			s.exec(`UPDATE weft_runs SET phase=?,settlement_state=? WHERE id=?`, c.phase, c.settlement, run)
		}
		// "" reads the ticket as the host serves it.
		body := strings.ReplaceAll(c.body, `"read_updated":""`, `"read_updated":"`+updatedOf(s, c.id)+`"`)
		var file []byte
		if !strings.Contains(c.id, "gone") {
			file = s.ticketFile(c.id)
		}
		seq := s.asOf()
		r := s.test(c.id, "refuse-"+c.name, body)
		message := r.refused(t, c.status, c.code)
		if want := c.message + s.fixtureMessage(c.fixture); message != want {
			t.Errorf("%s: message %q, want %q", c.name, message, want)
		}
		if c.fixture != "" {
			learn(decode(t, fixtureBytes(t, c.fixture))).check(t, c.name, r.body)
		}
		if file != nil && string(s.ticketFile(c.id)) != string(file) {
			t.Errorf("%s: the refused test rewrote %s", c.name, c.id)
		}
		if s.asOf() != seq {
			t.Errorf("%s: the refused test moved the stream from %d to %d", c.name, seq, s.asOf())
		}
		if again := s.test(c.id, "refuse-"+c.name, body); !again.replayed || again.raw != r.raw {
			t.Errorf("%s: replay = %v %s", c.name, again.replayed, again.raw)
		}
		if c.phase != "" {
			s.exec(`UPDATE weft_runs SET phase=?,settlement_state=? WHERE id=?`, ledger.WeftPhaseEnded, ledger.WeftSettlementLanded, run)
		}
	}

	// read_updated is an instant, however it is spelled.
	s.test(testedTicket, "spelled", testBody("pass", []int{}, nil, "2026-10-02T12:47:19+02:00")).expect(t, http.StatusOK)

	// A host that does not serve the capability refuses ahead of everything.
	served := developmentalCapabilities
	t.Cleanup(func() { developmentalCapabilities = served })
	developmentalCapabilities = nil
	for _, declared := range served {
		if declared.ID == "ticket_test" {
			var discovery struct {
				Service struct {
					Capabilities []struct{ ID, Reason string }
				}
			}
			fixtureJSON(t, "service-developmental.json", &discovery)
			for _, f := range discovery.Service.Capabilities {
				if f.ID == declared.ID {
					declared.Reason = f.Reason
				}
			}
		}
		developmentalCapabilities = append(developmentalCapabilities, declared)
	}
	message := s.test(testedTicket, "not-served", "{}").refused(t, http.StatusNotImplemented, "capability_not_served")
	if want := s.fixtureMessage("error-ticket-test-not-served.json"); message != want {
		t.Errorf("not served: message %q, want %q", message, want)
	}
}

// updatedOf is the ticket's updated as the host serves it, "" where it is not
// on the host.
func updatedOf(s *studio, id string) string {
	s.t.Helper()
	if strings.Contains(id, "gone") {
		return ""
	}
	return s.row(id)["updated"].(string)
}

func TestTicketTestAnnouncedOnStream(t *testing.T) {
	s := newTestStudio(t)
	server := httptest.NewServer(s.service)
	// A cleanup, so the stream's own cleanup closes it first.
	t.Cleanup(server.Close)
	events := openStream(t, server, "")
	events.next(t)

	shape := learn(fixtureEvents(t, "events-ticket-test.sse")["catalog"]...)
	for _, action := range []string{"pass", "undo", "fail"} {
		revision := s.revision("weft")
		r := s.test(testedTicket, "announce-"+action, s.testRequest(action, s.row(testedTicket)["updated"].(string)))
		s.decided(action, r)
		event := nextCatalog(t, events)
		shape.check(t, action, event)
		if event["project"] != "weft" || int64(event["catalog_revision"].(float64)) <= revision ||
			r.body["as_of"].(float64) < event["seq"].(float64) {
			t.Errorf("%s: catalog event %v after revision %d, test as_of %v", action, event, revision, r.body["as_of"])
		}
		if got := s.revision("weft"); got != int64(event["catalog_revision"].(float64)) {
			t.Errorf("%s: revision %d after the event of %v", action, got, event["catalog_revision"])
		}
	}
}
