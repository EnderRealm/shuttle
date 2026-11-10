package weftremote

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

// fixtureRefusal is an error fixture's message with the fixture's entry id
// replaced by id.
func fixtureRefusal(t *testing.T, name, fixtureID, id string) string {
	t.Helper()
	message := decode(t, fixtureBytes(t, name)).(map[string]any)["error"].(map[string]any)["message"].(string)
	if !strings.Contains(message, fixtureID) {
		t.Fatalf("%s does not name %s: %s", name, fixtureID, message)
	}
	return strings.ReplaceAll(message, fixtureID, id)
}

// dismissedEvents are the events written after seq, every one a queue event
// held to the shapes the state-stream and dismissal fixtures show.
func (h *queueHost) dismissedEvents(seq int64) []map[string]any {
	h.t.Helper()
	events, err := h.store.WeftEventsAfter(seq)
	if err != nil {
		h.t.Fatal(err)
	}
	queue := learn(append(stateSamples(h.t)["queue"], fixtureEvents(h.t, "events-queue-dismiss.sse")["queue"]...)...)
	var out []map[string]any
	for _, ev := range events {
		if ev.Type != "queue" {
			h.t.Fatalf("a dismissal wrote a %s event: %s", ev.Type, ev.Payload)
		}
		doc := decode(h.t, ev.Payload).(map[string]any)
		queue.check(h.t, "queue", doc)
		out = append(out, doc)
	}
	return out
}

func listed(q map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, e := range q["entries"].([]any) {
		out[e.(map[string]any)["entry_id"].(string)] = e.(map[string]any)
	}
	return out
}

// Dismissal takes a settled entry out of the listing, one at a time or every
// one at once, and nothing else: the entry keeps its state and revision and
// is still answered by id, its run reads as it did, and each commit raises
// the queue revision once with a dismissed queue event per entry and no
// capacity event. A waiting entry, a stale revision and an unknown entry are
// refused and write nothing (§6.3).
func TestQueueDismissTakesSettledEntriesOutOfTheListing(t *testing.T) {
	h := newQueueHost(t)
	for _, id := range []string{"weft/holds-0001", "weft/waits-0002", "weft/cancels-0003", "weft/closed-0004", "loom/parks-0005"} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	h.mode("weft/holds-0001", "hold")
	holder := entryOf(h.launch("weft/holds-0001", "l1"))
	waits := entryOf(h.launch("weft/waits-0002", "l2"))
	cancels := entryOf(h.launch("weft/cancels-0003", "l3"))
	closed := entryOf(h.launch("weft/closed-0004", "l4"))
	parksLaunch := h.launch("loom/parks-0005", "l5")
	parks := entryOf(parksLaunch)
	parkedRun := parks["run_id"].(string)
	h.waitPhase(parkedRun, ledger.WeftPhaseParked)
	h.waitPhase(holder["run_id"].(string), ledger.WeftPhaseRunning)

	beforeCancel := asOf(t, h.store)
	h.send(http.MethodPost, "/v2/queue/"+cancels["entry_id"].(string)+"/cancel", "c1", revisionBody(cancels["revision"])).
		expect(t, http.StatusOK, "queue-cancel-response.json")
	for _, ev := range h.eventsSince(beforeCancel) {
		if doc := decode(t, ev.Payload).(map[string]any); ev.Type == "queue" && doc["dismissed"] != false {
			t.Fatalf("a cancel's queue event = %s, want dismissed false", ev.Payload)
		}
	}
	h.ticket("weft/closed-0004", ticket.StatusClosed, 2)
	if _, err := h.service.syncCatalog(); err != nil {
		t.Fatal(err)
	}
	if e := h.entry(closed["entry_id"].(string)); e["state"] != "invalidated" {
		t.Fatalf("closed ticket's entry = %v", e)
	}

	// Each refusal writes nothing.
	dismiss := func(id, key string, revision any) response {
		return h.send(http.MethodPost, "/v2/queue/"+id+"/dismiss", key, revisionBody(revision))
	}
	queueBefore, seqBefore := h.queue(), asOf(t, h.store)
	unknown := "e-20200101T000000Z-00000000"
	if got := dismiss(unknown, "d-unknown", 1).refused(t, http.StatusNotFound, "entry_not_found"); got !=
		fixtureRefusal(t, "error-queue-dismiss-not-found.json", "e-20261002T084009Z-0c1d2e3f", unknown) {
		t.Errorf("unknown entry = %q", got)
	}
	waitsID := waits["entry_id"].(string)
	if got := dismiss(waitsID, "d-waiting", waits["revision"]).refused(t, http.StatusConflict, "entry_not_settled"); got !=
		fixtureRefusal(t, "error-queue-dismiss-not-settled.json", "e-20261002T095017Z-9a0b1c2d", waitsID) {
		t.Errorf("waiting entry = %q", got)
	}
	cancelsID := cancels["entry_id"].(string)
	if got := dismiss(cancelsID, "d-stale", 1).refused(t, http.StatusConflict, "revision_conflict"); got !=
		fixtureRefusal(t, "error-queue-dismiss-stale.json", "e-20261002T092917Z-708192a3", cancelsID) {
		t.Errorf("stale entry revision = %q", got)
	}
	h.send(http.MethodPost, "/v2/queue/dismiss-settled", "ds-stale", revisionBody(h.queueRevision()-1)).
		refused(t, http.StatusConflict, "revision_conflict")
	if got := h.queue(); !reflect.DeepEqual(got, queueBefore) || asOf(t, h.store) != seqBefore {
		t.Fatalf("a refusal wrote: queue %v, was %v; as_of %d, was %d", got, queueBefore, asOf(t, h.store), seqBefore)
	}

	// One dispatched entry: answered as it stands, gone from the listing,
	// one revision and one dismissed queue event; its run untouched.
	parksID := parks["entry_id"].(string)
	shown := h.entry(parksID)
	runBefore := h.get("/v2/runs/" + parkedRun)["run"]
	revision := h.queueRevision()
	seq := asOf(t, h.store)
	one := dismiss(parksID, "d1", shown["revision"])
	one.expect(t, http.StatusOK, "queue-dismiss-response.json")
	entryShape(t).check(t, "entry", entryOf(one))
	if !reflect.DeepEqual(entryOf(one), shown) {
		t.Fatalf("dismissed entry = %v, was listed as %v", entryOf(one), shown)
	}
	q := h.queue()
	if _, ok := listed(q)[parksID]; ok || int64(q["revision"].(float64)) != revision+1 {
		t.Fatalf("queue after dismissal = %v, want %s gone at revision %d", q, parksID, revision+1)
	}
	events := h.dismissedEvents(seq)
	if len(events) != 1 || events[0]["dismissed"] != true || events[0]["queue_revision"] != float64(revision+1) ||
		!reflect.DeepEqual(events[0]["entry"], shown) {
		t.Fatalf("dismissal events = %v", events)
	}
	if got := h.get("/v2/runs/" + parkedRun)["run"]; !reflect.DeepEqual(got, runBefore) {
		t.Fatalf("dismissal changed run %s: %v, was %v", parkedRun, got, runBefore)
	}
	// Still readable by id: the dismissal and the launch replay, and the
	// entry dismissed again answers as it stands, writing nothing.
	if replay := dismiss(parksID, "d1", shown["revision"]); !replay.replayed || replay.raw != one.raw {
		t.Fatalf("dismissal replay = %+v", replay)
	}
	if replay := h.launch("loom/parks-0005", "l5"); !replay.replayed || replay.raw != parksLaunch.raw {
		t.Fatalf("launch replay = %+v", replay)
	}
	seq = asOf(t, h.store)
	again := dismiss(parksID, "d1-again", shown["revision"])
	again.expect(t, http.StatusOK, "queue-dismiss-response.json")
	if again.replayed || !reflect.DeepEqual(entryOf(again), shown) || asOf(t, h.store) != seq || h.queueRevision() != revision+1 {
		t.Fatalf("dismissed again = %v, as_of %d was %d", again.raw, asOf(t, h.store), seq)
	}

	// Every settled entry at once — the running holder's dispatched entry,
	// the cancelled and the invalidated — and the waiting one stays.
	revision, seq = h.queueRevision(), asOf(t, h.store)
	settledIDs := []string{holder["entry_id"].(string), cancelsID, closed["entry_id"].(string)}
	shownSettled := listed(h.queue())
	all := h.send(http.MethodPost, "/v2/queue/dismiss-settled", "ds1", revisionBody(revision))
	all.expect(t, http.StatusOK, "queue-dismiss-settled-response.json")
	q = h.queue()
	if !reflect.DeepEqual(all.body["queue"], q) || int64(q["revision"].(float64)) != revision+1 {
		t.Fatalf("dismiss-settled = %v, the queue reads %v", all.body["queue"], q)
	}
	left := listed(q)
	if len(left) != 1 || left[waitsID]["state"] != "waiting" {
		t.Fatalf("after dismiss-settled the queue lists %v, want only %s waiting", left, waitsID)
	}
	events = h.dismissedEvents(seq)
	if len(events) != len(settledIDs) {
		t.Fatalf("dismiss-settled wrote %d events, want %d: %v", len(events), len(settledIDs), events)
	}
	for i, ev := range events {
		entry := ev["entry"].(map[string]any)
		if entry["entry_id"] != settledIDs[i] || ev["dismissed"] != true || ev["queue_revision"] != float64(revision+1) ||
			!reflect.DeepEqual(entry, shownSettled[settledIDs[i]]) {
			t.Fatalf("event %d = %v, want %s as listed, dismissed at revision %d", i, ev, settledIDs[i], revision+1)
		}
	}
	if run := h.run(holder["run_id"].(string)); run.Phase != ledger.WeftPhaseRunning {
		t.Fatalf("dismiss-settled touched the holder's run: %+v", run)
	}
	// Nothing settled is listed: the queue is answered and nothing written.
	seq = asOf(t, h.store)
	none := h.send(http.MethodPost, "/v2/queue/dismiss-settled", "ds2", revisionBody(revision+1))
	none.expect(t, http.StatusOK, "queue-dismiss-settled-response.json")
	if !reflect.DeepEqual(none.body["queue"], q) || asOf(t, h.store) != seq {
		t.Fatalf("dismiss-settled with nothing settled = %v, as_of %d was %d", none.raw, asOf(t, h.store), seq)
	}
	if !strings.Contains(h.logs.String(), "entry="+parksID+" run="+parkedRun+": dismissed launch loom/parks-0005") {
		t.Fatalf("the log does not name the dismissal:\n%s", h.logs.String())
	}
	h.release("weft/holds-0001")
}
