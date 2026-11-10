package weftremote

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

// asks makes the ticket's fake worker park on structured questions.
func (h *queueHost) asks(ticketID string, questions any) {
	h.t.Helper()
	doc, err := json.Marshal(questions)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.control, slugOf(ticketID)+".questions"), doc, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// fixtureField is one top-level field of a fixture, decoded afresh so a
// caller may change it.
func fixtureField(t *testing.T, name, field string) any {
	t.Helper()
	return decode(t, fixtureBytes(t, name)).(map[string]any)[field]
}

// structuredQuestionShape is the question resource of a structured park,
// pending and answered.
func structuredQuestionShape(t *testing.T) *shape {
	return learn(fixtureField(t, "questions-structured.json", "questions").([]any)[0],
		fixtureField(t, "question-answered-structured.json", "question"))
}

// structuredQuestionEventShape is the question event of a structured park:
// the stream fixture's question event, carrying the structured resource in
// place of its legacy one (§6.5, §10).
func structuredQuestionEventShape(t *testing.T) *shape {
	var samples []any
	for _, ev := range fixtureEvents(t, "events-reset.sse")["question"] {
		for _, q := range []any{fixtureField(t, "questions-structured.json", "questions").([]any)[0],
			fixtureField(t, "question-answered-structured.json", "question")} {
			sample := maps.Clone(ev.(map[string]any))
			sample["question"] = q
			samples = append(samples, sample)
		}
	}
	return learn(samples...)
}

func listedQuestion(t *testing.T, r response, id string) map[string]any {
	t.Helper()
	for _, q := range r.body["questions"].([]any) {
		if q.(map[string]any)["question_id"] == id {
			return q.(map[string]any)
		}
	}
	t.Fatalf("question %s is not listed: %s", id, r.raw)
	return nil
}

// A structured park is stored as asked and served in that form on the
// question list, the question resource, the run and the question event; a
// legacy park beside it keeps its form, and a park that breaks a bound is
// refused and records no question (§6.5).
func TestStructuredQuestionsAreStoredAndServed(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/asks-0001", ticket.StatusReady, 2)
	h.ticket("loom/legacy-0002", ticket.StatusReady, 2)
	h.ticket("warp/other-0003", ticket.StatusReady, 2)
	questions := fixtureField(t, "worker-parked-structured.json", "questions")
	h.asks("weft/asks-0001", questions)
	before, err := h.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	runID := entryOf(h.launch("weft/asks-0001", "l1"))["run_id"].(string)
	qid := h.waitPhase(runID, ledger.WeftPhaseParked).Question.ID
	legacyRun := entryOf(h.launch("loom/legacy-0002", "l2"))["run_id"].(string)
	legacyQID := h.waitPhase(legacyRun, ledger.WeftPhaseParked).Question.ID

	listed := h.send(http.MethodGet, "/v2/questions", "", "")
	listed.expect(t, http.StatusOK, "questions-structured.json", "questions.json")
	structured := listedQuestion(t, listed, qid)
	if _, legacy := structured["text"]; legacy || !reflect.DeepEqual(structured["questions"], questions) ||
		structured["state"] != "pending" || structured["answer"] != nil || structured["run_id"] != runID {
		t.Fatalf("structured question = %v", structured)
	}
	if legacy := listedQuestion(t, listed, legacyQID); legacy["text"] != "Which shape?" || legacy["questions"] != nil {
		t.Fatalf("legacy question = %v", legacy)
	}

	one := h.send(http.MethodGet, "/v2/questions/"+qid, "", "")
	one.expect(t, http.StatusOK, "question-answered-structured.json")
	resource := one.body["question"].(map[string]any)
	if !reflect.DeepEqual(resource, structured) {
		t.Fatalf("question resource = %v, listed as %v", resource, structured)
	}
	h.send(http.MethodGet, "/v2/questions/"+legacyQID, "", "").expect(t, http.StatusOK, "question-answered.json")

	run := h.send(http.MethodGet, "/v2/runs/"+runID, "", "").body["run"].(map[string]any)
	structuredQuestionShape(t).check(t, "run.question", run["question"])
	if !reflect.DeepEqual(run["question"], resource) {
		t.Fatalf("run question = %v, want %v", run["question"], resource)
	}

	events, err := h.store.WeftEventsAfter(before)
	if err != nil {
		t.Fatal(err)
	}
	var asked []map[string]any
	for _, ev := range events {
		if payload := decode(t, ev.Payload).(map[string]any); ev.Type == "question" && ev.RunID == runID {
			asked = append(asked, payload)
		}
	}
	if len(asked) != 1 {
		t.Fatalf("question events for the structured park = %v", asked)
	}
	structuredQuestionEventShape(t).check(t, "question event", asked[0])
	if !reflect.DeepEqual(asked[0]["question"], resource) {
		t.Fatalf("question event = %v, want %v", asked[0]["question"], resource)
	}

	// An option labelled Other is refused: the run is not parked and asks nothing.
	other := fixtureField(t, "worker-parked-structured.json", "questions").([]any)
	other[0].(map[string]any)["options"].([]any)[1].(map[string]any)["label"] = "other"
	h.asks("warp/other-0003", other)
	otherRun := entryOf(h.launch("warp/other-0003", "l3"))["run_id"].(string)
	ended := h.waitPhase(otherRun, ledger.WeftPhaseEnded)
	if ended.Outcome.String != "failed" || ended.Segments[0].Failure.String != "protocol" ||
		!strings.Contains(ended.HaltDetail.String, "questions[0].options[1].label") || ended.Question != nil {
		t.Fatalf("run refused its park as %+v", ended)
	}
	if _, err := h.store.LatestWeftQuestion(otherRun); err == nil {
		t.Fatal("a refused park recorded a question")
	}
}

// A structured answer is held to its questions before anything is recorded:
// a label not among the options, an entry short, two selections without
// multiSelect, no selection and no other, a header not its question's, both
// forms or neither, and either form against the other's question are 400
// malformed. A valid answer is accepted once — a concurrent second gets 409
// — and the relaunched segment reads every question beside its answer
// (§6.5).
func TestStructuredAnswersAreValidatedAgainstTheirQuestions(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/asks-0001", ticket.StatusReady, 2)
	h.ticket("loom/legacy-0002", ticket.StatusReady, 2)
	questions := fixtureField(t, "worker-parked-structured.json", "questions")
	h.asks("weft/asks-0001", questions)
	runID := entryOf(h.launch("weft/asks-0001", "l1"))["run_id"].(string)
	qid := h.waitPhase(runID, ledger.WeftPhaseParked).Question.ID
	legacyRun := entryOf(h.launch("loom/legacy-0002", "l2"))["run_id"].(string)
	legacyQID := h.waitPhase(legacyRun, ledger.WeftPhaseParked).Question.ID
	before, err := h.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}

	answer := func(run, key string, fields map[string]any) response {
		t.Helper()
		body := map[string]any{"contract_version": ServiceVersion}
		maps.Copy(body, fields)
		doc, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return h.send(http.MethodPost, "/v2/runs/"+run+"/answer", key, string(doc))
	}
	answers := func() []any { return fixtureField(t, "answer-request-structured.json", "answers").([]any) }
	negative := func(name string) any { return fixtureField(t, "negative/"+name, "answers") }
	wrongHeader := answers()
	wrongHeader[0].(map[string]any)["header"] = "Title shown"
	emptyOther := answers()
	emptyOther[2].(map[string]any)["other"] = ""
	cases := []struct {
		name   string
		run    string
		fields map[string]any
	}{
		{"a label not among the options", runID, map[string]any{"question_id": qid, "answers": negative("answer-unknown-label.json")}},
		{"an entry short", runID, map[string]any{"question_id": qid, "answers": negative("answer-missing-entry.json")}},
		{"two selections without multiSelect", runID, map[string]any{"question_id": qid, "answers": negative("answer-multi-without-multiselect.json")}},
		{"no selection and no other", runID, map[string]any{"question_id": qid, "answers": negative("answer-empty.json")}},
		{"a header not its question's", runID, map[string]any{"question_id": qid, "answers": wrongHeader}},
		{"an empty other", runID, map[string]any{"question_id": qid, "answers": emptyOther}},
		{"both forms", runID, map[string]any{"question_id": qid, "answer": "The project.", "answers": answers()}},
		{"neither form", runID, map[string]any{"question_id": qid}},
		{"a legacy answer to structured questions", runID, map[string]any{"question_id": qid, "answer": "The project."}},
		{"a structured answer to a legacy question", legacyRun, map[string]any{"question_id": legacyQID, "answers": answers()}},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answer(c.run, fmt.Sprintf("bad-%d", i), c.fields).refused(t, http.StatusBadRequest, "malformed")
		})
	}
	events, err := h.store.WeftEventsAfter(before)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("a refused answer wrote %d events", len(events))
	}
	for _, id := range []string{qid, legacyQID} {
		q := h.send(http.MethodGet, "/v2/questions/"+id, "", "").body["question"].(map[string]any)
		if q["state"] != "pending" || q["answer"] != nil {
			t.Fatalf("question %s after refused answers = %v", id, q)
		}
	}

	// Two clients answering at once: one 202, one 409 naming the answer.
	results := make([]response, 2)
	var wg sync.WaitGroup
	for i, key := range []string{"mac-studio", "macbook"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = answer(runID, key, map[string]any{"question_id": qid, "answers": answers()})
		}()
	}
	wg.Wait()
	slices.SortFunc(results, func(a, b response) int { return a.code - b.code })
	won, lost := results[0], results[1]
	won.expect(t, http.StatusAccepted, "answer-response-structured.json")
	entryShape(t).check(t, "entry", entryOf(won))
	accepted := won.body["question"].(map[string]any)
	recorded := accepted["answer"].(map[string]any)
	if !reflect.DeepEqual(recorded["answers"], answers()) || !reflect.DeepEqual(accepted["questions"], questions) ||
		accepted["state"] != "answered" || recorded["entry_id"] != entryOf(won)["entry_id"] {
		t.Fatalf("accepted = %v", accepted)
	}
	message := lost.refused(t, http.StatusConflict, "question_not_pending")
	if !strings.Contains(message, recorded["answer_id"].(string)) {
		t.Fatalf("losing answer = %s, want the recorded answer named", message)
	}

	// The recording's answer and question events, the latter carrying the
	// structured resource.
	events, err = h.store.WeftEventsAfter(before)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 2 || events[0].Type != "answer" || events[1].Type != "question" {
		t.Fatalf("answer events = %+v", events)
	}
	samples := fixtureEvents(t, "events.sse")
	for name, more := range fixtureEvents(t, "events-reset.sse") {
		samples[name] = append(samples[name], more...)
	}
	learn(samples["answer"]...).check(t, "answer event", decode(t, events[0].Payload))
	recordedEvent := decode(t, events[1].Payload).(map[string]any)
	structuredQuestionEventShape(t).check(t, "question event", recordedEvent)
	if !reflect.DeepEqual(recordedEvent["question"], accepted) {
		t.Fatalf("question event = %v, want %v", recordedEvent["question"], accepted)
	}

	// The answer dispatches at once; its segment's hello acknowledges it and
	// its answer file carries every question beside its answer.
	var resumed *ledger.WeftRun
	waitFor(t, func() bool {
		resumed = h.run(runID)
		return len(resumed.Segments) == 2 && resumed.Phase == ledger.WeftPhaseParked && h.reaped(resumed)
	})
	doc, err := os.ReadFile(filepath.Join(resumed.Segments[1].RunDir.String, "answer.json"))
	if err != nil {
		t.Fatal(err)
	}
	file := decode(t, doc).(map[string]any)
	learn(decode(t, fixtureBytes(t, "answer-file-structured.json"))).check(t, "answer file", file)
	if file["contract_version"] != WorkerContractV2 || file["question_id"] != qid || file["answer_id"] != recorded["answer_id"] ||
		!reflect.DeepEqual(file["questions"], questions) || !reflect.DeepEqual(file["answers"], answers()) {
		t.Fatalf("answer file = %s", doc)
	}
	consumed := h.send(http.MethodGet, "/v2/questions/"+qid, "", "")
	consumed.expect(t, http.StatusOK, "question-answered-structured.json")
	if a := consumed.body["question"].(map[string]any)["answer"].(map[string]any); a["consumed_segment"] != float64(2) {
		t.Fatalf("answer after its segment's hello = %v", a)
	}

	// The legacy park still takes a legacy answer.
	answer(legacyRun, "legacy", map[string]any{"question_id": legacyQID, "answer": "The project."}).
		expect(t, http.StatusAccepted, "answer-response.json")
}
