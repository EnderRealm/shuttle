package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	observercontract "shuttle/internal/observer"
)

// doorQuestionTicket is the harness question's provenance ticket.
const doorQuestionTicket = "shuttle/vary-report-headline-1a2b"

// doorTK is a temp tk central store with the shuttle project registered, and
// the harness question's provenance ticket parked in it on that question.
type doorTK struct {
	config  string
	central string
}

func newDoorTK(t *testing.T) *doorTK {
	t.Helper()
	root := t.TempDir()
	// pkg/ticket keeps its store locks under the user cache directory and its
	// mutation log under ~/.ticket, both derived from HOME.
	t.Setenv("HOME", root)
	central := filepath.Join(root, "tk-store")
	for _, dir := range []string{filepath.Join(central, "tickets", "shuttle"), filepath.Join(root, "code", "shuttle"), filepath.Join(root, ".ticket")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tk := &doorTK{config: filepath.Join(root, ".ticket", "config.yaml"), central: central}
	body := "central_root: " + central + "\nprojects:\n  shuttle:\n    path: " + filepath.Join(root, "code", "shuttle") + "\n"
	if err := os.WriteFile(tk.config, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	tk.write(t)
	return tk
}

// write creates the provenance ticket in backlog, waiting on the question.
func (tk *doorTK) write(t *testing.T) {
	t.Helper()
	now := time.Now().UTC()
	_, bare := ticket.ParseNamespacedID(doorQuestionTicket)
	data, err := ticket.Serialize(&ticket.Ticket{
		ID: bare, Status: ticket.StatusBacklog, Type: ticket.TypeFeature, Priority: 2,
		Title:   "Vary the nightly summary heading",
		Body:    "Why: the nightly summary keeps its heading from one night to the next.\n\n## Acceptance Criteria\n\n- The heading varies night to night.\n",
		Extra:   map[string]string{ticket.WaitingOnField: doorIdentity.Qualify("days-repeating")},
		Created: now, Updated: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tk.file(), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// park holds the provenance ticket in backlog on the question again, the way
// a person parks it, keeping everything else the ticket carries.
func (tk *doorTK) park(t *testing.T) {
	t.Helper()
	tkt := tk.get(t)
	tkt.Status = ticket.StatusBacklog
	if tkt.Extra == nil {
		tkt.Extra = map[string]string{}
	}
	tkt.Extra[ticket.WaitingOnField] = doorIdentity.Qualify("days-repeating")
	if err := ticket.WithSource(ticket.NewMultiStore(filepath.Join(tk.central, "tickets")), "human").Update(tkt); err != nil {
		t.Fatal(err)
	}
}

func (tk *doorTK) file() string {
	_, bare := ticket.ParseNamespacedID(doorQuestionTicket)
	return filepath.Join(tk.central, "tickets", "shuttle", bare+".md")
}

func (tk *doorTK) get(t *testing.T) *ticket.Ticket {
	t.Helper()
	tkt, err := ticket.NewMultiStore(filepath.Join(tk.central, "tickets")).Get(doorQuestionTicket)
	if err != nil {
		t.Fatal(err)
	}
	return tkt
}

// read is the ticket file's bytes, which a door call that authorizes nothing
// leaves exactly as they were.
func (tk *doorTK) read(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(tk.file())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// recordInsufficient records the observer's own insufficient-evidence reading
// as a run would, leaving the question observing.
func (e *doorEnv) recordInsufficient(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	assessment, err := observercontract.ValidateAssessment([]byte(doorPayload(t, "assessment.json", func(a map[string]any) {
		a["outcome"] = observercontract.OutcomeInsufficient
	})))
	if err != nil {
		t.Fatal(err)
	}
	revision, err := e.held.Revision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.held.Record(ctx, observercontract.Request{Identity: doorIdentity, Revision: revision, RequestID: "run-record"}, assessment); err != nil {
		t.Fatal(err)
	}
}

// release decodes an observe answer's release.
func release(t *testing.T, answer map[string]any) map[string]any {
	t.Helper()
	got, ok := answer["release"].(map[string]any)
	if !ok {
		t.Fatalf("answer %v carries no release", answer)
	}
	return got
}

func TestPersonObservationRecordedAsHuman(t *testing.T) {
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	env.recordInsufficient(t)

	out, err := execObserver(t, doorPayload(t, "assessment-person.json", nil), "observe", "--request-id", "seen")
	if err != nil {
		t.Fatalf("observe: %v\n%s", err, env.logs.String())
	}
	// A person's effect-observed answers the question as the observer's would.
	answer := decodeAnswer(t, out, "entry_id", "state", "release")
	if answer["entry_id"] != id || answer["state"] != observercontract.StateAnswered {
		t.Fatalf("answer = %v, want %s answered", answer, id)
	}
	entry := env.entry(t, id)
	if entry.State != observercontract.StateAnswered || len(entry.Observations) != 2 {
		t.Fatalf("entry = %s with %d observations, want answered with the observer's and the person's", entry.State, len(entry.Observations))
	}
	observer, person := entry.Observations[0], entry.Observations[1]
	if observer.Kind() != observercontract.ObservationAssessment || observer.Author != nil || observer.Evidence == nil {
		t.Fatalf("first row = %s by %+v, want the observer's own with its bundle", observer.Kind(), observer.Author)
	}
	want, err := observercontract.ValidateAssessment([]byte(doorPayload(t, "assessment-person.json", nil)))
	if err != nil {
		t.Fatal(err)
	}
	if person.Kind() != observercontract.ObservationPerson || person.Author == nil ||
		*person.Author != (observercontract.Author{Kind: observercontract.AuthorHuman, Ref: "steve"}) {
		t.Fatalf("second row = %s by %+v, want a person's by steve", person.Kind(), person.Author)
	}
	if !slices.Equal(person.Facts, want.Facts) || person.At != want.At || person.Evidence != nil || person.Outcome != want.Outcome {
		t.Fatalf("second row = %+v, want the facts, time and outcome as stated and no bundle", person)
	}
	env.requireLog(t, `operation="observe" request="seen" entry="harness/shuttle/days-repeating" outcome=ok`)

	// Anything that is not a person's own statement is refused naming the
	// field, and writes nothing.
	before := env.commits(t)
	for _, refusal := range []struct {
		name  string
		edit  func(map[string]any)
		field string
	}{
		{name: "by a worker", field: "author.kind", edit: func(a map[string]any) {
			a["author"] = map[string]any{"kind": observercontract.AuthorWorker, "ref": "shuttle/vary-report-headline-1a2b"}
		}},
		{name: "by nobody", field: "author.kind", edit: func(a map[string]any) { delete(a, "author") }},
		{name: "with no facts", field: "facts", edit: func(a map[string]any) { delete(a, "facts") }},
		{name: "with a bundle", field: "evidence", edit: func(a map[string]any) {
			var assessment map[string]any
			if err := json.Unmarshal([]byte(doorPayload(t, "assessment.json", nil)), &assessment); err != nil {
				t.Fatal(err)
			}
			a["evidence"] = assessment["evidence"]
		}},
		{name: "with no time", field: "at", edit: func(a map[string]any) { delete(a, "at") }},
		{name: "carrying a correction", field: "corrects", edit: func(a map[string]any) {
			a["corrects"] = map[string]any{"observation": 1, "wrong": "it was wrong"}
		}},
	} {
		_, err := execObserver(t, doorPayload(t, "assessment-person.json", refusal.edit), "observe", "--request-id", "refused-"+refusal.name)
		if err == nil || !strings.Contains(err.Error(), "observer "+refusal.field) || exitCode(err) != 1 {
			t.Fatalf("observe %s = %v (exit %d), want a refusal naming %s", refusal.name, err, exitCode(err), refusal.field)
		}
	}
	if after := env.commits(t); after != before {
		t.Fatalf("commits %s -> %s, want the refusals to write nothing", before, after)
	}
	if n := len(env.entry(t, id).Observations); n != 2 {
		t.Fatalf("%d observations after the refusals, want 2", n)
	}
}

func TestCorrectionKeepsAndMarksEarlier(t *testing.T) {
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	env.recordInsufficient(t)
	earlier, err := json.Marshal(env.entry(t, id).Observations[0])
	if err != nil {
		t.Fatal(err)
	}

	correction := doorPayload(t, "assessment-correction.json", nil)
	out, err := execObserver(t, correction, "correct", "--request-id", "correct")
	if err != nil {
		t.Fatalf("correct: %v\n%s", err, env.logs.String())
	}
	// A correction moves no state: its effect-observed would otherwise have
	// answered the question.
	answer := decodeAnswer(t, out, "entry_id", "state")
	if answer["entry_id"] != id || answer["state"] != observercontract.StateObserving {
		t.Fatalf("answer = %v, want %s still observing", answer, id)
	}
	entry := env.entry(t, id)
	if len(entry.Observations) != 2 || entry.State != observercontract.StateObserving {
		t.Fatalf("entry = %s with %d observations, want the earlier row and the correction, observing", entry.State, len(entry.Observations))
	}
	if kept, _ := json.Marshal(entry.Observations[0]); !bytes.Equal(kept, earlier) {
		t.Fatalf("corrected row = %s, want it exactly as recorded: %s", kept, earlier)
	}
	appended := entry.Observations[1]
	if appended.Kind() != observercontract.ObservationCorrection ||
		*appended.Corrects != (observercontract.Correction{Observation: 1, Wrong: "counted the 2026-09-06 rerun as a nightly report that repeated a headline"}) ||
		appended.Author.Kind != observercontract.AuthorHuman {
		t.Fatalf("appended row = %s %+v by %+v, want a person's correction naming observation 1 and what was wrong", appended.Kind(), appended.Corrects, appended.Author)
	}
	if !slices.Equal(entry.Corrected, []int{1}) {
		t.Fatalf("corrected = %v, want observation 1 marked", entry.Corrected)
	}

	// The mark is derived on read, not stored: the entry's file holds the rows
	// alone, and a notebook loaded afresh from it marks the same row.
	files, err := filepath.Glob(filepath.Join(env.held.Dir, "entries", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("entry files = %v, %v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"corrected"`)) {
		t.Fatalf("entry file stores the mark:\n%s", data)
	}
	reloaded, err := observercontract.Held{Dir: env.held.Dir, Identity: doorIdentity}.Entry(context.Background(), id)
	if err != nil || !slices.Equal(reloaded.Corrected, []int{1}) {
		t.Fatalf("reloaded corrected = %v, %v, want observation 1 marked", reloaded, err)
	}

	// A replay answers the original and writes nothing.
	before := env.commits(t)
	replay, err := execObserver(t, correction, "correct", "--request-id", "correct")
	if err != nil || replay != out {
		t.Fatalf("replay = %q, %v, want the original %q", replay, err, out)
	}
	if after := env.commits(t); after != before {
		t.Fatalf("commits %s -> %s, want a replay to write nothing", before, after)
	}

	// A correction names a row the entry holds, and never another correction:
	// the row that was believed is the one corrected.
	for _, refusal := range []struct {
		name  string
		edit  func(map[string]any)
		field string
	}{
		{name: "of a correction", field: "corrects.observation", edit: func(c map[string]any) {
			c["corrects"] = map[string]any{"observation": 2, "wrong": "the correction was wrong too"}
		}},
		{name: "of a row not recorded", field: "corrects.observation", edit: func(c map[string]any) {
			c["corrects"] = map[string]any{"observation": 7, "wrong": "no such row"}
		}},
		{name: "saying nothing was wrong", field: "corrects.wrong", edit: func(c map[string]any) {
			c["corrects"] = map[string]any{"observation": 1}
		}},
		{name: "by the observer", field: "author.kind", edit: func(c map[string]any) {
			c["author"] = map[string]any{"kind": observercontract.AuthorObserver}
		}},
	} {
		_, err := execObserver(t, doorPayload(t, "assessment-correction.json", refusal.edit), "correct", "--request-id", "refused-"+refusal.name)
		if err == nil || !strings.Contains(err.Error(), "observer "+refusal.field) {
			t.Fatalf("correct %s = %v, want a refusal naming %s", refusal.name, err, refusal.field)
		}
	}
	if after := env.commits(t); after != before {
		t.Fatalf("commits %s -> %s, want the refusals to write nothing", before, after)
	}
}

func TestCarriedOverReadingKeepsDateAndSource(t *testing.T) {
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	// Two readings carried in from the file they were first written to, one
	// dated by its day alone and one by the instant, both after the fact.
	carried := doorPayload(t, "assessment-carried.json", nil)
	out, err := execObserver(t, carried, "carry", "--request-id", "carry-1")
	if err != nil {
		t.Fatalf("carry: %v\n%s", err, env.logs.String())
	}
	decodeAnswer(t, out, "entry_id", "state")
	out, err = execObserver(t, doorPayload(t, "assessment-carried.json", func(a map[string]any) {
		a["outcome"] = observercontract.OutcomeEffectObserved
		a["interpretation"] = "No headline repeated across the three nights after the change."
		a["at"] = "2026-09-07T06:00:00-07:00"
	}), "carry", "--request-id", "carry-2")
	if err != nil {
		t.Fatalf("carry: %v", err)
	}
	// Recorded, and nothing moved: the store it came from applied it, if at all.
	answer := decodeAnswer(t, out, "entry_id", "state")
	if answer["entry_id"] != id || answer["state"] != observercontract.StateObserving {
		t.Fatalf("answer = %v, want %s still observing", answer, id)
	}

	// Read from a notebook loaded afresh, as a later run reads it.
	entry, err := observercontract.Held{Dir: env.held.Dir, Identity: doorIdentity}.Entry(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if entry.State != observercontract.StateObserving || len(entry.Observations) != 2 {
		t.Fatalf("entry = %s with %d observations, want both readings and observing", entry.State, len(entry.Observations))
	}
	for i, at := range []string{"2026-08-28", "2026-09-07T06:00:00-07:00"} {
		reading := entry.Observations[i]
		if reading.Kind() != observercontract.ObservationCarried || reading.At != at ||
			reading.CarriedOver == nil || reading.CarriedOver.Source != "~/.shuttle/observer/readings.toml#days-repeating" ||
			*reading.Author != (observercontract.Author{Kind: observercontract.AuthorObserver, Ref: "shuttle"}) {
			t.Fatalf("observation %d = %s at %q from %+v by %+v, want the reading at %q from its source as its author declared",
				i+1, reading.Kind(), reading.At, reading.CarriedOver, reading.Author, at)
		}
	}
	env.requireLog(t, `operation="carry" request="carry-1" entry="harness/shuttle/days-repeating" outcome=ok`)

	for _, refusal := range []struct {
		name  string
		edit  func(map[string]any)
		field string
	}{
		{name: "with no source", field: "carried_over.source", edit: func(a map[string]any) {
			a["carried_over"] = map[string]any{"source": ""}
		}},
		{name: "with no author", field: "author.kind", edit: func(a map[string]any) { delete(a, "author") }},
		{name: "with no date", field: "at", edit: func(a map[string]any) { delete(a, "at") }},
		{name: "dated by no calendar", field: "at", edit: func(a map[string]any) { a["at"] = "late August" }},
	} {
		_, err := execObserver(t, doorPayload(t, "assessment-carried.json", refusal.edit), "carry", "--request-id", "refused-"+refusal.name)
		if err == nil || !strings.Contains(err.Error(), "observer "+refusal.field) {
			t.Fatalf("carry %s = %v, want a refusal naming %s", refusal.name, err, refusal.field)
		}
	}
	// A carried reading goes through carry, not observe.
	_, err = execObserver(t, carried, "observe", "--request-id", "observe-carried")
	if err == nil || !strings.Contains(err.Error(), "observer carried_over") {
		t.Fatalf("observe of a carried reading = %v, want a refusal naming carried_over", err)
	}
}

func TestPersonObservedAuthorizesAction(t *testing.T) {
	tk := newDoorTK(t)
	env := startObserverDoorWith(t, tk.config)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}

	observed := doorPayload(t, "assessment-person.json", nil)
	out, err := execObserver(t, observed, "observe", "--request-id", "seen")
	if err != nil {
		t.Fatalf("observe: %v\n%s", err, env.logs.String())
	}
	answer := decodeAnswer(t, out, "entry_id", "state", "release")
	if got := release(t, answer); !reflect.DeepEqual(got, map[string]any{"status": observercontract.ReceiptApplied, "ticket": doorQuestionTicket}) {
		t.Fatalf("release = %v, want applied on %s", got, doorQuestionTicket)
	}
	// The release a run's effect-observed would apply: waiting_on cleared,
	// ready, one note and the receipt under the fixed observer id.
	released := tk.get(t)
	if released.Status != ticket.StatusReady || ticket.WaitingOn(released) != "" || len(released.Notes) != 1 || len(released.Actions) != 1 {
		t.Fatalf("ticket = %s, waiting_on %q, %d notes, %d receipts; want it released once",
			released.Status, ticket.WaitingOn(released), len(released.Notes), len(released.Actions))
	}
	if released.Actions[0].ID != "observer-release" || released.Actions[0].Outcome != string(ticket.StatusReady) {
		t.Fatalf("receipt = %+v, want observer-release to ready", released.Actions[0])
	}
	note := released.Notes[0].Text
	for _, want := range []string{"Observed by steve (human) on " + id + ": effect-observed at 2026-09-10T07:40:00-07:00",
		"observer door request seen", "- The 2026-09-09 report's headline names the ledger"} {
		if !strings.Contains(note, want) {
			t.Fatalf("note lacks %q:\n%s", want, note)
		}
	}
	env.requireLog(t, `observer door: request="seen" entry="harness/shuttle/days-repeating" release=applied ticket="shuttle/vary-report-headline-1a2b"`)

	// A repeat of the call composes the same note, and tk answers it as the
	// write that already landed rather than a second release.
	replay, err := execObserver(t, observed, "observe", "--request-id", "seen")
	if err != nil || replay != out {
		t.Fatalf("replay = %q, %v, want the original %q", replay, err, out)
	}
	if again := tk.get(t); len(again.Notes) != 1 || len(again.Actions) != 1 {
		t.Fatalf("ticket after the replay has %d notes, %d receipts; want the one release", len(again.Notes), len(again.Actions))
	}

	// Parked again by a person, another person's observation is recorded, and
	// the ticket's own receipt refuses a second release.
	tk.park(t)
	out, err = execObserver(t, doorPayload(t, "assessment-person.json", func(a map[string]any) {
		a["author"] = map[string]any{"kind": observercontract.AuthorHuman, "ref": "dana"}
	}), "observe", "--request-id", "seen-again")
	if err != nil {
		t.Fatalf("second observe: %v", err)
	}
	got := release(t, decodeAnswer(t, out, "entry_id", "state", "release"))
	if got["status"] != observercontract.ReceiptRefused || got["reason"] != "the ticket was already released once by an observer" {
		t.Fatalf("second release = %v, want it refused by the ticket's receipt", got)
	}
	if n := len(env.entry(t, id).Observations); n != 2 {
		t.Fatalf("%d observations, want the refused release to leave the observation recorded", n)
	}
	if still := tk.get(t); still.Status != ticket.StatusBacklog || ticket.WaitingOn(still) == "" {
		t.Fatalf("ticket = %s waiting on %q, want it left parked", still.Status, ticket.WaitingOn(still))
	}
	env.requireLog(t, `request="seen-again" entry="harness/shuttle/days-repeating" release=refused`)
}

func TestNonRunObservationAuthorizesNothingElse(t *testing.T) {
	tk := newDoorTK(t)
	env := startObserverDoorWith(t, tk.config)
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	parked := tk.read(t)
	unchanged := func(name string) {
		t.Helper()
		if !bytes.Equal(tk.read(t), parked) {
			t.Fatalf("%s wrote the parked ticket:\n%s", name, tk.read(t))
		}
	}

	// A carried-over reading of the very event the ticket waits on records
	// and authorizes nothing, whoever first wrote it.
	for _, author := range []string{observercontract.AuthorHuman, observercontract.AuthorWorker, observercontract.AuthorObserver} {
		out, err := execObserver(t, doorPayload(t, "assessment-carried.json", func(a map[string]any) {
			a["author"] = map[string]any{"kind": author}
			a["outcome"] = observercontract.OutcomeEffectObserved
		}), "carry", "--request-id", "carry-"+author)
		if err != nil {
			t.Fatalf("carry by %s: %v", author, err)
		}
		decodeAnswer(t, out, "entry_id", "state")
		unchanged("a carried-over reading by " + author)
	}

	// Nor does a correction saying the event was observed.
	out, err := execObserver(t, doorPayload(t, "assessment-correction.json", nil), "correct", "--request-id", "correct")
	if err != nil {
		t.Fatalf("correct: %v", err)
	}
	decodeAnswer(t, out, "entry_id", "state")
	unchanged("a correction")

	// A person's outcome other than effect-observed is recorded and answers
	// that no release followed.
	for _, outcome := range []string{observercontract.OutcomeInsufficient, observercontract.OutcomeEffectAbsent} {
		out, err := execObserver(t, doorPayload(t, "assessment-person.json", func(a map[string]any) {
			a["outcome"] = outcome
		}), "observe", "--request-id", "observe-"+outcome)
		if err != nil {
			t.Fatalf("observe %s: %v", outcome, err)
		}
		if got := release(t, decodeAnswer(t, out, "entry_id", "state", "release")); !reflect.DeepEqual(got, map[string]any{"status": observercontract.DoorReleaseNone}) {
			t.Fatalf("release on %s = %v, want none", outcome, got)
		}
		unchanged("a person's " + outcome)
		env.requireLog(t, `request="observe-`+outcome+`" entry="harness/shuttle/days-repeating" release=none`)
	}
	entry := env.entry(t, doorIdentity.Qualify("days-repeating"))
	if len(entry.Observations) != 6 {
		t.Fatalf("%d observations, want every one recorded", len(entry.Observations))
	}

	// The ticket was releasable throughout: a person's effect-observed is what
	// releases it.
	out, err = execObserver(t, doorPayload(t, "assessment-person.json", nil), "observe", "--request-id", "observe-observed")
	if err != nil {
		t.Fatal(err)
	}
	if got := release(t, decodeAnswer(t, out, "entry_id", "state", "release")); got["status"] != observercontract.ReceiptApplied {
		t.Fatalf("release on effect-observed = %v, want applied", got)
	}
	if tk.get(t).Status != ticket.StatusReady {
		t.Fatal("the parked ticket was not released by a person's effect-observed")
	}
}

func TestObserverPersonObservationIsTheOperators(t *testing.T) {
	for _, peer := range []struct {
		name     string
		operator func() int
	}{
		// The daemon's operator is another uid than the one calling: the
		// caller is a hosted agent user as far as the door can tell.
		{name: "another uid", operator: func() int { return os.Getuid() + 1 }},
		{name: "the operator", operator: os.Getuid},
	} {
		t.Run(peer.name, func(t *testing.T) {
			restore := doorOperator
			doorOperator = peer.operator
			t.Cleanup(func() { doorOperator = restore })
			tk := newDoorTK(t)
			env := startObserverDoorWith(t, tk.config)
			id := doorIdentity.Qualify("days-repeating")
			// Registering and carrying keep today's rule: any peer, its uid a
			// record.
			if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
				t.Fatal(err)
			}
			if _, err := execObserver(t, doorPayload(t, "assessment-carried.json", nil), "carry", "--request-id", "carry"); err != nil {
				t.Fatalf("carry: %v", err)
			}
			parked := tk.read(t)
			before := env.commits(t)

			observe, observeErr := execObserver(t, doorPayload(t, "assessment-person.json", nil), "observe", "--request-id", "seen")
			_, correctErr := execObserver(t, doorPayload(t, "assessment-correction.json", nil), "correct", "--request-id", "correct")
			if peer.name == "the operator" {
				if observeErr != nil || correctErr != nil {
					t.Fatalf("operator's observe = %v, correct = %v; want both recorded", observeErr, correctErr)
				}
				if got := release(t, decodeAnswer(t, observe, "entry_id", "state", "release")); got["status"] != observercontract.ReceiptApplied {
					t.Fatalf("release = %v, want applied", got)
				}
				if n := len(env.entry(t, id).Observations); n != 3 || tk.get(t).Status != ticket.StatusReady {
					t.Fatalf("%d observations, ticket %s; want both recorded and the ticket released", n, tk.get(t).Status)
				}
				return
			}
			for _, refused := range []struct {
				operation, request string
				err                error
			}{
				{operation: "observe", request: "seen", err: observeErr},
				{operation: "correct", request: "correct", err: correctErr},
			} {
				if refused.err == nil || !strings.Contains(refused.err.Error(), "observer author.kind") || exitCode(refused.err) != 1 {
					t.Fatalf("%s from another uid = %v (exit %d), want a refusal naming author.kind", refused.operation, refused.err, exitCode(refused.err))
				}
				env.requireLog(t, `operation="`+refused.operation+`" request="`+refused.request+`" entry="" outcome=invalid exit=0 field="author.kind"`)
			}
			if after := env.commits(t); after != before {
				t.Fatalf("commits %s -> %s, want the refusals to write nothing", before, after)
			}
			if n := len(env.entry(t, id).Observations); n != 1 {
				t.Fatalf("%d observations, want only the carried reading", n)
			}
			if !bytes.Equal(tk.read(t), parked) {
				t.Fatalf("the refused observe wrote the parked ticket:\n%s", tk.read(t))
			}
		})
	}
}
