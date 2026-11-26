package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	observercontract "shuttle/internal/observer"
)

// registerHeldObserver registers an observer as registerObserver does, then
// names the notebook Shuttle holds in place of the stand-in notebook command,
// and returns that notebook.
func registerHeldObserver(t *testing.T, f *liveFixture, name string, identity observercontract.Identity, questions []string, evidence []*observercontract.Bundle) observercontract.Held {
	t.Helper()
	registerObserver(t, f, name, identity, questions, evidence)
	registration := f.pipeline.Config.Observers[name]
	registration.Notebook = config.Command{}
	registration.HeldNotebook = true
	f.pipeline.Config.Observers[name] = registration
	dir, err := heldNotebookDir(f.root, identity)
	if err != nil {
		t.Fatal(err)
	}
	return observercontract.Held{Dir: dir, Identity: identity}
}

func registerHeldQuestion(t *testing.T, held observercontract.Held, question *observercontract.Question) {
	t.Helper()
	request := observercontract.Request{Identity: question.Identity, Revision: 0, RequestID: "register-" + question.ID}
	if _, err := held.Register(context.Background(), request, question); err != nil {
		t.Fatal(err)
	}
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestObserverRunUsesHeldNotebook(t *testing.T) {
	ctx := context.Background()
	f := newLiveFixture(t, "success")
	question := harnessQuestion(t)
	answer := harnessAssessment(t, "assessment.json")
	answer.Outcome = observercontract.OutcomeEffectObserved
	answer.Interpretation = "Every one of the three nights repeated a headline, which is the effect the question asks about."
	unprompted := harnessAssessment(t, "assessment-unprompted.json")
	held := registerHeldObserver(t, f, "fixture", question.Identity, []string{question.ID},
		[]*observercontract.Bundle{answer.Evidence, unprompted.Evidence})
	registerHeldQuestion(t, held, question)
	observerAgent(f, "fixture")
	installObserverModel(t, f, map[string]any{
		"silent": true, "assessments": []any{answer, unprompted}, "actions": []any{},
	}, "")

	run := f.claim(t, ledger.TriggerManual)
	result, err := f.pipeline.Run(ctx, f.def, run, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if held.Dir != filepath.Join(f.root, "notebooks", question.Identity.Domain, question.Identity.Observer) {
		t.Fatalf("held notebook at %s, want it under the state root by identity", held.Dir)
	}

	// The snapshot is the held notebook's: the registered question at the
	// revision its registration committed.
	var presentation observerPresentation
	readJSONFixture(t, filepath.Join(result.RunDir, "inputs", "observer.json"), &presentation)
	if presentation.NotebookRevision != 1 || len(presentation.Entries) != 1 || presentation.Entries[0].ID != question.ID ||
		!reflect.DeepEqual(presentation.Entries[0].Question, question) {
		t.Fatalf("model input = %+v, want the held notebook's registered question", presentation)
	}
	var snapshot observerSnapshot
	readJSONFixture(t, filepath.Join(result.RunDir, "observer", "notebook.json"), &snapshot)
	if snapshot.Revision != 1 || len(snapshot.Entries) != 1 || snapshot.Entries[0].ID != question.ID {
		t.Fatalf("retained notebook snapshot = %+v", snapshot)
	}

	// The assessments are recorded into it, one commit each.
	revision, err := held.Revision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if revision != 3 {
		t.Fatalf("held notebook revision %d, want the registration and both records", revision)
	}
	answered, err := held.Entry(ctx, question.ID)
	if err != nil {
		t.Fatal(err)
	}
	if answered.State != observercontract.StateAnswered || len(answered.Observations) != 1 ||
		canonicalJSON(answered.Observations[0]) != canonicalJSON(answer) {
		t.Fatalf("question entry = %+v, want it answered by the model's assessment", answered)
	}
	opened, err := held.Entry(ctx, unprompted.EntryID())
	if err != nil {
		t.Fatal(err)
	}
	if opened.Finding == nil || len(opened.Observations) != 1 || canonicalJSON(opened.Observations[0]) != canonicalJSON(unprompted) {
		t.Fatalf("finding entry = %+v, want it opened by the model's finding", opened)
	}
	if commits := gitOutput(t, held.Dir, "rev-list", "--count", "HEAD"); commits != "3" {
		t.Fatalf("%s commits in the held notebook, want 3", commits)
	}
	var applied observerApplied
	readJSONFixture(t, filepath.Join(result.RunDir, "applied.json"), &applied)
	if len(applied.Observations) != 2 || applied.NotebookRevision != 3 ||
		applied.Observations[0].RequestID != run.ID+"-assessment-001" || applied.Observations[0].RevisionBefore != 1 {
		t.Fatalf("applied = %+v, want both records under run-derived request ids", applied)
	}
	outcome := readObserverResult(t, result.RunDir)
	if outcome.NotebookStatus != "applied" || outcome.NotebookRevision != 3 || outcome.NotebookPush != observercontract.HeldNoRemote {
		t.Fatalf("observer-result.json = %+v, want it applied and the push skipped for want of a remote", outcome)
	}

	// The model never had the notebook to read.
	profile, err := readBounded(filepath.Join(result.RunDir, "srt.json"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Filesystem struct {
			DenyRead []string `json:"denyRead"`
		} `json:"filesystem"`
	}
	if err := json.Unmarshal(profile, &parsed); err != nil {
		t.Fatal(err)
	}
	denied := false
	for _, path := range parsed.Filesystem.DenyRead {
		if path == filepath.Join(f.root, notebooksDir) {
			denied = true
		}
	}
	if !denied {
		t.Fatalf("sandbox profile does not deny the held notebooks: %s", profile)
	}
}

func TestHeldNotebookPushPolicy(t *testing.T) {
	// heldRun runs one silent observer recording a finding into a held
	// notebook whose origin is remote, and returns the run's outcome.
	heldRun := func(t *testing.T, f *liveFixture, remote string) (observercontract.Held, observerResultRecord, *ledger.Run) {
		t.Helper()
		unprompted := harnessAssessment(t, "assessment-unprompted.json")
		held := registerHeldObserver(t, f, "fixture", unprompted.Identity, []string{}, []*observercontract.Bundle{unprompted.Evidence})
		// The repository exists once the notebook is first opened; the
		// operator adds its remote.
		if _, err := held.Revision(context.Background()); err != nil {
			t.Fatal(err)
		}
		gitOutput(t, held.Dir, "remote", "add", "origin", remote)
		observerAgent(f, "fixture")
		installObserverModel(t, f, map[string]any{
			"silent": true, "assessments": []any{unprompted}, "actions": []any{},
		}, "")
		run := f.claim(t, ledger.TriggerManual)
		result, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		row, err := f.store.Get(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.Status != ledger.StatusCompleted {
			t.Fatalf("run is %s, want completed", row.Status)
		}
		return held, readObserverResult(t, result.RunDir), row
	}

	t.Run("a remote is pushed by the end of the run", func(t *testing.T) {
		f := newLiveFixture(t, "success")
		remote := filepath.Join(f.root, "remote.git")
		if out, err := exec.Command("git", "init", "--quiet", "--bare", remote).CombinedOutput(); err != nil {
			t.Fatalf("git init --bare: %v: %s", err, out)
		}
		held, outcome, _ := heldRun(t, f, remote)
		if outcome.NotebookPush != observercontract.HeldPushed || outcome.NotebookPushError != "" {
			t.Fatalf("observer-result.json push = %q %q, want pushed", outcome.NotebookPush, outcome.NotebookPushError)
		}
		if local, pushed := gitOutput(t, held.Dir, "rev-parse", "HEAD"), gitOutput(t, remote, "rev-parse", "refs/heads/main"); local != pushed {
			t.Fatalf("remote main at %s, the notebook at %s", pushed, local)
		}
	})

	t.Run("a local-only registration is never pushed", func(t *testing.T) {
		// A local-only registration is refused before any cloud runtime, and
		// claude is the only one, so the policy is held at the push itself:
		// a notebook with commits and a reachable remote stays local.
		f := newLiveFixture(t, "success")
		remote := filepath.Join(f.root, "remote.git")
		if out, err := exec.Command("git", "init", "--quiet", "--bare", remote).CombinedOutput(); err != nil {
			t.Fatalf("git init --bare: %v: %s", err, out)
		}
		question := harnessQuestion(t)
		held := registerHeldObserver(t, f, "fixture", question.Identity, []string{question.ID}, nil)
		registerHeldQuestion(t, held, question)
		gitOutput(t, held.Dir, "remote", "add", "origin", remote)
		registration := f.pipeline.Config.Observers["fixture"]
		registration.DataPolicy = "local-only"
		state := &observerRunState{Registration: registration, Notebook: held}
		f.pipeline.pushObserverNotebook(context.Background(), state)
		if state.Push.Status != notebookPushLocalOnly || state.Push.Error != "" {
			t.Fatalf("push = %+v, want it withheld as local-only", state.Push)
		}
		if refs := gitOutput(t, remote, "for-each-ref"); refs != "" {
			t.Fatalf("remote holds %s, want nothing pushed", refs)
		}
		if err := validateObserverRegistration(registration, f.def.Manifest.Runtime.Kind, ""); err == nil || !strings.Contains(err.Error(), "local-only") {
			t.Fatalf("local-only under %s: %v, want it refused before any run", f.def.Manifest.Runtime.Kind, err)
		}
	})

	t.Run("a failed push is reported and the run completes", func(t *testing.T) {
		f := newLiveFixture(t, "success")
		var logs bytes.Buffer
		f.pipeline.Logger = log.New(&logs, "", 0)
		held, outcome, _ := heldRun(t, f, filepath.Join(f.root, "missing.git"))
		if outcome.NotebookPush != notebookPushFailed || !strings.Contains(outcome.NotebookPushError, held.Dir) ||
			!strings.Contains(outcome.NotebookPushError, "git push") {
			t.Fatalf("observer-result.json push = %q %q, want it failed naming the notebook and the command", outcome.NotebookPush, outcome.NotebookPushError)
		}
		if outcome.NotebookStatus != "applied" || outcome.NotebookRevision != 1 {
			t.Fatalf("observer-result.json = %+v, want the record applied regardless of the push", outcome)
		}
		if !strings.Contains(logs.String(), "observer notebook push") {
			t.Fatalf("log = %q, want the failed push logged", logs.String())
		}
	})
}

// Identities differing only in case reach one directory on a case-folding
// volume. A cloud run for the second must neither read the first's local-only
// notebook nor push it to the origin it carries.
func TestHeldNotebookRefusesAnAliasedOwner(t *testing.T) {
	ctx := context.Background()
	f := newLiveFixture(t, "success")
	question := harnessQuestion(t)
	owner := question.Identity
	alias := observercontract.Identity{Domain: owner.Domain, Observer: strings.ToUpper(owner.Observer)}
	ownerDir, err := heldNotebookDir(f.root, owner)
	if err != nil {
		t.Fatal(err)
	}
	local := observercontract.Held{Dir: ownerDir, Identity: owner}
	registerHeldQuestion(t, local, question)
	remote := filepath.Join(f.root, "remote.git")
	if out, err := exec.Command("git", "init", "--quiet", "--bare", remote).CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v: %s", err, out)
	}
	gitOutput(t, ownerDir, "remote", "add", "origin", remote)

	registerHeldObserver(t, f, "fixture", alias, []string{}, nil)
	aliasDir, err := heldNotebookDir(f.root, alias)
	if err != nil {
		t.Fatal(err)
	}
	// On a case-sensitive volume the alias is made explicit, so the test
	// holds whichever volume it runs on.
	if _, err := os.Stat(aliasDir); errors.Is(err, os.ErrNotExist) {
		if err := os.Symlink(ownerDir, aliasDir); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	observerAgent(f, "fixture")
	installObserverModel(t, f, map[string]any{"silent": true, "assessments": []any{}, "actions": []any{}}, "")
	refusal := func(err string) bool {
		return strings.Contains(err, aliasDir) && strings.Contains(err, owner.Domain+"/"+owner.Observer) &&
			strings.Contains(err, alias.Domain+"/"+alias.Observer)
	}

	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(ctx, f.def, run, Options{}); err == nil {
		t.Fatal("a run for the alias completed against the owner's notebook")
	}
	row, err := f.store.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	outcome := readObserverResult(t, row.RunDir.String)
	if outcome.Collection != "notebook-failed" || !refusal(outcome.Detail) {
		t.Fatalf("observer-result.json = %+v, want the snapshot refused naming the notebook and both identities", outcome)
	}

	registration := f.pipeline.Config.Observers["fixture"]
	notebook, err := newObserverNotebook(f.root, registration, alias, nil)
	if err != nil {
		t.Fatal(err)
	}
	state := &observerRunState{Registration: registration, Notebook: notebook}
	f.pipeline.pushObserverNotebook(ctx, state)
	if state.Push.Status != notebookPushFailed || !refusal(state.Push.Error) {
		t.Fatalf("push = %+v, want it refused naming the notebook and both identities", state.Push)
	}
	if refs := gitOutput(t, remote, "for-each-ref"); refs != "" {
		t.Fatalf("remote holds %s, want the owner's local-only notebook unpushed", refs)
	}
	if revision, err := local.Revision(ctx); err != nil || revision != 1 {
		t.Fatalf("owner's notebook at revision %d, %v; want its registration intact", revision, err)
	}
}

// A held notebook replaces the notebook command; a registration naming both
// is refused rather than one of them silently winning.
func TestHeldNotebookRegistrationNamesNoCommand(t *testing.T) {
	f := newLiveFixture(t, "success")
	question := harnessQuestion(t)
	registerHeldObserver(t, f, "fixture", question.Identity, []string{question.ID}, nil)
	registration := f.pipeline.Config.Observers["fixture"]
	if err := validateObserverRegistration(registration, f.def.Manifest.Runtime.Kind, ""); err != nil {
		t.Fatalf("held registration without a notebook command: %v", err)
	}
	registration.Notebook = registration.Collector
	if err := validateObserverRegistration(registration, f.def.Manifest.Runtime.Kind, ""); err == nil || !strings.Contains(err.Error(), "held_notebook") {
		t.Fatalf("held registration with a notebook command: %v, want it refused", err)
	}
	if _, err := heldNotebookDir(f.root, observercontract.Identity{Domain: "..", Observer: "mead"}); err == nil {
		t.Fatal("an identity of .. named a held notebook outside the notebooks root")
	}
}

// heldRequest composes a request against the held notebook's current
// revision, the way the door does for an author.
func heldRequest(t *testing.T, held observercontract.Held, identity observercontract.Identity, requestID string) observercontract.Request {
	t.Helper()
	revision, err := held.Revision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return observercontract.Request{Identity: identity, Revision: revision, RequestID: requestID}
}

// heldQuestion is the harness question under another name, by author.
func heldQuestion(t *testing.T, name string, author observercontract.Author) *observercontract.Question {
	t.Helper()
	question := harnessQuestion(t)
	question.ID = question.Identity.Qualify(name)
	question.Author = author
	return question
}

// silentObserverRun runs the fixture observer once, silent, on a slot of its
// own, and returns the snapshot and the presentation it retained.
func silentObserverRun(t *testing.T, f *liveFixture) (observerSnapshot, observerPresentation) {
	t.Helper()
	installObserverModel(t, f, map[string]any{"silent": true, "assessments": []any{}, "actions": []any{}}, "")
	f.slot = f.slot.Add(-24 * time.Hour)
	run := f.claim(t, ledger.TriggerManual)
	result, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var snapshot observerSnapshot
	readJSONFixture(t, filepath.Join(result.RunDir, "observer", "notebook.json"), &snapshot)
	var presentation observerPresentation
	readJSONFixture(t, filepath.Join(result.RunDir, "inputs", "observer.json"), &presentation)
	return snapshot, presentation
}

func entryIDs(entries []*observercontract.Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}
	return ids
}

// A question registered once the definition is written — by a worker when its
// ticket lands, here — is read by the next run, though the definition's
// questions list never names it.
func TestRunSnapshotReadsEveryObservingEntry(t *testing.T) {
	ctx := context.Background()
	f := newLiveFixture(t, "success")
	standing := harnessQuestion(t)
	held := registerHeldObserver(t, f, "fixture", standing.Identity, []string{standing.ID},
		[]*observercontract.Bundle{observedAnswer(t).Evidence})
	registerHeldQuestion(t, held, standing)
	observerAgent(f, "fixture")

	snapshot, _ := silentObserverRun(t, f)
	if got := entryIDs(snapshot.Entries); !reflect.DeepEqual(got, []string{standing.ID}) {
		t.Fatalf("first snapshot = %v, want the one registered question", got)
	}

	landed := heldQuestion(t, "landed-change", observercontract.Author{Kind: observercontract.AuthorWorker, Ref: "shuttle/landed-1234"})
	if _, err := held.Register(ctx, heldRequest(t, held, landed.Identity, "register-landed"), landed); err != nil {
		t.Fatal(err)
	}
	snapshot, presentation := silentObserverRun(t, f)
	want := []string{standing.ID, landed.ID}
	if got := entryIDs(snapshot.Entries); !reflect.DeepEqual(got, want) || snapshot.Revision != 2 {
		t.Fatalf("next snapshot = %v at revision %d, want %v at 2", got, snapshot.Revision, want)
	}
	if got := entryIDs(presentation.Entries); !reflect.DeepEqual(got, want) {
		t.Fatalf("model input entries = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(snapshot.Entries[1].Question, landed) {
		t.Fatalf("snapshot entry = %+v, want the worker's question as registered", snapshot.Entries[1].Question)
	}
}

// A retired entry is not read. An answered one is not read either, until a
// reopen returns it to observing.
func TestRunSnapshotLeavesOutRetired(t *testing.T) {
	ctx := context.Background()
	f := newLiveFixture(t, "success")
	human := observercontract.Author{Kind: observercontract.AuthorHuman, Ref: "steve"}
	observing := heldQuestion(t, "a-observing", human)
	retired := heldQuestion(t, "b-retired", human)
	answered := heldQuestion(t, "c-answered", human)
	reopened := heldQuestion(t, "d-reopened", human)
	held := registerHeldObserver(t, f, "fixture", observing.Identity,
		[]string{observing.ID, retired.ID, answered.ID, reopened.ID}, []*observercontract.Bundle{observedAnswer(t).Evidence})
	for _, question := range []*observercontract.Question{observing, retired, answered, reopened} {
		if _, err := held.Register(ctx, heldRequest(t, held, question.Identity, "register-"+question.ID), question); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := held.Retire(ctx, heldRequest(t, held, retired.Identity, "retire"), retired.ID, "asked again as a narrower question"); err != nil {
		t.Fatal(err)
	}
	for _, question := range []*observercontract.Question{answered, reopened} {
		answer := observedAnswer(t)
		answer.QuestionID = question.ID
		if _, err := held.Record(ctx, heldRequest(t, held, question.Identity, "answer-"+question.ID), answer); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := held.Reopen(ctx, heldRequest(t, held, reopened.Identity, "reopen"), reopened.ID, observercontract.ReopenReactivated); err != nil {
		t.Fatal(err)
	}
	observerAgent(f, "fixture")

	snapshot, presentation := silentObserverRun(t, f)
	want := []string{observing.ID, reopened.ID}
	if got := entryIDs(snapshot.Entries); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot = %v, want only the observing and the reopened entry", got)
	}
	if got := entryIDs(presentation.Entries); !reflect.DeepEqual(got, want) {
		t.Fatalf("model input entries = %v, want %v", got, want)
	}
	// The notebook still holds all four: leaving one out of a run is not
	// losing it.
	listed, err := held.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 4 {
		t.Fatalf("notebook lists %v, want all four entries kept", entryIDs(listed))
	}
}

// Every observing entry is read, so the model's input can outgrow its limit
// on history alone. It is bounded as before — the globally oldest history
// dropped first — and records how much it dropped; the retained snapshot
// keeps all of it.
func TestRunSnapshotBoundedWithLosses(t *testing.T) {
	ctx := context.Background()
	f := newLiveFixture(t, "success")
	human := observercontract.Author{Kind: observercontract.AuthorHuman, Ref: "steve"}
	first := heldQuestion(t, "first", human)
	second := heldQuestion(t, "second", human)
	evidence := observedAnswer(t).Evidence
	held := registerHeldObserver(t, f, "fixture", first.Identity, []string{}, []*observercontract.Bundle{evidence})
	const limit = 16 << 10
	registration := f.pipeline.Config.Observers["fixture"]
	registration.MaxModelInputBytes = limit
	f.pipeline.Config.Observers["fixture"] = registration
	start := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	recorded := 0
	for i, question := range []*observercontract.Question{first, second} {
		if _, err := held.Register(ctx, heldRequest(t, held, question.Identity, "register-"+question.ID), question); err != nil {
			t.Fatal(err)
		}
		// Insufficient evidence answers nothing, so the entry stays observing
		// and its history grows night by night. The two entries' nights
		// interleave, so the oldest history is spread across both.
		for night := 0; night < 6; night++ {
			reading := harnessAssessment(t, "assessment.json")
			reading.QuestionID = question.ID
			reading.Outcome = observercontract.OutcomeInsufficient
			reading.At = start.AddDate(0, 0, 2*night+i).Format(time.RFC3339)
			if _, err := held.Record(ctx, heldRequest(t, held, question.Identity, fmt.Sprintf("record-%s-%d", question.ID, night)), reading); err != nil {
				t.Fatal(err)
			}
			recorded++
		}
	}
	observerAgent(f, "fixture")

	installObserverModel(t, f, map[string]any{"silent": true, "assessments": []any{}, "actions": []any{}}, "")
	run := f.claim(t, ledger.TriggerManual)
	result, err := f.pipeline.Run(ctx, f.def, run, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var snapshot observerSnapshot
	readJSONFixture(t, filepath.Join(result.RunDir, "observer", "notebook.json"), &snapshot)
	retained := 0
	for _, entry := range snapshot.Entries {
		retained += len(entry.Observations)
	}
	if got := entryIDs(snapshot.Entries); !reflect.DeepEqual(got, []string{first.ID, second.ID}) || retained != recorded {
		t.Fatalf("retained snapshot = %v with %d observations, want both entries with all %d", got, retained, recorded)
	}

	path := filepath.Join(result.RunDir, "inputs", "observer.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > limit {
		t.Fatalf("model input is %d bytes, over the %d-byte limit", info.Size(), limit)
	}
	var presentation observerPresentation
	readJSONFixture(t, path, &presentation)
	if got := entryIDs(presentation.Entries); !reflect.DeepEqual(got, []string{first.ID, second.ID}) {
		t.Fatalf("model input entries = %v, want both questions kept", got)
	}
	shown := 0
	var oldestShown time.Time
	for _, entry := range presentation.Entries {
		for _, observation := range entry.Observations {
			if observation == nil {
				continue
			}
			at, err := time.Parse(time.RFC3339, observation.At)
			if err != nil {
				t.Fatal(err)
			}
			if shown == 0 || at.Before(oldestShown) {
				oldestShown = at
			}
			shown++
		}
	}
	if presentation.OmittedHistory == 0 || shown+presentation.OmittedHistory != recorded {
		t.Fatalf("model input shows %d observations and omits %d, want a recorded loss accounting for all %d",
			shown, presentation.OmittedHistory, recorded)
	}
	// What was dropped is the globally oldest: every retained observation
	// older than the oldest one shown is one the presentation omitted.
	older := 0
	for _, entry := range snapshot.Entries {
		for _, observation := range entry.Observations {
			at, err := time.Parse(time.RFC3339, observation.At)
			if err != nil {
				t.Fatal(err)
			}
			if at.Before(oldestShown) {
				older++
			}
		}
	}
	if older != presentation.OmittedHistory {
		t.Fatalf("%d observations are older than the oldest shown, %d omitted; want the omitted history to be the oldest", older, presentation.OmittedHistory)
	}
	if presentation.OmittedEvidence != 0 || presentation.TruncatedExploration || len(presentation.Evidence) != 1 {
		t.Fatalf("model input = evidence %d (omitted %d), truncated %t; want history dropped before evidence or prose",
			len(presentation.Evidence), presentation.OmittedEvidence, presentation.TruncatedExploration)
	}
}
