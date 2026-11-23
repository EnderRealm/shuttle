package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
