package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	observercontract "shuttle/internal/observer"
	"shuttle/internal/output"
)

// observerNotebookState is the stand-in adapter's store: a request log keyed
// by observer-scoped request id, the entry each request returned, and the
// current entries by id. CrashOnce makes the next record commit and then exit
// without a response, the uncertain outcome the runner replays.
type observerNotebookState struct {
	Revision  int64                      `json:"revision"`
	Records   int                        `json:"records"`
	CrashOnce bool                       `json:"crash_once,omitempty"`
	Requests  map[string]json.RawMessage `json:"requests"`
	Responses map[string]json.RawMessage `json:"responses"`
	Entries   map[string]json.RawMessage `json:"entries"`
}

func emptyNotebookState() observerNotebookState {
	return observerNotebookState{
		Requests: map[string]json.RawMessage{}, Responses: map[string]json.RawMessage{},
		Entries: map[string]json.RawMessage{},
	}
}

// observerFixture is one registered observer on a live fixture: its
// definition, the stand-in collector and notebook, and the files they read.
type observerFixture struct {
	name       string
	identity   observercontract.Identity
	evidence   string
	state      string
	definition string
}

func harnessAssessment(t *testing.T, file string) *observercontract.Assessment {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", file))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(data)
	if err != nil {
		t.Fatal(err)
	}
	return assessment
}

func helperCommand(root, mode string, args ...string) config.Command {
	return config.Command{
		Argv:       append([]string{os.Args[0], "-test.run=TestObserverRunnerHelper", "--", mode}, args...),
		WorkingDir: root,
	}
}

// registerObserver writes the definition, evidence and notebook state for one
// observer under the fixture root and registers it in config.json's map. The
// collector returns every bundle in evidence as an observed collection.
func registerObserver(t *testing.T, f *liveFixture, name string, identity observercontract.Identity, questions []string, evidence []*observercontract.Bundle) *observerFixture {
	t.Helper()
	o := &observerFixture{
		name: name, identity: identity,
		evidence:   filepath.Join(f.root, name+"-evidence.json"),
		state:      filepath.Join(f.root, name+"-notebook-state.json"),
		definition: filepath.Join(f.root, name+"-definition.json"),
	}
	writeJSONFixture(t, o.evidence, evidence)
	writeJSONFixture(t, o.state, emptyNotebookState())
	writeJSONFixture(t, o.definition, observerDefinition{
		ContractVersion: observerRunVersion, Revision: "fixture-r1",
		Identity: identity, Questions: questions,
		Instructions: "Assess the retained evidence and keep uncertainty explicit.",
	})
	if f.pipeline.Config.Observers == nil {
		f.pipeline.Config.Observers = map[string]config.Observer{}
	}
	f.pipeline.Config.Observers[name] = config.Observer{
		Definition: o.definition, Collector: helperCommand(f.root, "collector", o.evidence),
		Notebook: helperCommand(f.root, "notebook", o.state), DataPolicy: "cloud",
		DenyRead: []string{filepath.Join(f.root, name+"-domain-store")}, MaxModelInputBytes: 64 << 10, TimeoutSeconds: 10,
	}
	return o
}

// seedQuestion puts a standing question into the stand-in notebook as an
// observing entry with no history, the way a person would have registered it.
func seedQuestion(t *testing.T, o *observerFixture, question *observercontract.Question) {
	t.Helper()
	var state observerNotebookState
	readJSONFixture(t, o.state, &state)
	entry := &observercontract.Entry{
		ID: question.ID, Identity: question.Identity, Question: question,
		State:        observercontract.StateObserving,
		Observations: []*observercontract.Assessment{}, InFlight: []string{},
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	state.Entries[question.ID] = data
	state.Revision++
	writeJSONFixture(t, o.state, state)
}

// installObserverModel replaces the fixture's stand-in claude with one that
// answers the auth probe, then writes the given observer.json manifest and
// optional report. It refuses to run if the full evidence is visible from its
// working directory while it executes.
func installObserverModel(t *testing.T, f *liveFixture, manifest any, report string) {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeReport := ""
	if report != "" {
		writeReport = fmt.Sprintf("printf '%%s\n' %q > out/report.md\n", report)
	}
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then exit 0; fi
cat >/dev/null
printf 'call\n' >> %q
calls=$(wc -l < %q)
if [ "$calls" -eq 1 ]; then
  printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.001}'
  exit 0
fi
if [ -e observer ]; then
  echo 'full observer evidence was visible during model execution' >&2
  exit 42
fi
mkdir -p out
printf '%%s\n' %q > out/observer.json
%sprintf '%%s\n' '{"type":"system","subtype":"init","session_id":"observer-session"}'
printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"session_id":"observer-session","num_turns":2,"total_cost_usd":0.02,"usage":{"input_tokens":17,"output_tokens":9}}'
`, f.calls, f.calls, string(data), writeReport)
	if err := os.WriteFile(f.pipeline.Config.Paths.ClaudeBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func readObserverResult(t *testing.T, runDir string) observerResultRecord {
	t.Helper()
	var result observerResultRecord
	readJSONFixture(t, filepath.Join(runDir, "observer-result.json"), &result)
	return result
}

func TestObserverPipelineRetainsAndAppliesDomainEvidence(t *testing.T) {
	f := newLiveFixture(t, "success")
	assessment := harnessAssessment(t, "assessment-unprompted.json")
	fixture := registerObserver(t, f, "fixture", assessment.Identity, []string{}, []*observercontract.Bundle{assessment.Evidence})
	statePath := fixture.state
	f.pipeline.Config.Observers["local-other"] = config.Observer{
		DataPolicy: "local-only",
		DenyRead:   []string{filepath.Join(f.root, "other-domain-store")},
	}
	f.pipeline.Config.Email = config.Email{}
	f.def.Manifest.Observer = &agentdef.Observer{Config: "fixture"}
	f.def.Manifest.Inputs = nil
	f.def.Manifest.Outputs = agentdef.Outputs{
		File: &agentdef.FileOutput{Dir: "observer-fixture", Max: 1},
	}

	action := &observercontract.Action{
		Envelope: observercontract.Envelope{ContractVersion: observercontract.ContractVersion},
		Identity: assessment.Identity, ID: assessment.Identity.Qualify("proposed-action"),
		QuestionID: assessment.EntryID(), Kind: observercontract.ActionTicketOpen,
		Body:     "Investigate the retained unexpected finding.",
		Evidence: []observercontract.Source{assessment.Evidence.Sources[0]},
	}
	report := "# Observer report\n\n" + strings.Repeat("The retained evidence supports a bounded, reviewable observation. ", 5)
	installObserverModel(t, f, map[string]any{
		"silent": false, "assessments": []any{assessment}, "actions": []any{action},
	}, report)

	run := f.claim(t, ledger.TriggerManual)
	result, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
	if err != nil {
		t.Fatal(err)
	}
	row, err := f.store.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != ledger.StatusCompleted || row.Silent || row.DeliveryStatus.String != ledger.DeliverySkipped {
		t.Fatalf("row = %+v", row)
	}
	for _, name := range []string{
		"run.json", "prompt.md", "observer-result.json", "applied.json",
		filepath.Join("observer", "definition.json"),
		filepath.Join("observer", "notebook.json"),
		filepath.Join("observer", "evidence.json"),
		filepath.Join("observer", "validated.json"),
		filepath.Join("inputs", "observer.json"),
	} {
		if _, err := os.Stat(filepath.Join(result.RunDir, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	var record runRecord
	readJSONFixture(t, filepath.Join(result.RunDir, "run.json"), &record)
	if record.Observer == nil || record.Observer.Config != "fixture" || record.Observer.NotebookRevision != 0 ||
		record.Observer.DefinitionRevision != "fixture-r1" || record.Observer.DefinitionSHA256 == "" ||
		record.Observer.EvidenceStatus != "observed" {
		t.Fatalf("run.json observer = %+v", record.Observer)
	}
	var rawEvidence struct {
		Evidence []json.RawMessage `json:"evidence"`
	}
	readJSONFixture(t, filepath.Join(result.RunDir, "observer", "evidence.json"), &rawEvidence)
	if len(rawEvidence.Evidence) != 1 {
		t.Fatalf("retained evidence = %+v, want the collector's bundle verbatim", rawEvidence)
	}
	var validated persistedObserverOutput
	readJSONFixture(t, filepath.Join(result.RunDir, "observer", "validated.json"), &validated)
	if len(validated.Assessments) != 1 || len(validated.Actions) != 1 || validated.Silent {
		t.Fatalf("validated.json = %+v", validated)
	}
	if _, err := os.Stat(filepath.Join(result.RunDir, "out", observerOutputFile)); err != nil {
		t.Fatalf("raw model output not retained: %v", err)
	}
	outcome := readObserverResult(t, result.RunDir)
	if outcome.Collection != "observed" || outcome.AssessmentStatus != "validated" || outcome.AssessmentCount != 1 ||
		outcome.ActionCount != 1 || outcome.NotebookStatus != "applied" || outcome.NotebookRevision != 1 {
		t.Fatalf("observer-result.json = %+v", outcome)
	}
	var notebook observerNotebookState
	readJSONFixture(t, statePath, &notebook)
	if notebook.Records != 1 || notebook.Revision != 1 {
		t.Fatalf("notebook = %+v", notebook)
	}
	var applied observerApplied
	readJSONFixture(t, filepath.Join(result.RunDir, "applied.json"), &applied)
	if len(applied.Observations) != 1 || len(applied.Actions) != 1 {
		t.Fatalf("applied = %+v", applied)
	}
	if applied.Observations[0].RequestID != run.ID+"-assessment-001" || applied.Observations[0].RevisionBefore != 0 ||
		applied.Observations[0].EntryID != assessment.EntryID() {
		t.Fatalf("observation receipt = %+v, want a request id derived from the run id", applied.Observations[0])
	}
	if _, err := os.Stat(filepath.Join(f.root, reportsDir, "observer-fixture", runWindowLabel(t, result.RunDir)+".md")); err != nil {
		t.Fatal(err)
	}
	settings, err := os.ReadFile(filepath.Join(result.RunDir, "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(settings), filepath.Join(result.RunDir, "inputs")+"/**") ||
		strings.Contains(string(settings), `"Read(`+result.RunDir+`/**)"`) {
		t.Fatalf("observer read permissions expose more than bounded inputs: %s", settings)
	}
	profile, err := os.ReadFile(filepath.Join(result.RunDir, "srt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(profile), observerStagingDir(result.RunDir)) ||
		!strings.Contains(string(profile), filepath.Join(f.root, "other-domain-store")) {
		t.Fatalf("sandbox profile does not deny staged full evidence: %s", profile)
	}
}

// observerAgent points the fixture's agent at one registration as a file-only
// observer with no mail configured.
func observerAgent(f *liveFixture, name string) {
	f.pipeline.Config.Email = config.Email{}
	f.def.Manifest.Observer = &agentdef.Observer{Config: name}
	f.def.Manifest.Inputs = nil
	f.def.Manifest.Outputs = agentdef.Outputs{File: &agentdef.FileOutput{Dir: name, Max: 1}}
}

func TestObserverRunRecordsEachOutcomeDistinctly(t *testing.T) {
	assessment := harnessAssessment(t, "assessment-unprompted.json")
	silent := map[string]any{"silent": true, "assessments": []any{}, "actions": []any{}}
	for _, test := range []struct {
		name       string
		collector  string
		notebook   string
		model      map[string]any
		status     string
		class      string
		collection string
		assessed   string
		notebooked string
		detail     string
	}{
		{
			name: "empty but valid", collector: "collector-empty", model: silent,
			status: ledger.StatusCompleted, collection: "empty", assessed: "none", notebooked: "applied",
		},
		{
			name: "unavailable source", collector: "collector-unavailable", model: silent,
			status: ledger.StatusCompleted, collection: "unavailable", assessed: "none", notebooked: "applied",
		},
		{
			name: "invalid payload", collector: "collector-invalid", model: silent,
			status: ledger.StatusFailed, class: ledger.ClassProvider, collection: "invalid-payload",
			assessed: "not-run", notebooked: "not-run", detail: "collector payload",
		},
		{
			name: "failed collection", collector: "collector-fail", model: silent,
			status: ledger.StatusFailed, class: ledger.ClassProvider, collection: "collection-failed",
			assessed: "not-run", notebooked: "not-run", detail: "domain store is locked",
		},
		{
			name: "failed notebook snapshot", collector: "collector", notebook: "collector-fail", model: silent,
			status: ledger.StatusFailed, class: ledger.ClassProvider, collection: "notebook-failed",
			assessed: "not-run", notebooked: "not-run", detail: "notebook snapshot",
		},
		{
			name: "deliberate silence", collector: "collector", model: silent,
			status: ledger.StatusCompleted, collection: "observed", assessed: "none", notebooked: "applied",
		},
		{
			name: "invalid model output", collector: "collector",
			model:  map[string]any{"silent": true, "assessments": []any{map[string]any{"contract_version": "observer/v1"}}, "actions": []any{}},
			status: ledger.StatusFailed, class: ledger.ClassOutput, collection: "observed",
			assessed: "invalid", notebooked: "not-run", detail: "assessments[0]",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newLiveFixture(t, "success")
			fixture := registerObserver(t, f, "fixture", assessment.Identity, []string{}, []*observercontract.Bundle{assessment.Evidence})
			registration := f.pipeline.Config.Observers["fixture"]
			registration.Collector = helperCommand(f.root, test.collector, fixture.evidence)
			if test.notebook != "" {
				registration.Notebook = helperCommand(f.root, test.notebook)
			}
			f.pipeline.Config.Observers["fixture"] = registration
			observerAgent(f, "fixture")
			installObserverModel(t, f, test.model, "")

			run := f.claim(t, ledger.TriggerManual)
			_, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
			row, getErr := f.store.Get(run.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if row.Status != test.status || (err == nil) != (test.status == ledger.StatusCompleted) {
				t.Fatalf("row status = %s, err = %v; want %s", row.Status, err, test.status)
			}
			if test.class != "" && row.FailureClass.String != test.class {
				t.Fatalf("failure class = %q, want %q", row.FailureClass.String, test.class)
			}
			outcome := readObserverResult(t, row.RunDir.String)
			if outcome.Collection != test.collection || outcome.AssessmentStatus != test.assessed || outcome.NotebookStatus != test.notebooked {
				t.Fatalf("observer-result.json = %+v, want collection %s, assessments %s, notebook %s",
					outcome, test.collection, test.assessed, test.notebooked)
			}
			if test.detail != "" && !strings.Contains(outcome.Detail, test.detail) {
				t.Fatalf("detail = %q, want it to name %q", outcome.Detail, test.detail)
			}
			if test.status == ledger.StatusCompleted && (outcome.Silent == nil || !*outcome.Silent || !row.Silent) {
				t.Fatalf("silence not recorded: result %+v, row silent %t", outcome, row.Silent)
			}
			// No run turned what it did not read into an observation.
			var notebook observerNotebookState
			readJSONFixture(t, fixture.state, &notebook)
			if notebook.Records != 0 {
				t.Fatalf("notebook = %+v, want no observation recorded", notebook)
			}
			// The definition and whatever the collector returned stay with
			// the run whether or not the model ever ran.
			if _, err := os.Stat(filepath.Join(row.RunDir.String, "observer", "definition.json")); err != nil {
				t.Fatal(err)
			}
			if test.collection != "notebook-failed" && test.collection != "collection-failed" {
				if _, err := os.Stat(filepath.Join(row.RunDir.String, "observer", "evidence.json")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestObserverRunCarriesStandingQuestionsAndUnpromptedFindings(t *testing.T) {
	f := newLiveFixture(t, "success")
	questionData, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "question.json"))
	if err != nil {
		t.Fatal(err)
	}
	question, err := observercontract.ValidateQuestion(questionData)
	if err != nil {
		t.Fatal(err)
	}
	// The standing question's fixture assessment declares an absence, which
	// needs an experiment nobody registered; the same evidence read as an
	// observed effect answers the question on its own terms.
	answer := harnessAssessment(t, "assessment.json")
	answer.Outcome = observercontract.OutcomeEffectObserved
	answer.Interpretation = "Every one of the three nights repeated a headline, which is the effect the question asks about."
	unprompted := harnessAssessment(t, "assessment-unprompted.json")
	fixture := registerObserver(t, f, "fixture", question.Identity, []string{question.ID},
		[]*observercontract.Bundle{answer.Evidence, unprompted.Evidence})
	seedQuestion(t, fixture, question)
	observerAgent(f, "fixture")
	installObserverModel(t, f, map[string]any{
		"silent": true, "assessments": []any{answer, unprompted}, "actions": []any{},
	}, "")

	run := f.claim(t, ledger.TriggerManual)
	result, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var presentation observerPresentation
	readJSONFixture(t, filepath.Join(result.RunDir, "inputs", "observer.json"), &presentation)
	if presentation.NotebookRevision != 1 || len(presentation.Entries) != 1 || presentation.Entries[0].ID != question.ID ||
		presentation.Entries[0].Question == nil || len(presentation.Evidence) != 2 ||
		presentation.Exploration != "A bounded exploratory note." {
		t.Fatalf("model input = %+v, want the standing question, both bundles and the exploratory prose together", presentation)
	}
	var snapshot observerSnapshot
	readJSONFixture(t, filepath.Join(result.RunDir, "observer", "notebook.json"), &snapshot)
	if snapshot.Revision != 1 || len(snapshot.Entries) != 1 || snapshot.Entries[0].ID != question.ID {
		t.Fatalf("retained notebook snapshot = %+v", snapshot)
	}

	var notebook observerNotebookState
	readJSONFixture(t, fixture.state, &notebook)
	if notebook.Records != 2 || notebook.Revision != 3 {
		t.Fatalf("notebook = %+v, want both observations appended", notebook)
	}
	var answered observercontract.Entry
	if err := json.Unmarshal(notebook.Entries[question.ID], &answered); err != nil {
		t.Fatal(err)
	}
	if answered.State != observercontract.StateAnswered || len(answered.Observations) != 1 {
		t.Fatalf("question entry = %+v, want it answered by one observation", answered)
	}
	var opened observercontract.Entry
	if err := json.Unmarshal(notebook.Entries[unprompted.EntryID()], &opened); err != nil {
		t.Fatalf("finding entry %s: %v", unprompted.EntryID(), err)
	}
	if opened.Finding == nil || opened.Question != nil || opened.State != observercontract.StateObserving || len(opened.Observations) != 1 {
		t.Fatalf("finding entry = %+v, want an entry opened by the finding itself", opened)
	}
	var applied observerApplied
	readJSONFixture(t, filepath.Join(result.RunDir, "applied.json"), &applied)
	if len(applied.Observations) != 2 || applied.NotebookRevision != 3 ||
		applied.Observations[0].RequestID != run.ID+"-assessment-001" || applied.Observations[0].RevisionBefore != 1 ||
		applied.Observations[1].RequestID != run.ID+"-assessment-002" || applied.Observations[1].RevisionBefore != 2 {
		t.Fatalf("applied = %+v, want two receipts under run-derived request ids and consecutive revisions", applied)
	}
}

func TestObserverRunReplaysAnUncertainRecordWithoutDuplicating(t *testing.T) {
	f := newLiveFixture(t, "success")
	assessment := harnessAssessment(t, "assessment-unprompted.json")
	fixture := registerObserver(t, f, "fixture", assessment.Identity, []string{}, []*observercontract.Bundle{assessment.Evidence})
	state := emptyNotebookState()
	state.CrashOnce = true
	writeJSONFixture(t, fixture.state, state)
	observerAgent(f, "fixture")
	installObserverModel(t, f, map[string]any{
		"silent": true, "assessments": []any{assessment}, "actions": []any{},
	}, "")

	run := f.claim(t, ledger.TriggerManual)
	result, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var after observerNotebookState
	readJSONFixture(t, fixture.state, &after)
	if after.Records != 1 || after.Revision != 1 || after.CrashOnce {
		t.Fatalf("notebook = %+v, want the one observation the crashed adapter had already committed", after)
	}
	var opened observercontract.Entry
	if err := json.Unmarshal(after.Entries[assessment.EntryID()], &opened); err != nil {
		t.Fatal(err)
	}
	if len(opened.Observations) != 1 {
		t.Fatalf("entry = %+v, want exactly one observation after the replay", opened)
	}
	var applied observerApplied
	readJSONFixture(t, filepath.Join(result.RunDir, "applied.json"), &applied)
	if len(applied.Observations) != 1 || applied.NotebookRevision != 1 {
		t.Fatalf("applied = %+v", applied)
	}
	if outcome := readObserverResult(t, result.RunDir); outcome.NotebookStatus != "applied" {
		t.Fatalf("observer-result.json = %+v", outcome)
	}
}

func TestObserverRunSurfacesAConcurrentWorkerEditInsteadOfOverwriting(t *testing.T) {
	f := newLiveFixture(t, "success")
	assessment := harnessAssessment(t, "assessment-unprompted.json")
	fixture := registerObserver(t, f, "fixture", assessment.Identity, []string{}, []*observercontract.Bundle{assessment.Evidence})
	registration := f.pipeline.Config.Observers["fixture"]
	registration.Collector = helperCommand(f.root, "collector-bump", fixture.evidence, fixture.state)
	f.pipeline.Config.Observers["fixture"] = registration
	observerAgent(f, "fixture")
	installObserverModel(t, f, map[string]any{
		"silent": true, "assessments": []any{assessment}, "actions": []any{},
	}, "")

	run := f.claim(t, ledger.TriggerManual)
	_, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
	if !errors.Is(err, observercontract.ErrStaleRevision) {
		t.Fatalf("err = %v, want the stale revision surfaced", err)
	}
	row, getErr := f.store.Get(run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if row.Status != ledger.StatusFailed || row.FailureClass.String != ledger.ClassOutput {
		t.Fatalf("row = %+v", row)
	}
	var state observerNotebookState
	readJSONFixture(t, fixture.state, &state)
	if state.Records != 0 || state.Revision != 1 {
		t.Fatalf("notebook = %+v, want the worker's revision untouched and nothing recorded over it", state)
	}
	outcome := readObserverResult(t, row.RunDir.String)
	if outcome.NotebookStatus != "conflict-or-failure" || outcome.NotebookRevision != 0 ||
		outcome.AssessmentStatus != "validated" || !strings.Contains(outcome.Detail, run.ID+"-assessment-001") {
		t.Fatalf("observer-result.json = %+v, want the conflict named with its request id", outcome)
	}
	var applied observerApplied
	readJSONFixture(t, filepath.Join(row.RunDir.String, "applied.json"), &applied)
	if len(applied.Observations) != 0 {
		t.Fatalf("applied = %+v, want no receipt for a refused record", applied)
	}
	// The snapshot the decision was taken against, and the validated output,
	// stay with the run for a re-evaluation against the moved notebook.
	for _, name := range []string{
		filepath.Join("observer", "notebook.json"), filepath.Join("observer", "evidence.json"),
		filepath.Join("observer", "validated.json"),
	} {
		if _, err := os.Stat(filepath.Join(row.RunDir.String, name)); err != nil {
			t.Error(err)
		}
	}
}

func TestObserverDefinitionsKeepSeparateScopes(t *testing.T) {
	f := newLiveFixture(t, "success")
	template := harnessAssessment(t, "assessment-unprompted.json")
	// Two observers in the harness domain, each with its own registration,
	// notebook, denied store and report directory.
	scoped := func(observer string) *observercontract.Assessment {
		data, _ := json.Marshal(template)
		var out observercontract.Assessment
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatal(err)
		}
		out.Identity.Observer = observer
		out.Evidence.Identity.Observer = observer
		out.Finding.ID = out.Identity.Qualify("unprompted-pattern")
		return &out
	}
	alpha, beta := scoped("alpha"), scoped("beta")
	alphaFixture := registerObserver(t, f, "alpha", alpha.Identity, []string{}, []*observercontract.Bundle{alpha.Evidence})
	betaFixture := registerObserver(t, f, "beta", beta.Identity, []string{}, []*observercontract.Bundle{beta.Evidence})
	report := "# Scoped report\n\n" + strings.Repeat("Each observer archives under its own directory. ", 5)

	runObserver := func(name string, assessment *observercontract.Assessment) (*ledger.Run, error) {
		f.def.Name = name
		f.def.Manifest.Name = name
		observerAgent(f, name)
		installObserverModel(t, f, map[string]any{
			"silent": false, "assessments": []any{assessment}, "actions": []any{},
		}, report)
		run := f.claim(t, ledger.TriggerManual)
		_, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
		return run, err
	}
	if _, err := runObserver("alpha", alpha); err != nil {
		t.Fatal(err)
	}
	if _, err := runObserver("beta", beta); err != nil {
		t.Fatal(err)
	}
	for _, o := range []struct {
		fixture *observerFixture
		entry   string
		other   string
	}{
		{fixture: alphaFixture, entry: alpha.EntryID(), other: beta.EntryID()},
		{fixture: betaFixture, entry: beta.EntryID(), other: alpha.EntryID()},
	} {
		var state observerNotebookState
		readJSONFixture(t, o.fixture.state, &state)
		if state.Records != 1 || len(state.Entries) != 1 {
			t.Fatalf("%s notebook = %+v, want exactly its own observation", o.fixture.name, state)
		}
		if _, ok := state.Entries[o.entry]; !ok {
			t.Fatalf("%s notebook lacks %s", o.fixture.name, o.entry)
		}
		if _, ok := state.Entries[o.other]; ok {
			t.Fatalf("%s notebook holds the other observer's entry %s", o.fixture.name, o.other)
		}
		reports, err := filepath.Glob(filepath.Join(f.root, reportsDir, o.fixture.name, "*.md"))
		if err != nil || len(reports) != 1 {
			t.Fatalf("%s reports = %v, %v", o.fixture.name, reports, err)
		}
		runs, err := filepath.Glob(filepath.Join(f.root, "runs", o.fixture.name, "*", "observer", "validated.json"))
		if err != nil || len(runs) != 1 {
			t.Fatalf("%s retained runs = %v, %v", o.fixture.name, runs, err)
		}
	}
	// beta cannot write about alpha's entry: an assessment carrying the other
	// identity is refused before any notebook is touched.
	f.slot = f.slot.Add(-24 * time.Hour)
	run, err := runObserver("beta", alpha)
	if err == nil || !strings.Contains(err.Error(), "does not match configured observer") {
		t.Fatalf("cross-scope err = %v", err)
	}
	row, err := f.store.Get(run.ID)
	if err != nil || row.FailureClass.String != ledger.ClassOutput {
		t.Fatalf("cross-scope row = %+v, %v", row, err)
	}
	var state observerNotebookState
	readJSONFixture(t, alphaFixture.state, &state)
	if state.Records != 1 {
		t.Fatalf("alpha notebook = %+v after beta's refused output", state)
	}
	readJSONFixture(t, betaFixture.state, &state)
	if state.Records != 1 {
		t.Fatalf("beta notebook = %+v after its refused output", state)
	}
}

func TestObserverCollectionOutcomesRemainDistinct(t *testing.T) {
	identity := observercontract.Identity{Domain: "harness", Observer: "shuttle"}
	assessmentData, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(assessmentData)
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := json.Marshal(assessment.Evidence)
	for _, test := range []struct {
		name string
		data string
		want string
		ok   bool
	}{
		{name: "empty", data: `{"status":"empty","evidence":[],"exploration":""}`, want: "empty", ok: true},
		{name: "observed", data: `{"status":"observed","evidence":[` + string(evidence) + `],"exploration":""}`, want: "observed", ok: true},
		{name: "unavailable", data: `{"status":"unavailable","evidence":[` + string(evidence) + `],"exploration":""}`, want: "unavailable", ok: true},
		{name: "unavailable without evidence", data: `{"status":"unavailable","evidence":[],"exploration":""}`},
		{name: "evidence omitted", data: `{"status":"empty","exploration":""}`},
		{name: "evidence null", data: `{"status":"empty","evidence":null,"exploration":""}`},
		{name: "exploration omitted", data: `{"status":"empty","evidence":[]}`},
		{name: "exploration null", data: `{"status":"empty","evidence":[],"exploration":null}`},
		{name: "invalid payload", data: `{"status":"empty","evidence":[],"unknown":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateCollection([]byte(test.data), identity)
			if test.ok {
				if err != nil || got.Status != test.want {
					t.Fatalf("collection = %+v, %v", got, err)
				}
			} else if err == nil {
				t.Fatal("invalid collection succeeded")
			}
		})
	}
}

func TestObserverEmptyArtifactsRetainJSONArrays(t *testing.T) {
	identity := observercontract.Identity{Domain: "harness", Observer: "empty"}
	collection, err := validateCollection([]byte(`{"status":"empty","evidence":[],"exploration":""}`), identity)
	if err != nil {
		t.Fatal(err)
	}
	presentation, err := boundPresentation(&observerDefinition{
		ContractVersion: observerRunVersion, Revision: "r1", Identity: identity,
		Instructions: "Observe.",
	}, observerSnapshot{Entries: []*observercontract.Entry{}}, collection, 4096)
	if err != nil {
		t.Fatal(err)
	}
	presentationJSON, _ := json.Marshal(presentation)
	if !strings.Contains(string(presentationJSON), `"entries":[]`) ||
		!strings.Contains(string(presentationJSON), `"evidence":[]`) {
		t.Fatalf("presentation arrays = %s", presentationJSON)
	}

	runDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(runDir, "observer"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "out", observerOutputFile), []byte(`{"silent":true,"assessments":[],"actions":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateObserverOutput(filepath.Join(runDir, "out"), &agentdef.Definition{}, &observerRunState{
		Definition: &observerDefinition{Identity: identity},
		Snapshot:   observerSnapshot{Entries: []*observercontract.Entry{}},
		Collection: collection,
	}); err != nil {
		t.Fatal(err)
	}
	validated, err := os.ReadFile(filepath.Join(runDir, "observer", "validated.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(validated), `"assessments": []`) ||
		!strings.Contains(string(validated), `"actions": []`) {
		t.Fatalf("validated arrays = %s", validated)
	}
}

func TestObserverPresentationIsBoundedWithoutMutatingRetainedEvidence(t *testing.T) {
	assessmentData, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(assessmentData)
	if err != nil {
		t.Fatal(err)
	}
	definition := &observerDefinition{
		ContractVersion: observerRunVersion, Revision: "r1", Identity: assessment.Identity,
		Instructions: "Observe.",
	}
	collection := &validatedCollection{
		Status: "observed", Evidence: []*observercontract.Bundle{assessment.Evidence},
		Exploration: strings.Repeat("exploratory evidence ", 2000),
	}
	presentation, err := boundPresentation(definition, observerSnapshot{}, collection, 4096)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "observer.json")
	if err := writeJSON(path, presentation, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 4096 || !presentation.TruncatedExploration {
		t.Fatalf("presentation = %d bytes, truncated=%t", info.Size(), presentation.TruncatedExploration)
	}
	if len(collection.Evidence) != 1 || len(collection.Exploration) < 10000 {
		t.Fatal("bounding changed the retained collection")
	}
}

func TestObserverBoundingRemovesGloballyOldestHistory(t *testing.T) {
	entries := []*observercontract.Entry{
		{Observations: []*observercontract.Assessment{{At: "2026-09-02T00:00:00Z"}}},
		{Observations: []*observercontract.Assessment{
			{At: "2026-09-01T00:00:00Z"},
			{At: "2026-09-03T00:00:00Z"},
		}},
	}
	if !removeOldestObservation(entries) {
		t.Fatal("no observation removed")
	}
	if len(entries[0].Observations) != 1 || len(entries[1].Observations) != 1 ||
		entries[1].Observations[0].At != "2026-09-03T00:00:00Z" {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestObserverEmailOutputRequiresAndAcceptsSubject(t *testing.T) {
	assessmentData, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(assessmentData)
	if err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(runDir, "observer"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeJSONFixture(t, filepath.Join(runDir, "out", observerOutputFile), map[string]any{
		"silent": false, "subject": "Observer finding",
		"assessments": []any{assessment}, "actions": []any{},
	})
	report := "# Observer finding\n\n" + strings.Repeat("Evidence-backed report content. ", 10)
	if err := os.WriteFile(filepath.Join(runDir, "out", "report.md"), []byte(report), 0o600); err != nil {
		t.Fatal(err)
	}
	def := &agentdef.Definition{Manifest: agentdef.Manifest{
		Outputs: agentdef.Outputs{Email: &agentdef.EmailOutput{Max: 1}},
	}}
	state := &observerRunState{
		Definition: &observerDefinition{Identity: assessment.Identity},
		Collection: &validatedCollection{Evidence: []*observercontract.Bundle{assessment.Evidence}},
	}
	out, err := validateObserverOutput(filepath.Join(runDir, "out"), def, state)
	if err != nil {
		t.Fatal(err)
	}
	if out.Subject != "Observer finding" {
		t.Fatalf("subject = %q", out.Subject)
	}
}

func TestObserverOutputCannotUpdateAnUnselectedQuestion(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(data)
	if err != nil {
		t.Fatal(err)
	}
	missing := *assessment
	missing.QuestionID = assessment.Identity.Qualify("not-in-notebook")
	for _, test := range []struct {
		name       string
		assessment *observercontract.Assessment
	}{
		{name: "existing but unselected", assessment: assessment},
		{name: "nonexistent", assessment: &missing},
	} {
		t.Run(test.name, func(t *testing.T) {
			runDir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(runDir, "observer"), 0o755); err != nil {
				t.Fatal(err)
			}
			writeJSONFixture(t, filepath.Join(runDir, "out", observerOutputFile), map[string]any{
				"silent": true, "assessments": []any{test.assessment}, "actions": []any{},
			})
			state := &observerRunState{
				Definition: &observerDefinition{Identity: assessment.Identity},
				Snapshot:   observerSnapshot{Entries: []*observercontract.Entry{}},
				Collection: &validatedCollection{
					Evidence: []*observercontract.Bundle{assessment.Evidence},
				},
			}
			_, err := validateObserverOutput(filepath.Join(runDir, "out"), &agentdef.Definition{}, state)
			if err == nil || !strings.Contains(err.Error(), "selected notebook snapshot") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestObserverOutputRejectsFindingIDCollisionsBeforeApply(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		assessments []any
		entries     []*observercontract.Entry
	}{
		{
			name: "existing entry", assessments: []any{assessment},
			entries: []*observercontract.Entry{{ID: assessment.EntryID()}},
		},
		{
			name: "earlier finding in output", assessments: []any{assessment, assessment},
			entries: []*observercontract.Entry{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runDir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(runDir, "observer"), 0o755); err != nil {
				t.Fatal(err)
			}
			writeJSONFixture(t, filepath.Join(runDir, "out", observerOutputFile), map[string]any{
				"silent": true, "assessments": test.assessments, "actions": []any{},
			})
			state := &observerRunState{
				Definition: &observerDefinition{Identity: assessment.Identity},
				Snapshot:   observerSnapshot{Entries: test.entries},
				Collection: &validatedCollection{
					Evidence: []*observercontract.Bundle{assessment.Evidence},
				},
			}
			_, err := validateObserverOutput(filepath.Join(runDir, "out"), &agentdef.Definition{}, state)
			if err == nil || !strings.Contains(err.Error(), "collides") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestObserverOutputRejectsDuplicateActionIDs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(data)
	if err != nil {
		t.Fatal(err)
	}
	action := &observercontract.Action{
		Envelope: observercontract.Envelope{ContractVersion: observercontract.ContractVersion},
		Identity: assessment.Identity, ID: assessment.Identity.Qualify("duplicate-action"),
		QuestionID: assessment.EntryID(), Kind: observercontract.ActionTicketOpen,
		Body: "Investigate once.", Evidence: []observercontract.Source{assessment.Evidence.Sources[0]},
	}
	runDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(runDir, "observer"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeJSONFixture(t, filepath.Join(runDir, "out", observerOutputFile), map[string]any{
		"silent": true, "assessments": []any{assessment}, "actions": []any{action, action},
	})
	state := &observerRunState{
		Definition: &observerDefinition{Identity: assessment.Identity},
		Collection: &validatedCollection{Evidence: []*observercontract.Bundle{assessment.Evidence}},
	}
	_, err = validateObserverOutput(filepath.Join(runDir, "out"), &agentdef.Definition{}, state)
	if err == nil || !strings.Contains(err.Error(), "appears twice") {
		t.Fatalf("error = %v", err)
	}
}

func TestObserverOutputRequiresAssessmentAndActionArrays(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "assessments omitted", body: `{"silent":true,"actions":[]}`, want: "assessments"},
		{name: "assessments null", body: `{"silent":true,"assessments":null,"actions":[]}`, want: "assessments"},
		{name: "actions omitted", body: `{"silent":true,"assessments":[]}`, want: "actions"},
		{name: "actions null", body: `{"silent":true,"assessments":[],"actions":null}`, want: "actions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runDir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(runDir, "out", observerOutputFile), []byte(test.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := validateObserverOutput(filepath.Join(runDir, "out"), &agentdef.Definition{}, &observerRunState{
				Definition: &observerDefinition{}, Collection: &validatedCollection{},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestObserverOutputRejectsReportWhenSilent(t *testing.T) {
	runDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "out", observerOutputFile), []byte(`{"silent":true,"assessments":[],"actions":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "out", output.ReportFile), []byte("quiet"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := validateObserverOutput(filepath.Join(runDir, "out"), &agentdef.Definition{}, &observerRunState{
		Definition: &observerDefinition{}, Collection: &validatedCollection{},
	})
	if err == nil || !strings.Contains(err.Error(), "present while silent is true") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadObserverReportRevalidatesRetainedFile(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.md")
	if err := os.WriteFile(valid, []byte(strings.Repeat("valid report ", 30)), 0o600); err != nil {
		t.Fatal(err)
	}
	short := filepath.Join(dir, "short.md")
	if err := os.WriteFile(short, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalid := filepath.Join(dir, "invalid.md")
	if err := os.WriteFile(invalid, []byte{0xff, 0xfe}, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.md")
	if err := os.Symlink(valid, link); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		path string
		ok   bool
	}{
		{name: "valid", path: valid, ok: true},
		{name: "short", path: short},
		{name: "invalid utf8", path: invalid},
		{name: "symlink", path: link},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadObserverReport(test.path, false)
			if test.ok && err != nil {
				t.Fatal(err)
			}
			if !test.ok && err == nil {
				t.Fatal("invalid retained report succeeded")
			}
		})
	}
}

func TestObserverDryRunRejectsRelativeCollectorBeforeExecution(t *testing.T) {
	root := t.TempDir()
	definitionPath := filepath.Join(root, "definition.json")
	writeJSONFixture(t, definitionPath, observerDefinition{
		ContractVersion: observerRunVersion, Revision: "r1",
		Identity:  observercontract.Identity{Domain: "harness", Observer: "dry"},
		Questions: []string{}, Instructions: "Observe.",
	})
	marker := filepath.Join(root, "executed")
	relative := filepath.Join(root, "collector")
	if err := os.WriteFile(relative, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Observers: map[string]config.Observer{
		"dry": {
			Definition: definitionPath,
			Collector:  config.Command{Argv: []string{"collector"}, WorkingDir: root},
			Notebook: config.Command{
				Argv:       []string{os.Args[0], "-test.run=TestObserverRunnerHelper", "--", "notebook", filepath.Join(root, "state.json")},
				WorkingDir: root,
			},
			DataPolicy: "cloud", DenyRead: []string{root},
			MaxModelInputBytes: 4096, TimeoutSeconds: 10,
		},
	}}
	def := &agentdef.Definition{Name: "dry", Prompt: "Observe.", Manifest: agentdef.Manifest{
		Name: "dry", Observer: &agentdef.Observer{Config: "dry"},
		Runtime:  agentdef.Runtime{Kind: "claude"},
		Triggers: []agentdef.Trigger{{Kind: "cron", Expression: "0 4 * * *", Timezone: "UTC"}},
	}}
	t.Setenv("PATH", root+":"+os.Getenv("PATH"))
	_, err := DryRun(context.Background(), def, time.Now().UTC(), cfg, root, nil)
	if err == nil || !strings.Contains(err.Error(), "must be an absolute binary path") {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("relative collector executed: %v", err)
	}
}

func TestObserverDryRunSameSlotRetainsDistinctRuns(t *testing.T) {
	root := t.TempDir()
	data, err := os.ReadFile(filepath.Join("..", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment(data)
	if err != nil {
		t.Fatal(err)
	}
	evidencePath := filepath.Join(root, "evidence.json")
	writeJSONFixture(t, evidencePath, []*observercontract.Bundle{assessment.Evidence})
	statePath := filepath.Join(root, "state.json")
	writeJSONFixture(t, statePath, emptyNotebookState())
	definitionPath := filepath.Join(root, "definition.json")
	writeJSONFixture(t, definitionPath, observerDefinition{
		ContractVersion: observerRunVersion, Revision: "r1",
		Identity: assessment.Identity, Questions: []string{}, Instructions: "Observe.",
	})
	command := func(mode, path string) config.Command {
		return config.Command{
			Argv:       []string{os.Args[0], "-test.run=TestObserverRunnerHelper", "--", mode, path},
			WorkingDir: root,
		}
	}
	cfg := &config.Config{Observers: map[string]config.Observer{
		"dry": {
			Definition: definitionPath, Collector: command("collector", evidencePath),
			Notebook: command("notebook", statePath), DataPolicy: "cloud",
			DenyRead: []string{root}, MaxModelInputBytes: 4096, TimeoutSeconds: 10,
		},
	}}
	def := &agentdef.Definition{Name: "dry", Prompt: "Observe.", Manifest: agentdef.Manifest{
		Name: "dry", Observer: &agentdef.Observer{Config: "dry"},
		Runtime:  agentdef.Runtime{Kind: "claude"},
		Triggers: []agentdef.Trigger{{Kind: "cron", Expression: "0 4 * * *", Timezone: "UTC"}},
	}}
	slot := time.Date(2026, 9, 18, 4, 0, 0, 0, time.UTC)
	first, err := DryRun(context.Background(), def, slot, cfg, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DryRun(context.Background(), def, slot, cfg, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Dir == second.Dir {
		t.Fatalf("same-slot dry runs reused %s", first.Dir)
	}
	for _, dir := range []string{first.Dir, second.Dir} {
		if _, err := os.Stat(filepath.Join(dir, "observer", "evidence.json")); err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
	}
}

func TestObserverCollectorBoundsInheritedPipeWait(t *testing.T) {
	command := config.Command{
		Argv:       []string{os.Args[0], "-test.run=TestObserverRunnerHelper", "--", "held-pipe", "unused"},
		WorkingDir: t.TempDir(),
	}
	start := time.Now()
	if _, err := runDomainCommand(context.Background(), command, []string{}, 100*time.Millisecond, map[string]any{"operation": "collect"}); err == nil {
		t.Fatal("held pipe returned no error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("held pipe took %s, want bounded by WaitDelay", elapsed)
	}
}

func TestObserverRetryDeliveryHonorsEligibilityAndClearsFailures(t *testing.T) {
	for _, test := range []struct {
		name           string
		silent         bool
		email          bool
		retryEmail     bool
		report         bool
		wantSend       bool
		priorError     string
		removeSelector bool
	}{
		{name: "silent", silent: true, email: true},
		{name: "historical file-only", report: true, retryEmail: true},
		{name: "no-deliver retry", email: true, report: true, wantSend: true},
		{name: "failed retry", email: true, report: true, wantSend: true, priorError: "temporary failure"},
		{name: "selector removed", email: true, report: true, wantSend: true, removeSelector: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newLiveFixture(t, "success")
			f.def.Manifest.Observer = &agentdef.Observer{Config: "fixture"}
			f.def.Manifest.Outputs.Email = nil
			if test.email {
				f.def.Manifest.Outputs.Email = &agentdef.EmailOutput{Max: 1}
			}
			run := f.claim(t, ledger.TriggerManual)
			runDir := filepath.Join(f.root, "runs", f.def.Name, run.ID)
			if err := os.MkdirAll(filepath.Join(runDir, "observer"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(runDir, "out"), 0o755); err != nil {
				t.Fatal(err)
			}
			subject := "Observer retry"
			if test.retryEmail {
				subject = ""
			}
			persisted := persistedObserverOutput{
				Silent: test.silent, Subject: subject,
				Assessments: []*observercontract.Assessment{}, Actions: []*observercontract.Action{},
			}
			if err := writeJSON(filepath.Join(runDir, "observer", "validated.json"), persisted, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := writeJSON(filepath.Join(runDir, "run.json"), runRecord{
				RunID: run.ID, Agent: f.def.Name, Outputs: f.def.Manifest.Outputs,
				Observer: &observerRecord{Config: "fixture"},
			}, 0o600); err != nil {
				t.Fatal(err)
			}
			if test.report {
				report := "# Observer retry\n\n" + strings.Repeat("Retained report content. ", 10)
				if err := os.WriteFile(filepath.Join(runDir, "out", "report.md"), []byte(report), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			status := ledger.DeliverySkipped
			if test.priorError != "" {
				status = ledger.DeliveryFailed
			}
			applied := &observerApplied{
				DeliveryStatus: status, DeliveryError: test.priorError,
				Report: &output.Applied{
					DeliveryStatus: status, DeliveryError: test.priorError,
				},
			}
			if err := f.store.SetRunDir(run.ID, runDir); err != nil {
				t.Fatal(err)
			}
			if err := f.store.MarkRunning(run.ID, os.Getpid(), runDir); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Complete(run.ID, ledger.Result{Silent: test.silent}); err != nil {
				t.Fatal(err)
			}
			if err := writeObserverApplied(f.pipeline, run.ID, runDir, applied); err != nil {
				t.Fatal(err)
			}
			if err := f.store.SetDelivery(run.ID, status, test.priorError); err != nil {
				t.Fatal(err)
			}
			sender := &recordingSender{}
			f.pipeline.Sender = sender
			if test.retryEmail {
				f.def.Manifest.Outputs.Email = &agentdef.EmailOutput{Max: 1}
			}
			if test.removeSelector {
				f.def.Manifest.Observer = nil
			}
			row, _ := f.store.Get(run.ID)
			if err := f.pipeline.RetryDelivery(context.Background(), f.def, row); err != nil {
				t.Fatal(err)
			}
			if got := len(sender.messages); got != boolInt(test.wantSend) {
				t.Fatalf("sent %d messages, want %t", got, test.wantSend)
			}
			if test.priorError != "" {
				var after observerApplied
				readJSONFixture(t, filepath.Join(runDir, "applied.json"), &after)
				if after.DeliveryError != "" || after.Report == nil || after.Report.DeliveryError != "" {
					t.Fatalf("stale delivery error retained: %+v", after)
				}
			}
		})
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestObserverRunnerHelper(t *testing.T) {
	args := helperArgsAfterDash()
	if len(args) == 0 {
		return
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(81)
	}
	mode, args := args[0], args[1:]
	switch {
	case mode == "collector" && len(args) == 1:
		evidence, err := os.ReadFile(args[0])
		if err != nil {
			os.Exit(82)
		}
		fmt.Printf(`{"status":"observed","evidence":%s,"exploration":"A bounded exploratory note."}`+"\n", evidence)
	case mode == "collector-unavailable" && len(args) == 1:
		evidence, err := os.ReadFile(args[0])
		if err != nil {
			os.Exit(82)
		}
		fmt.Printf(`{"status":"unavailable","evidence":%s,"exploration":"One source could not be read."}`+"\n", evidence)
	case mode == "collector-empty":
		fmt.Print(`{"status":"empty","evidence":[],"exploration":"Nothing moved in the window."}` + "\n")
	case mode == "collector-fail":
		fmt.Fprint(os.Stderr, "domain store is locked\n")
		os.Exit(1)
	case mode == "collector-invalid":
		fmt.Print(`{"status":"observed","evidence":[{"not":"a bundle"}],"exploration":""}` + "\n")
	case mode == "collector-bump" && len(args) == 2:
		// A worker edits the notebook after the runner took its snapshot and
		// before the model's assessment is recorded.
		var state observerNotebookState
		data, err := os.ReadFile(args[1])
		if err != nil || json.Unmarshal(data, &state) != nil {
			os.Exit(85)
		}
		state.Revision++
		writeHelperJSON(args[1], state)
		evidence, err := os.ReadFile(args[0])
		if err != nil {
			os.Exit(82)
		}
		fmt.Printf(`{"status":"observed","evidence":%s,"exploration":"A bounded exploratory note."}`+"\n", evidence)
	case mode == "notebook" && len(args) == 1:
		runNotebookHelper(args[0], input)
	case mode == "held-pipe":
		cmd := exec.Command("/bin/sh", "-c", "sleep 10")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if cmd.Start() != nil {
			os.Exit(90)
		}
		fmt.Print(`{"status":"empty","evidence":[],"exploration":""}`)
	default:
		os.Exit(83)
	}
	os.Exit(0)
}

// runNotebookHelper is a stand-in adapter over a JSON state file. It keeps the
// protocol facts the runner depends on — request ids checked before the
// revision, stale and retired refusals by exit status, append-only
// observations, a finding opening its own entry — and nothing more.
func runNotebookHelper(path string, input []byte) {
	var request struct {
		Operation string          `json:"operation"`
		EntryID   string          `json:"entry_id"`
		Request   json.RawMessage `json:"request"`
		Payload   json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(input, &request) != nil {
		os.Exit(84)
	}
	var state observerNotebookState
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &state) != nil {
		os.Exit(85)
	}
	switch request.Operation {
	case "revision":
		fmt.Printf("{\"revision\":%d}\n", state.Revision)
	case "entry":
		entry, ok := state.Entries[request.EntryID]
		if !ok {
			os.Exit(12)
		}
		_, _ = os.Stdout.Write(entry)
	case "record":
		var req observercontract.Request
		var assessment observercontract.Assessment
		if json.Unmarshal(request.Request, &req) != nil || json.Unmarshal(request.Payload, &assessment) != nil {
			os.Exit(86)
		}
		key := req.Identity.Qualify(req.RequestID)
		if previous, ok := state.Requests[key]; ok {
			if !bytes.Equal(previous, request.Payload) {
				os.Exit(14)
			}
			_, _ = os.Stdout.Write(state.Responses[key])
			return
		}
		if req.Revision != state.Revision {
			os.Exit(10)
		}
		entry := &observercontract.Entry{}
		if existing, ok := state.Entries[assessment.EntryID()]; ok {
			if json.Unmarshal(existing, entry) != nil {
				os.Exit(85)
			}
			if entry.State == observercontract.StateRetired {
				os.Exit(11)
			}
		} else if assessment.Finding != nil {
			entry = &observercontract.Entry{
				ID: assessment.EntryID(), Identity: assessment.Identity,
				Finding: assessment.Finding, State: observercontract.StateObserving,
				InFlight: []string{},
			}
		} else {
			os.Exit(12)
		}
		entry.Observations = append(entry.Observations, &assessment)
		entry.State, err = observercontract.TransitionFrom(entry.State, assessment.Outcome)
		if err != nil {
			os.Exit(87)
		}
		entryData, _ := json.Marshal(entry)
		state.Requests[key] = append(json.RawMessage(nil), request.Payload...)
		state.Responses[key] = entryData
		state.Entries[entry.ID] = entryData
		state.Revision++
		state.Records++
		if state.CrashOnce {
			// Committed, then died before answering: the runner sees a
			// failure it cannot distinguish from one that wrote nothing.
			state.CrashOnce = false
			writeHelperJSON(path, state)
			os.Exit(1)
		}
		writeHelperJSON(path, state)
		_, _ = os.Stdout.Write(entryData)
	default:
		os.Exit(88)
	}
}

func helperArgsAfterDash() []string {
	for i, arg := range os.Args {
		if arg == "--" {
			return os.Args[i+1:]
		}
	}
	return nil
}

func writeHelperJSON(path string, value any) {
	data, _ := json.Marshal(value)
	if os.WriteFile(path, data, 0o600) != nil {
		os.Exit(89)
	}
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func runWindowLabel(t *testing.T, runDir string) string {
	t.Helper()
	var record runRecord
	readJSONFixture(t, filepath.Join(runDir, "run.json"), &record)
	// All runner fixtures use UTC. The manual slot reports the previous day.
	slot, err := time.Parse(time.RFC3339, record.Slot)
	if err != nil {
		t.Fatal(err)
	}
	return slot.AddDate(0, 0, -1).Format("2006-01-02")
}
