package observer

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
)

type commandFixtureState struct {
	Revision    int64             `json:"revision"`
	Records     int               `json:"records"`
	Fingerprint map[string]string `json:"fingerprint"`
}

func TestCommandNotebookUsesStableRequestsAndRevisionChecks(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	entryPath := filepath.Join(dir, "entry.json")
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment-unprompted.json")
	memory := NewMemory()
	entry, err := memory.Record(Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "seed",
	}, assessment)
	if err != nil {
		t.Fatal(err)
	}
	writeCommandFixtureJSON(t, entryPath, entry)
	writeCommandFixtureJSON(t, statePath, commandFixtureState{Fingerprint: map[string]string{}})

	notebook := commandNotebookFixture(t, dir, statePath, entryPath)
	request := Request{Identity: assessment.Identity, Revision: 0, RequestID: "run-1-assessment-001"}
	first, err := notebook.Record(context.Background(), request, assessment)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := notebook.Record(context.Background(), request, assessment)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalTestJSON(first) != canonicalTestJSON(replayed) {
		t.Fatal("replay did not return the original entry")
	}
	var state commandFixtureState
	readCommandFixtureJSON(t, statePath, &state)
	if state.Revision != 1 || state.Records != 1 {
		t.Fatalf("state after replay = %+v, want one append at revision 1", state)
	}

	changed := *assessment
	changed.Interpretation += " changed"
	if _, err := notebook.Record(context.Background(), request, &changed); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed request error = %v, want request conflict", err)
	}
	stale := Request{Identity: assessment.Identity, Revision: 0, RequestID: "another-request"}
	if _, err := notebook.Record(context.Background(), stale, assessment); !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("stale request error = %v, want stale revision", err)
	}
}

func TestCommandNotebookRecordRequiresSubmittedAssessmentInResponse(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	entryPath := filepath.Join(dir, "entry.json")
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment-unprompted.json")
	memory := NewMemory()
	entry, err := memory.Record(Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "seed",
	}, assessment)
	if err != nil {
		t.Fatal(err)
	}
	writeCommandFixtureJSON(t, entryPath, entry)
	writeCommandFixtureJSON(t, statePath, commandFixtureState{Fingerprint: map[string]string{}})

	changed := *assessment
	changed.Interpretation += " a later interpretation"
	changed.At = "2026-09-19T07:00:00Z"
	notebook := commandNotebookFixture(t, dir, statePath, entryPath)
	_, err = notebook.Record(context.Background(), Request{
		Identity: changed.Identity, Revision: 0, RequestID: "later",
	}, &changed)
	if err == nil || !strings.Contains(err.Error(), "does not contain the submitted assessment") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandNotebookFindingResponseHasNoInFlightActions(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	entryPath := filepath.Join(dir, "entry.json")
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment-unprompted.json")
	memory := NewMemory()
	entry, err := memory.Record(Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "seed",
	}, assessment)
	if err != nil {
		t.Fatal(err)
	}
	entry.InFlight = []string{assessment.Identity.Qualify("impossible-action")}
	writeCommandFixtureJSON(t, entryPath, entry)
	writeCommandFixtureJSON(t, statePath, commandFixtureState{Fingerprint: map[string]string{}})

	notebook := commandNotebookFixture(t, dir, statePath, entryPath)
	_, err = notebook.Record(context.Background(), Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "record",
	}, assessment)
	if err == nil || !strings.Contains(err.Error(), "finding with in-flight actions") {
		t.Fatalf("error = %v", err)
	}

	entry, err = memory.Entry(assessment.EntryID())
	if err != nil {
		t.Fatal(err)
	}
	entry.Finding.Changes = "altered by adapter"
	writeCommandFixtureJSON(t, entryPath, entry)
	writeCommandFixtureJSON(t, statePath, commandFixtureState{Fingerprint: map[string]string{}})
	_, err = notebook.Record(context.Background(), Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "record",
	}, assessment)
	if err == nil || !strings.Contains(err.Error(), "differs from the submitted finding") {
		t.Fatalf("finding metadata error = %v", err)
	}
}

func TestCommandNotebookRecordRejectsRetiredSuccessResponse(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	entryPath := filepath.Join(dir, "entry.json")
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment-unprompted.json")
	memory := NewMemory()
	if _, err := memory.Record(Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "seed",
	}, assessment); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Retire(Request{
		Identity: assessment.Identity, Revision: 1, RequestID: "retire",
	}, assessment.EntryID(), "retired by fixture"); err != nil {
		t.Fatal(err)
	}
	entry, err := memory.Entry(assessment.EntryID())
	if err != nil {
		t.Fatal(err)
	}
	writeCommandFixtureJSON(t, entryPath, entry)
	writeCommandFixtureJSON(t, statePath, commandFixtureState{Fingerprint: map[string]string{}})

	notebook := commandNotebookFixture(t, dir, statePath, entryPath)
	_, err = notebook.Record(context.Background(), Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "record",
	}, assessment)
	if err == nil || !strings.Contains(err.Error(), "transition from") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandNotebookReplaySurvivesAnInterveningMutation(t *testing.T) {
	dir := t.TempDir()
	question := questionPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/question.json")
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment.json")
	assessment.Outcome = OutcomeInsufficient
	assessment.Interpretation = "The first read was insufficient."
	memory := NewMemory()
	if _, err := memory.Register(Request{
		Identity: question.Identity, Revision: 0, RequestID: "register",
	}, question); err != nil {
		t.Fatal(err)
	}
	request := Request{Identity: question.Identity, Revision: 1, RequestID: "first-read"}
	original, err := memory.Record(request, assessment)
	if err != nil {
		t.Fatal(err)
	}
	conclusive := *assessment
	conclusive.Outcome = OutcomeEffectObserved
	conclusive.Interpretation = "A later read observed the effect."
	conclusive.At = "2026-09-18T07:00:00-07:00"
	if _, err := memory.Record(Request{
		Identity: question.Identity, Revision: 2, RequestID: "later-read",
	}, &conclusive); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal(assessment)
	entry, _ := json.Marshal(original)
	key := request.Identity.Qualify(request.RequestID)
	statePath := filepath.Join(dir, "state.json")
	entryPath := filepath.Join(dir, "entry.json")
	writeCommandFixtureJSON(t, statePath, commandFixtureState{
		Revision: 3, Records: 2,
		Fingerprint: map[string]string{key: string(payload)},
	})
	if err := os.WriteFile(entryPath, entry, 0o600); err != nil {
		t.Fatal(err)
	}

	notebook := commandNotebookFixture(t, dir, statePath, entryPath)
	got, err := notebook.Record(context.Background(), request, assessment)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateObserving {
		t.Fatalf("replayed state = %q, want original observing snapshot", got.State)
	}
}

func TestCommandNotebookRecordAppliesReferenceRequirementsBeforeMutation(t *testing.T) {
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment-unprompted.json")
	changed := *assessment
	changed.Outcome = OutcomeEffectAbsent
	notebook := CommandNotebook{
		Argv: []string{os.Args[0], "-test.run=TestCommandNotebookHelper", "--", "unreachable"},
		Dir:  t.TempDir(), Env: []string{}, Timeout: 5 * time.Second,
	}
	_, err := notebook.Record(context.Background(), Request{
		Identity: changed.Identity, Revision: 0, RequestID: "record",
	}, &changed)
	if err == nil || !strings.Contains(err.Error(), "needs an experiment") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandNotebookDoesNotInheritCWDOrEnvironment(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	entryPath := filepath.Join(dir, "entry.json")
	writeCommandFixtureJSON(t, statePath, commandFixtureState{Fingerprint: map[string]string{}})
	writeCommandFixtureJSON(t, entryPath, map[string]any{})
	t.Setenv("SHUTTLE_PROJECT_SECRET", "must-not-cross")
	notebook := commandNotebookFixture(t, dir, statePath, entryPath)
	revision, err := notebook.Revision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if revision != 0 {
		t.Fatalf("revision = %d", revision)
	}
}

func TestCommandNotebookRejectsMalformedSuccessfulResponse(t *testing.T) {
	notebook := CommandNotebook{
		Argv: []string{os.Args[0], "-test.run=TestCommandNotebookHelper", "--", "malformed"},
		Dir:  t.TempDir(), Env: []string{}, Timeout: 5 * time.Second,
	}
	if _, err := notebook.Revision(context.Background()); err == nil || !strings.Contains(err.Error(), "multiple JSON values") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandNotebookRequiresRevisionField(t *testing.T) {
	notebook := CommandNotebook{
		Argv: []string{os.Args[0], "-test.run=TestCommandNotebookHelper", "--", "missing-revision"},
		Dir:  t.TempDir(), Env: []string{}, Timeout: 5 * time.Second,
	}
	if _, err := notebook.Revision(context.Background()); err == nil || !strings.Contains(err.Error(), "revision is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandNotebookSettleRequiresSettledReceipt(t *testing.T) {
	identity := Identity{Domain: "harness", Observer: "shuttle"}
	notebook := CommandNotebook{
		Argv: []string{os.Args[0], "-test.run=TestCommandNotebookHelper", "--", "unreachable"},
		Dir:  t.TempDir(), Env: []string{}, Timeout: 5 * time.Second,
	}
	_, err := notebook.Settle(context.Background(), Request{
		Identity: identity, Revision: 1, RequestID: "settle",
	}, &Receipt{
		Envelope: Envelope{ContractVersion: ContractVersion},
		Identity: identity, ActionID: identity.Qualify("action"),
		Status: ReceiptInFlight, At: "2026-09-18T12:00:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "settling records the outcome") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandNotebookChecksMutationPostconditions(t *testing.T) {
	question := questionPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/question.json")
	memory := NewMemory()
	entry, err := memory.Register(Request{
		Identity: question.Identity, Revision: 0, RequestID: "register",
	}, question)
	if err != nil {
		t.Fatal(err)
	}
	wrong := *entry
	wrong.Question = nil
	wrong.Finding = &Finding{
		ID: entry.ID, Subject: question.Subject, Question: "Different shape", Changes: "Different authority",
	}
	path := filepath.Join(t.TempDir(), "response.json")
	writeCommandFixtureJSON(t, path, &wrong)
	client := CommandNotebook{
		Argv: []string{os.Args[0], "-test.run=TestCommandNotebookHelper", "--", "static", path},
		Dir:  filepath.Dir(path), Env: []string{}, Timeout: 5 * time.Second,
	}
	if _, err := client.Register(context.Background(), Request{
		Identity: question.Identity, Revision: 0, RequestID: "register",
	}, question); err == nil || !strings.Contains(err.Error(), "submitted observing question") {
		t.Fatalf("register error = %v", err)
	}
	withAction := *entry
	withAction.InFlight = []string{question.Identity.Qualify("impossible-action")}
	writeCommandFixtureJSON(t, path, &withAction)
	if _, err := client.Register(context.Background(), Request{
		Identity: question.Identity, Revision: 0, RequestID: "register",
	}, question); err == nil || !strings.Contains(err.Error(), "submitted observing question") {
		t.Fatalf("register in-flight error = %v", err)
	}

	submitted := &Receipt{
		Envelope: Envelope{ContractVersion: ContractVersion},
		Identity: question.Identity, ActionID: question.Identity.Qualify("action"),
		Status: ReceiptSettled, Outcome: ReceiptFailed, At: "2026-09-18T12:00:00Z",
	}
	returned := *submitted
	returned.Outcome = ReceiptRefused
	writeCommandFixtureJSON(t, path, &returned)
	if _, err := client.Settle(context.Background(), Request{
		Identity: question.Identity, Revision: 1, RequestID: "settle",
	}, submitted); err == nil || !strings.Contains(err.Error(), "differs from the submitted receipt") {
		t.Fatalf("settle error = %v", err)
	}

	retirement := &Retirement{
		EntryID: question.ID, Reason: "done", At: "2026-09-18T12:00:00Z",
		InFlight: []string{
			question.Identity.Qualify("action"),
			question.Identity.Qualify("action"),
		},
		Revision: 1,
	}
	writeCommandFixtureJSON(t, path, retirement)
	if _, err := client.Retire(context.Background(), Request{
		Identity: question.Identity, Revision: 0, RequestID: "retire",
	}, question.ID, "done"); err == nil || !strings.Contains(err.Error(), "appears twice") {
		t.Fatalf("retire error = %v", err)
	}
}

func TestCommandNotebookBoundsInheritedPipeWait(t *testing.T) {
	notebook := CommandNotebook{
		Argv: []string{os.Args[0], "-test.run=TestCommandNotebookHelper", "--", "held-pipe"},
		Dir:  t.TempDir(), Env: []string{}, Timeout: 100 * time.Millisecond,
	}
	start := time.Now()
	if _, err := notebook.Revision(context.Background()); err == nil {
		t.Fatal("held pipe returned no error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("held pipe took %s, want bounded by WaitDelay", elapsed)
	}
}

func TestValidateEntryRejectsInconsistentActionAndRetirementState(t *testing.T) {
	assessment := assessmentPayload(t, filepath.Join("testdata", "conformance", "scenarios"), "../../wren/assessment-unprompted.json")
	notebook := NewMemory()
	entry, err := notebook.Record(Request{
		Identity: assessment.Identity, Revision: 0, RequestID: "record",
	}, assessment)
	if err != nil {
		t.Fatal(err)
	}
	entry.Observations[0].Evidence.Subject.Instance = "another-subject"
	if err := validateEntry(entry); err == nil || !strings.Contains(err.Error(), "observations[0].evidence.subject") {
		t.Fatalf("subject validation error = %v", err)
	}
	entry, err = notebook.Entry(assessment.EntryID())
	if err != nil {
		t.Fatal(err)
	}
	entry.InFlight = []string{"another/observer/action"}
	if err := validateEntry(entry); err == nil || !strings.Contains(err.Error(), "in_flight[0]") {
		t.Fatalf("in-flight validation error = %v", err)
	}

	if _, err := notebook.Retire(Request{
		Identity: assessment.Identity, Revision: 1, RequestID: "retire",
	}, assessment.EntryID(), "no longer relevant"); err != nil {
		t.Fatal(err)
	}
	retired, err := notebook.Entry(assessment.EntryID())
	if err != nil {
		t.Fatal(err)
	}
	retired.Retirement.EntryID = assessment.Identity.Qualify("different")
	if err := validateEntry(retired); err == nil || !strings.Contains(err.Error(), "retirement.entry_id") {
		t.Fatalf("retirement validation error = %v", err)
	}
}

func commandNotebookFixture(t *testing.T, dir, state, entry string) CommandNotebook {
	t.Helper()
	return CommandNotebook{
		Argv: []string{
			os.Args[0], "-test.run=TestCommandNotebookHelper", "--",
			"stateful", state, entry, dir,
		},
		Dir: dir, Env: []string{}, Timeout: 5 * time.Second,
	}
}

func TestCommandNotebookHelper(t *testing.T) {
	args := helperArgs()
	if len(args) == 0 {
		return
	}
	if args[0] == "malformed" {
		fmt.Print(`{"revision":0} {"revision":1}`)
		os.Exit(0)
	}
	if args[0] == "missing-revision" {
		fmt.Print(`{}`)
		os.Exit(0)
	}
	if args[0] == "static" && len(args) == 2 {
		_, _ = io.Copy(io.Discard, os.Stdin)
		data, err := os.ReadFile(args[1])
		if err != nil {
			os.Exit(99)
		}
		_, _ = os.Stdout.Write(data)
		os.Exit(0)
	}
	if args[0] == "held-pipe" {
		cmd := exec.Command("/bin/sh", "-c", "sleep 10")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if cmd.Start() != nil {
			os.Exit(98)
		}
		fmt.Print(`{"revision":0}`)
		os.Exit(0)
	}
	if len(args) != 4 || args[0] != "stateful" {
		os.Exit(90)
	}
	statePath, entryPath, wantDir := args[1], args[2], args[3]
	cwd, _ := os.Getwd()
	cwd, _ = filepath.EvalSymlinks(cwd)
	wantDir, _ = filepath.EvalSymlinks(wantDir)
	if cwd != wantDir || os.Getenv("SHUTTLE_PROJECT_SECRET") != "" {
		os.Exit(91)
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(92)
	}
	var request struct {
		Operation string          `json:"operation"`
		Request   Request         `json:"request"`
		Payload   json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(input, &request) != nil {
		os.Exit(93)
	}
	var state commandFixtureState
	data, err := os.ReadFile(statePath)
	if err != nil || json.Unmarshal(data, &state) != nil {
		os.Exit(94)
	}
	switch request.Operation {
	case "revision":
		fmt.Printf("{\"revision\":%d}\n", state.Revision)
	case "record":
		fingerprint := string(bytes.TrimSpace(request.Payload))
		key := request.Request.Identity.Qualify(request.Request.RequestID)
		if previous, ok := state.Fingerprint[key]; ok {
			if previous != fingerprint {
				os.Exit(14)
			}
		} else {
			if request.Request.Revision != state.Revision {
				os.Exit(10)
			}
			state.Fingerprint[key] = fingerprint
			state.Revision++
			state.Records++
			writeHelperState(statePath, state)
		}
		entry, err := os.ReadFile(entryPath)
		if err != nil {
			os.Exit(95)
		}
		_, _ = os.Stdout.Write(entry)
	default:
		os.Exit(96)
	}
	os.Exit(0)
}

func helperArgs() []string {
	for i, arg := range os.Args {
		if arg == "--" {
			return os.Args[i+1:]
		}
	}
	return nil
}

func writeHelperState(path string, state commandFixtureState) {
	data, _ := json.Marshal(state)
	if os.WriteFile(path, data, 0o600) != nil {
		os.Exit(97)
	}
}

func writeCommandFixtureJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readCommandFixtureJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func canonicalTestJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}
