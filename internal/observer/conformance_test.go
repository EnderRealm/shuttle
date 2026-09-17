package observer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type conformanceManifest struct {
	Scenarios []string `json:"scenarios"`
}

type conformanceScenario struct {
	Name  string            `json:"name"`
	Steps []conformanceStep `json:"steps"`
}

type conformanceStep struct {
	Operation string          `json:"operation"`
	Request   *wireRequest    `json:"request,omitempty"`
	Payload   string          `json:"payload,omitempty"`
	EntryID   string          `json:"entry_id,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Condition string          `json:"condition,omitempty"`
	Expect    json.RawMessage `json:"expect"`
}

type wireRequest struct {
	Identity  Identity `json:"identity"`
	Revision  int64    `json:"revision"`
	RequestID string   `json:"request_id"`
}

type conformanceEntry struct {
	ID               string                   `json:"id"`
	State            string                   `json:"state"`
	ExperimentStatus string                   `json:"experiment_status,omitempty"`
	Observations     []conformanceObservation `json:"observations"`
	InFlight         []string                 `json:"in_flight"`
}

type conformanceObservation struct {
	At      string `json:"at"`
	Outcome string `json:"outcome"`
}

type conformanceReceipt struct {
	ActionID  string `json:"action_id"`
	Status    string `json:"status"`
	Outcome   string `json:"outcome,omitempty"`
	Reference string `json:"reference,omitempty"`
	At        string `json:"at"`
}

type conformanceRetirement struct {
	EntryID  string   `json:"entry_id"`
	Reason   string   `json:"reason"`
	InFlight []string `json:"in_flight"`
	Revision int64    `json:"revision"`
	At       string   `json:"at"`
}

func TestMemoryConformance(t *testing.T) {
	root := filepath.Join("testdata", "conformance")
	var manifest conformanceManifest
	decodeConformanceFile(t, filepath.Join(root, "manifest.json"), &manifest)
	scenarioDir := filepath.Join(root, "scenarios")
	checkScenarioSet(t, scenarioDir, manifest.Scenarios)

	for _, name := range manifest.Scenarios {
		var scenario conformanceScenario
		path := filepath.Join(scenarioDir, name)
		decodeConformanceFile(t, path, &scenario)
		t.Run(scenario.Name, func(t *testing.T) {
			nb := NewMemory()
			outcomes := make([]any, 0, len(scenario.Steps))
			for i, step := range scenario.Steps {
				got, err := runConformanceStep(t, nb, scenarioDir, step)
				if err != nil {
					t.Fatalf("scenario %q step %d %s: unexpected error: %v", scenario.Name, i+1, step.Operation, err)
				}
				if err := compareConformanceOutcome(scenario.Name, i+1, step.Operation, step.Expect, got, outcomes); err != nil {
					t.Fatal(err)
				}
				outcomes = append(outcomes, got)
			}
		})
	}
}

func TestConformanceDifferenceIdentifiesScenarioStepAndOperation(t *testing.T) {
	err := compareConformanceOutcome(
		"example",
		4,
		"claim",
		json.RawMessage(`{"status":"ok","revision":2}`),
		map[string]any{"status": "refused", "revision": float64(2)},
		nil,
	)
	if err == nil {
		t.Fatal("got no difference")
	}
	for _, want := range []string{`scenario "example"`, "step 4", "claim", `"status":"ok"`, `"status":"refused"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("difference %q does not name %q", err, want)
		}
	}
}

func TestConformanceReplayComparesTheCompleteReceipt(t *testing.T) {
	previous := []any{map[string]any{
		"status":   "ok",
		"revision": float64(1),
		"receipt": map[string]any{
			"action_id": "wren/mead/action",
			"status":    "in-flight",
			"at":        "2026-09-17T07:00:00Z",
		},
	}}
	got := map[string]any{
		"status":   "ok",
		"revision": float64(1),
		"receipt": map[string]any{
			"action_id": "wren/mead/action",
			"status":    "in-flight",
			"at":        "2026-09-17T07:00:01Z",
		},
	}
	if err := compareConformanceOutcome("replay", 2, "claim", json.RawMessage(`{"same_as":1}`), got, previous); err == nil {
		t.Fatal("outcomes that differ only by receipt timestamp compared equal")
	}
}

func TestConformanceDecodeRejectsTrailingJSON(t *testing.T) {
	var manifest conformanceManifest
	if err := decodeConformanceJSON([]byte(`{"scenarios":[]} {"scenarios":[]}`), &manifest); err == nil {
		t.Fatal("a second JSON value was accepted")
	}
}

func TestConformanceWildcardIsOnlyATimestamp(t *testing.T) {
	if matchConformanceExpectation("*", "ok", "status") {
		t.Fatal("wildcard matched outside an at field")
	}
	if matchConformanceExpectation("*", "not-a-time", "at") {
		t.Fatal("wildcard matched a non-timestamp at field")
	}
	if !matchConformanceExpectation("*", "2026-09-17T07:00:00Z", "at") {
		t.Fatal("wildcard did not match an RFC3339 at field")
	}
}

func TestConformanceOutcomeRejectsUnexpectedError(t *testing.T) {
	if _, err := conformanceOutcome(0, nil, errors.New("storage failed")); err == nil {
		t.Fatal("a non-sentinel error became a conformance refusal")
	}
}

func decodeConformanceFile(t *testing.T, path string, out any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if err := decodeConformanceJSON(data, out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func decodeConformanceJSON(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return fmt.Errorf("trailing content: %w", err)
	}
	return nil
}

func checkScenarioSet(t *testing.T, dir string, named []string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	onDisk := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			t.Fatalf("%s contains non-scenario entry %q", dir, entry.Name())
		}
		onDisk = append(onDisk, entry.Name())
	}
	want := append([]string(nil), named...)
	sort.Strings(onDisk)
	sort.Strings(want)
	if !reflect.DeepEqual(onDisk, want) {
		t.Fatalf("%s scenario set differs: manifest names %v, directory holds %v", dir, want, onDisk)
	}
}

func runConformanceStep(t *testing.T, nb Notebook, scenarioDir string, step conformanceStep) (any, error) {
	t.Helper()
	var (
		result any
		err    error
	)
	switch step.Operation {
	case "revision":
	case "entry":
		result, err = nb.Entry(step.EntryID)
	case "register":
		result, err = nb.Register(requestFor(t, step), questionPayload(t, scenarioDir, step.Payload))
	case "activate":
		result, err = nb.Activate(requestFor(t, step), experimentPayload(t, scenarioDir, step.Payload))
	case "record":
		result, err = nb.Record(requestFor(t, step), assessmentPayload(t, scenarioDir, step.Payload))
	case "claim":
		result, err = nb.Claim(requestFor(t, step), actionPayload(t, scenarioDir, step.Payload))
	case "settle":
		result, err = nb.Settle(requestFor(t, step), receiptPayload(t, scenarioDir, step.Payload))
	case "retire":
		result, err = nb.Retire(requestFor(t, step), step.EntryID, step.Reason)
	case "reopen":
		result, err = nb.Reopen(requestFor(t, step), step.EntryID, step.Condition)
	default:
		t.Fatalf("unknown operation %q", step.Operation)
	}
	return conformanceOutcome(nb.Revision(), result, err)
}

func requestFor(t *testing.T, step conformanceStep) Request {
	t.Helper()
	if step.Request == nil {
		t.Fatalf("%s requires request", step.Operation)
	}
	return Request{
		Identity:  step.Request.Identity,
		Revision:  step.Request.Revision,
		RequestID: step.Request.RequestID,
	}
}

func payloadPath(t *testing.T, scenarioDir, name string) string {
	t.Helper()
	if name == "" {
		t.Fatal("payload path is required")
	}
	testdata, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	path, err := filepath.Abs(filepath.Join(scenarioDir, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(testdata, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("payload %q resolves outside testdata", name)
	}
	return path
}

func payloadBytes(t *testing.T, scenarioDir, name string) []byte {
	t.Helper()
	path := payloadPath(t, scenarioDir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("payload %q: %v", name, err)
	}
	return data
}

func questionPayload(t *testing.T, dir, name string) *Question {
	t.Helper()
	out, err := ValidateQuestion(payloadBytes(t, dir, name))
	if err != nil {
		t.Fatalf("payload %q: %v", name, err)
	}
	return out
}

func experimentPayload(t *testing.T, dir, name string) *Experiment {
	t.Helper()
	out, err := ValidateExperiment(payloadBytes(t, dir, name))
	if err != nil {
		t.Fatalf("payload %q: %v", name, err)
	}
	return out
}

func assessmentPayload(t *testing.T, dir, name string) *Assessment {
	t.Helper()
	out, err := ValidateAssessment(payloadBytes(t, dir, name))
	if err != nil {
		t.Fatalf("payload %q: %v", name, err)
	}
	return out
}

func actionPayload(t *testing.T, dir, name string) *Action {
	t.Helper()
	out, err := ValidateAction(payloadBytes(t, dir, name))
	if err != nil {
		t.Fatalf("payload %q: %v", name, err)
	}
	return out
}

func receiptPayload(t *testing.T, dir, name string) *Receipt {
	t.Helper()
	out, err := ValidateReceipt(payloadBytes(t, dir, name))
	if err != nil {
		t.Fatalf("payload %q: %v", name, err)
	}
	return out
}

func conformanceOutcome(revision int64, result any, err error) (any, error) {
	out := map[string]any{"revision": revision}
	if err != nil {
		refusal, ok := conformanceRefusal(err)
		if !ok {
			return nil, err
		}
		out["status"] = "refused"
		out["refusal"] = refusal
		return normalizeConformance(out), nil
	}
	out["status"] = "ok"
	switch value := result.(type) {
	case *Entry:
		entry := conformanceEntry{
			ID:               value.ID,
			State:            value.State,
			ExperimentStatus: value.ExperimentStatus,
			Observations:     make([]conformanceObservation, 0, len(value.Observations)),
			InFlight:         append([]string{}, value.InFlight...),
		}
		for _, observation := range value.Observations {
			entry.Observations = append(entry.Observations, conformanceObservation{
				At:      observation.At,
				Outcome: observation.Outcome,
			})
		}
		out["entry"] = entry
	case *Receipt:
		out["receipt"] = conformanceReceipt{
			ActionID:  value.ActionID,
			Status:    value.Status,
			Outcome:   value.Outcome,
			Reference: value.Reference,
			At:        value.At,
		}
	case *Retirement:
		out["retirement"] = conformanceRetirement{
			EntryID:  value.EntryID,
			Reason:   value.Reason,
			InFlight: append([]string{}, value.InFlight...),
			Revision: value.Revision,
			At:       value.At,
		}
	}
	return normalizeConformance(out), nil
}

func conformanceRefusal(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrStaleRevision):
		return "stale-revision", true
	case errors.Is(err, ErrRetired):
		return "retired", true
	case errors.Is(err, ErrNoEntry):
		return "no-entry", true
	case errors.Is(err, ErrNotClaimed):
		return "not-claimed", true
	case errors.Is(err, ErrRequestConflict):
		return "request-conflict", true
	default:
		return "", false
	}
}

func normalizeConformance(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}

func compareConformanceOutcome(name string, step int, operation string, expected json.RawMessage, got any, previous []any) error {
	var want any
	if err := json.Unmarshal(expected, &want); err != nil {
		return fmt.Errorf("scenario %q step %d %s: decode expectation: %w", name, step, operation, err)
	}
	sameAs := false
	if object, ok := want.(map[string]any); ok && len(object) == 1 {
		if raw, ok := object["same_as"]; ok {
			index, ok := raw.(float64)
			if !ok || index < 1 || index != float64(int(index)) || int(index) > len(previous) {
				return fmt.Errorf("scenario %q step %d %s: invalid same_as %v", name, step, operation, raw)
			}
			want = previous[int(index)-1]
			sameAs = true
		}
	}
	if sameAs && reflect.DeepEqual(got, want) || !sameAs && matchConformanceExpectation(want, got, "") {
		return nil
	}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	return fmt.Errorf("scenario %q step %d %s: outcome differed: expected %s, got %s", name, step, operation, wantJSON, gotJSON)
}

func matchConformanceExpectation(want, got any, field string) bool {
	if value, ok := want.(string); ok && value == "*" && field == "at" {
		actual, ok := got.(string)
		if !ok {
			return false
		}
		_, err := time.Parse(time.RFC3339, actual)
		return err == nil
	}
	switch expected := want.(type) {
	case map[string]any:
		actual, ok := got.(map[string]any)
		if !ok || len(actual) != len(expected) {
			return false
		}
		for key, value := range expected {
			if !matchConformanceExpectation(value, actual[key], key) {
				return false
			}
		}
		return true
	case []any:
		actual, ok := got.([]any)
		if !ok || len(actual) != len(expected) {
			return false
		}
		for i := range expected {
			if !matchConformanceExpectation(expected[i], actual[i], field) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(want, got)
	}
}
