package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"shuttle/internal/ledger"
	observercontract "shuttle/internal/observer"
)

// failedRecordEnv turns the fixture observer into one whose run fails on the
// notebook: the collector returns the harness finding's evidence, the model
// assesses it, and the notebook refuses the record twice — the run's attempt
// and its replay — before it admits it. The run is left failed with its
// assessment unrecorded, which is what reconcile resumes.
func failedRecordEnv(t *testing.T) {
	t.Helper()
	// Read before the fixture moves the process to an unrelated directory.
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "observer", "testdata", "harness", "assessment-unprompted.json"))
	if err != nil {
		t.Fatal(err)
	}
	env := newObserverEnv(t)
	root := filepath.Dir(env.domainRoot)
	finding, err := observercontract.ValidateAssessment(data)
	if err != nil {
		t.Fatal(err)
	}
	state, err := observercontract.TransitionFrom(observercontract.StateObserving, finding.Outcome)
	if err != nil {
		t.Fatal(err)
	}
	writeJSON := func(name string, value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	collection := writeJSON("collection.json", map[string]any{
		"status": "observed", "evidence": []any{finding.Evidence}, "exploration": "One finding.",
	})
	output := writeJSON("observer-output.json", map[string]any{
		"silent": true, "assessments": []any{finding}, "actions": []any{},
	})
	entry := writeJSON("entry.json", observercontract.Entry{
		ID: finding.EntryID(), Identity: finding.Identity, Finding: finding.Finding, State: state,
		Observations: []*observercontract.Assessment{finding}, InFlight: []string{},
	})
	identity := finding.Identity
	if err := os.WriteFile(filepath.Join(root, "definition.json"), []byte(fmt.Sprintf(`{
  "contract_version": "observer-run/v1",
  "revision": "fixture-r1",
  "identity": {"domain": %q, "observer": %q},
  "questions": [],
  "instructions": "Report only what the retained evidence supports."
}`, identity.Domain, identity.Observer)), 0o600); err != nil {
		t.Fatal(err)
	}
	writeScript(t, filepath.Join(root, "collector"), fmt.Sprintf("#!/bin/sh\ncat >/dev/null\ncat %q\n", collection))
	records := filepath.Join(root, "record-attempts")
	writeScript(t, filepath.Join(root, "notebook"), fmt.Sprintf(`#!/bin/sh
request=$(cat)
case "$request" in
  *'"operation":"record"'*)
    printf 'record\n' >> %q
    if [ "$(wc -l < %q)" -le 2 ]; then
      echo 'notebook store is locked' >&2
      exit 1
    fi
    cat %q
    ;;
  *)
    printf '%%s\n' '{"revision":0}'
    ;;
esac
`, records, records, entry))
	calls := filepath.Join(root, "model-calls")
	writeScript(t, filepath.Join(root, "claude"), fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then exit 0; fi
cat >/dev/null
printf 'call\n' >> %q
if [ "$(wc -l < %q)" -eq 1 ]; then
  printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.001}'
  exit 0
fi
mkdir -p out
cp %q out/observer.json
printf '%%s\n' '{"type":"system","subtype":"init","session_id":"observer-cli"}'
printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"session_id":"observer-cli","num_turns":1,"total_cost_usd":0.002}'
`, calls, calls, output))
}

func execReconcile(t *testing.T, runID string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"reconcile", runID})
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetArgs(nil)
	})
	err := rootCmd.Execute()
	return out.String(), err
}

func TestReconcileConvergesAFailedObserverRun(t *testing.T) {
	failedRecordEnv(t)
	rootCmd.SetArgs([]string{"run", "watch", "--slot", yesterday()})
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		runSlot = ""
	})
	if err := rootCmd.Execute(); err == nil || !strings.Contains(err.Error(), "notebook store is locked") {
		t.Fatalf("run = %v, want the refused record surfaced", err)
	}
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runs, err := store.Runs("watch", 1)
	if err != nil || len(runs) != 1 || runs[0].Status != ledger.StatusFailed {
		t.Fatalf("runs = %+v, %v; want the failed manual run", runs, err)
	}
	runID, runDir := runs[0].ID, runs[0].RunDir.String

	out, err := execReconcile(t, runID)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if out != "completed\t"+runDir+"\n" {
		t.Fatalf("output = %q", out)
	}
	row, err := store.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != ledger.StatusCompleted || !row.Silent || row.DeliveryStatus.String != ledger.DeliverySkipped {
		t.Fatalf("row = %+v, want it completed, silent and not delivered", row)
	}
	var outcome struct {
		Collection      string `json:"collection"`
		EvidenceCount   int    `json:"evidence_count"`
		AssessmentCount int    `json:"assessment_count"`
		Notebook        string `json:"notebook_status"`
		Detail          string `json:"detail"`
	}
	data, err := os.ReadFile(filepath.Join(runDir, "observer-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.Collection != "observed" || outcome.EvidenceCount != 1 || outcome.AssessmentCount != 1 ||
		outcome.Notebook != "applied" || outcome.Detail != "" {
		t.Fatalf("observer-result.json = %s", data)
	}

	// Converged, the run is no longer failed: a second reconcile is refused
	// and says why.
	if _, err := execReconcile(t, runID); err == nil || !strings.Contains(err.Error(), "reconcile "+runID) ||
		!strings.Contains(err.Error(), "only a failed run") {
		t.Fatalf("second reconcile = %v, want the completed run refused", err)
	}
	if _, err := execReconcile(t, "watch-19700101-000000-0000"); err == nil || !strings.Contains(err.Error(), "not in the ledger") {
		t.Fatalf("unknown run = %v", err)
	}
}
