package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/output"
	"shuttle/internal/provider"
	"shuttle/internal/provider/registry"
)

type liveFixture struct {
	root     string
	calls    string
	store    *ledger.Store
	def      *agentdef.Definition
	pipeline *Pipeline
	slot     time.Time
}

func newLiveFixture(t *testing.T, mode string) *liveFixture {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "shuttle-live-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	calls := filepath.Join(root, "model-calls")
	claude := filepath.Join(root, "claude")
	report := "# Daily report\n\n" + strings.Repeat("A retained report line with enough content for delivery validation. ", 5) + "\n"
	actual := fmt.Sprintf("mkdir -p out\nprintf '%%s' %q > out/report.md\nprintf '%%s\\n' '{\"email\":{\"subject\":\"Daily report\"},\"silent\":false}' > out/outputs.json\n", report)
	switch mode {
	case "output-missing":
		actual = "printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"live-session\",\"num_turns\":1,\"total_cost_usd\":0.01}'\n"
	case "silent":
		actual = "mkdir -p out\nprintf '# Quiet day\\n' > out/report.md\nprintf '%s\\n' '{\"silent\":true}' > out/outputs.json\n" +
			"printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"live-session\",\"num_turns\":1,\"total_cost_usd\":0.01}'\n"
	case "died":
		actual = "echo 'runtime died' >&2\nexit 3\n"
	case "timeout":
		actual = "sleep 300\n"
	case "unknown-cost":
		actual += "printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"live-session\"}'\nprintf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"live-session\",\"num_turns\":1,\"usage\":{\"input_tokens\":11,\"output_tokens\":7}}'\nexit 0\n"
	default:
		actual += "printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"live-session\"}'\nprintf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"live-session\",\"num_turns\":2,\"total_cost_usd\":0.0123,\"usage\":{\"input_tokens\":11,\"output_tokens\":7}}'\nexit 0\n"
	}
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  exit 0
fi
cat >/dev/null
printf 'call\n' >> %q
calls=$(wc -l < %q)
if [ "$calls" -eq 1 ]; then
  printf '%%s\n' '{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.001}'
  exit 0
fi
%s`, calls, calls, actual)
	if err := os.WriteFile(claude, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	srt := filepath.Join(root, "srt")
	if err := os.WriteFile(srt, []byte(`#!/bin/sh
if [ "$3" = "-c" ]; then
  exit 0
fi
shift 3
exec "$@"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	reports := filepath.Join(root, reportsDir)
	if err := output.InitReports(reports); err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	cfg := &config.Config{
		Anthropic: config.Anthropic{KeychainItem: "model"},
		Email: config.Email{
			Provider: "cloudflare", AccountID: "account", From: "from@example.test",
			To: []string{"to@example.test"}, KeychainItem: "mail",
		},
		Paths: config.Paths{ClaudeBin: claude, SRTBin: srt},
	}
	def := &agentdef.Definition{
		Dir: filepath.Join(root, "agents", "observer"), Name: "observer", Prompt: "Write the report.",
		Manifest: agentdef.Manifest{
			Name: "observer",
			Runtime: agentdef.Runtime{
				Kind: "claude", Auth: "api_key", Model: "haiku", MaxTurns: 2,
				MaxBudgetUSD: 0.05, TimeoutSeconds: 10, Tools: []string{"Read", "Write"},
			},
			Triggers: []agentdef.Trigger{{Kind: "cron", Expression: "0 4 * * *", Timezone: "UTC"}},
			Inputs:   []agentdef.Input{{Provider: "prior-reports", Args: map[string]any{"agent": "observer"}}},
			Outputs: agentdef.Outputs{
				Email: &agentdef.EmailOutput{Max: 1},
				File:  &agentdef.FileOutput{Dir: "observer", Max: 1},
			},
			Sandbox: agentdef.Sandbox{Kind: "srt"},
		},
	}
	pipeline := &Pipeline{
		Ledger: store, Config: cfg, Home: root, HomeDir: root, PATH: "/usr/bin:/bin",
		FindKey: func(item string) (string, error) {
			if item != "model" && item != "mail" {
				return "", fmt.Errorf("missing key %q", item)
			}
			return "fixture-secret", nil
		},
	}
	return &liveFixture{
		root: root, calls: calls, store: store, def: def, pipeline: pipeline,
		slot: time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second),
	}
}

func (f *liveFixture) claim(t *testing.T, trigger string) *ledger.Run {
	t.Helper()
	run, ok, err := f.store.Claim(f.def.Name, f.slot, trigger)
	if err != nil || !ok {
		t.Fatalf("claim = (%t, %v)", ok, err)
	}
	return run
}

func TestPipelineRecordsTheCompleteControlledRun(t *testing.T) {
	f := newLiveFixture(t, "success")
	run := f.claim(t, ledger.TriggerManual)
	got, err := f.pipeline.Run(context.Background(), f.def, run, Options{NoDeliver: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != run.ID || filepath.Base(got.RunDir) != run.ID {
		t.Fatalf("result = %+v, want artifacts under %s", got, run.ID)
	}
	row, err := f.store.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != ledger.StatusCompleted || !row.RunDir.Valid || row.SessionID.String != "live-session" {
		t.Fatalf("ledger row = %+v", row)
	}
	if !row.CostUSD.Valid || row.CostUSD.Float64 != 0.0123 ||
		row.InputTokens.Int64 != 11 || row.OutputTokens.Int64 != 7 || row.NumTurns.Int64 != 2 {
		t.Fatalf("ledger result = cost %v, tokens %v/%v, turns %v",
			row.CostUSD, row.InputTokens, row.OutputTokens, row.NumTurns)
	}
	if row.DeliveryStatus.String != ledger.DeliverySkipped || !row.OutputsApplied.Valid {
		t.Fatalf("delivery/applied = %v/%v", row.DeliveryStatus, row.OutputsApplied)
	}
	for _, name := range []string{
		"run.json", "prompt.md", transcriptFile, stderrFile, resultFile, "applied.json",
		filepath.Join("inputs", "prior-reports.json"),
	} {
		if _, err := os.Stat(filepath.Join(got.RunDir, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	prompt, _ := os.ReadFile(filepath.Join(got.RunDir, "prompt.md"))
	transcript, _ := os.ReadFile(filepath.Join(got.RunDir, transcriptFile))
	if !strings.Contains(string(prompt), run.ID) || !strings.Contains(string(transcript), "live-session") {
		t.Fatalf("prompt or transcript is not tied to %s", run.ID)
	}
	var record runRecord
	data, _ := os.ReadFile(filepath.Join(got.RunDir, "run.json"))
	if err := json.Unmarshal(data, &record); err != nil || record.PID == 0 || len(record.Argv) == 0 {
		t.Fatalf("run.json = %+v, %v", record, err)
	}
	if calls := readCalls(t, f.calls); calls != 2 {
		t.Fatalf("model calls = %d, want one auth probe and one run", calls)
	}
}

func TestPipelineKeepsUnknownCostUnknown(t *testing.T) {
	f := newLiveFixture(t, "unknown-cost")
	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{NoDeliver: true}); err != nil {
		t.Fatal(err)
	}
	row, err := f.store.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.CostUSD.Valid {
		t.Fatalf("cost = %v, want unknown rather than zero", row.CostUSD)
	}
	if !row.InputTokens.Valid || row.InputTokens.Int64 != 11 {
		t.Fatalf("tokens = %v, want the reported usage", row.InputTokens)
	}
}

func TestPipelineSerializesConcurrentReportApplies(t *testing.T) {
	f := newLiveFixture(t, "success")
	auth := &AuthTick{}
	var (
		wg   sync.WaitGroup
		errs = make(chan error, 2)
		rows []*ledger.Run
	)
	for _, name := range []string{"observer-a", "observer-b"} {
		def := *f.def
		def.Name = name
		def.Manifest = f.def.Manifest
		def.Manifest.Name = name
		def.Manifest.Outputs = agentdef.Outputs{
			File: &agentdef.FileOutput{Dir: name, Max: 1},
		}
		run, ok, err := f.store.Claim(name, f.slot, ledger.TriggerCron)
		if err != nil || !ok {
			t.Fatalf("claim %s = (%t, %v)", name, ok, err)
		}
		rows = append(rows, run)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.pipeline.Run(context.Background(), &def, run, Options{Auth: auth})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range rows {
		got, err := f.store.Get(row.ID)
		if err != nil {
			t.Fatal(err)
		}
		var applied output.Applied
		if err := json.Unmarshal([]byte(got.OutputsApplied.String), &applied); err != nil {
			t.Fatal(err)
		}
		if got.Status != ledger.StatusCompleted || applied.Commit == "" || applied.CommitError != "" {
			t.Fatalf("run %s = status %s, applied %+v", row.ID, got.Status, applied)
		}
	}
	cmd := exec.Command("git", "-C", filepath.Join(f.root, reportsDir), "rev-list", "--count", "HEAD")
	count, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(count)) != "2" {
		t.Fatalf("report commits = %q, %v", count, err)
	}
}

func TestPipelinePreflightFailureSpendsNothingAndUsesFailurePolicy(t *testing.T) {
	f := newLiveFixture(t, "success")
	f.pipeline.Config.Email = config.Email{}
	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{}); err == nil {
		t.Fatal("run succeeded without required email configuration")
	}
	if _, err := os.Stat(f.calls); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("model was called before preflight completed: %v", err)
	}
	row, _ := f.store.Get(run.ID)
	if row.Status != ledger.StatusFailed || row.FailureClass.String != ledger.ClassPreflight ||
		row.CostUSD.Valid || !row.RunDir.Valid {
		t.Fatalf("row = %+v", row)
	}
}

// The sender is set up by hand after the agent is iterated live (§10.1), so a
// run that will not send must not require its account or from address.
func TestPipelineEmailPreflightFollowsNoDeliver(t *testing.T) {
	incomplete := func(f *liveFixture) {
		f.pipeline.Config.Email.AccountID = ""
		f.pipeline.Config.Email.From = ""
	}
	t.Run("no-deliver", func(t *testing.T) {
		f := newLiveFixture(t, "success")
		incomplete(f)
		sender := &recordingSender{}
		f.pipeline.Sender = sender
		run := f.claim(t, ledger.TriggerManual)
		if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{NoDeliver: true}); err != nil {
			t.Fatal(err)
		}
		if calls := readCalls(t, f.calls); calls != 2 {
			t.Fatalf("model calls = %d, want one auth probe and one run", calls)
		}
		row, _ := f.store.Get(run.ID)
		if row.Status != ledger.StatusCompleted || row.DeliveryStatus.String != ledger.DeliverySkipped {
			t.Fatalf("row = %+v", row)
		}
		if len(sender.messages) != 0 {
			t.Fatalf("no-deliver run sent %d messages", len(sender.messages))
		}
	})
	t.Run("delivering", func(t *testing.T) {
		f := newLiveFixture(t, "success")
		incomplete(f)
		run := f.claim(t, ledger.TriggerManual)
		_, err := f.pipeline.Run(context.Background(), f.def, run, Options{})
		var check *CheckError
		if !errors.As(err, &check) || check.Name != "output.email" {
			t.Fatalf("err = %v, want the output.email preflight check", err)
		}
		if _, err := os.Stat(f.calls); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("model was called before preflight completed: %v", err)
		}
		row, _ := f.store.Get(run.ID)
		if row.Status != ledger.StatusFailed || row.FailureClass.String != ledger.ClassPreflight || row.CostUSD.Valid {
			t.Fatalf("row = %+v", row)
		}
	})
}

func TestPipelineFileOnlyRunNeedsNoEmailConfiguration(t *testing.T) {
	f := newLiveFixture(t, "silent")
	f.def.Manifest.Outputs.Email = nil
	f.pipeline.Config.Email = config.Email{}
	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{}); err != nil {
		t.Fatal(err)
	}
	row, _ := f.store.Get(run.ID)
	if row.Status != ledger.StatusCompleted || row.DeliveryStatus.String != ledger.DeliverySkipped || !row.Silent {
		t.Fatalf("row = %+v", row)
	}
}

func TestPipelineClassifiesProviderOutputAndRuntimeFailures(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		class     string
		provider  bool
		wantCalls int
	}{
		{name: "provider", mode: "success", class: ledger.ClassProvider, provider: true, wantCalls: 1},
		{name: "output", mode: "output-missing", class: ledger.ClassOutput, wantCalls: 2},
		{name: "runtime", mode: "died", class: ledger.ClassDied, wantCalls: 2},
		{name: "timeout", mode: "timeout", class: ledger.ClassStuck, wantCalls: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newLiveFixture(t, tt.mode)
			if tt.name == "timeout" {
				f.def.Manifest.Runtime.TimeoutSeconds = 1
			}
			if tt.provider {
				original := registry.Providers["prior-reports"]
				registry.Providers["prior-reports"] = func(context.Context, provider.Env, map[string]any, provider.Window) (json.RawMessage, error) {
					return nil, errors.New("provider fixture failed")
				}
				t.Cleanup(func() { registry.Providers["prior-reports"] = original })
			}
			run := f.claim(t, ledger.TriggerManual)
			if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{NoDeliver: true}); err == nil {
				t.Fatal("run unexpectedly succeeded")
			}
			row, _ := f.store.Get(run.ID)
			if row.Status != ledger.StatusFailed || row.FailureClass.String != tt.class {
				t.Fatalf("row = %+v, want class %s", row, tt.class)
			}
			if calls := readCalls(t, f.calls); calls != tt.wantCalls {
				t.Fatalf("model calls = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}

type recordingSender struct {
	messages []Delivery
	err      error
}

func (s *recordingSender) Send(_ context.Context, message Delivery) error {
	s.messages = append(s.messages, message)
	return s.err
}

func TestRetryDeliveryUsesRetainedOutputWithoutRunningTheModel(t *testing.T) {
	f := newLiveFixture(t, "success")
	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{NoDeliver: true}); err != nil {
		t.Fatal(err)
	}
	before := readCalls(t, f.calls)
	row, _ := f.store.Get(run.ID)
	sender := &recordingSender{}
	f.pipeline.Sender = sender
	// Retry follows the retained run kind, not a selector added to the
	// definition after this standard run completed.
	f.def.Manifest.Observer = &agentdef.Observer{Config: "added-later"}
	if err := f.pipeline.RetryDelivery(context.Background(), f.def, row); err != nil {
		t.Fatal(err)
	}
	if after := readCalls(t, f.calls); after != before {
		t.Fatalf("retry made %d additional model calls", after-before)
	}
	if len(sender.messages) != 1 || sender.messages[0].RunID != run.ID ||
		len(sender.messages[0].Text) == 0 || len(sender.messages[0].HTML) == 0 {
		t.Fatalf("messages = %+v", sender.messages)
	}
	row, _ = f.store.Get(run.ID)
	if row.Status != ledger.StatusCompleted || row.DeliveryStatus.String != ledger.DeliveryDelivered ||
		!row.DeliveredAt.Valid {
		t.Fatalf("row = %+v", row)
	}
}

func TestDeliveryFailureLeavesTheRunCompleted(t *testing.T) {
	f := newLiveFixture(t, "success")
	f.pipeline.Sender = &recordingSender{err: errors.New("sender unavailable")}
	run := f.claim(t, ledger.TriggerManual)
	if _, err := f.pipeline.Run(context.Background(), f.def, run, Options{}); err == nil {
		t.Fatal("delivery failure was not returned")
	}
	row, _ := f.store.Get(run.ID)
	if row.Status != ledger.StatusCompleted || row.DeliveryStatus.String != ledger.DeliveryFailed ||
		!strings.Contains(row.DeliveryError.String, "sender unavailable") {
		t.Fatalf("row = %+v", row)
	}
	data, err := os.ReadFile(filepath.Join(row.RunDir.String, "applied.json"))
	if err != nil || !strings.Contains(string(data), "sender unavailable") {
		t.Fatalf("applied.json = %q, %v", data, err)
	}
}

func readCalls(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(data), "\n")
}
