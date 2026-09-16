package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
)

func preflightFixture(t *testing.T) (Preflight, *agentdef.Definition, string, string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "pf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	calls := filepath.Join(root, "calls")
	claude := filepath.Join(root, "claude")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = --version ]; then exit 0; fi
printf 'model\n' >> '%s'
cat >/dev/null
printf '%%s\n' '{"type":"result","subtype":"success","total_cost_usd":0.001}'
`, calls)
	if err := os.WriteFile(claude, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	srt := filepath.Join(root, "srt")
	if err := os.WriteFile(srt, []byte(`#!/bin/sh
shift 2
if [ "$1" = -c ]; then exit 0; fi
shift
exec "$@"
`), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "reports"), 0700); err != nil {
		t.Fatal(err)
	}
	p := Preflight{Home: root, HomeDir: root, PATH: "/usr/bin:/bin", Config: &config.Config{
		Paths: config.Paths{ClaudeBin: claude, SRTBin: srt}, Anthropic: config.Anthropic{KeychainItem: "model"},
	}, findKey: func(item string) (string, error) {
		if item != "model" && item != "mail" {
			return "", fmt.Errorf("missing keychain item %q", item)
		}
		return "fixture-secret", nil
	}}
	def := &agentdef.Definition{Name: "observer", Manifest: agentdef.Manifest{
		Runtime: agentdef.Runtime{Kind: "claude", Auth: "api_key", Model: "haiku", MaxTurns: 1, MaxBudgetUSD: 0.05, TimeoutSeconds: 60, Tools: []string{"Read"}},
		Sandbox: agentdef.Sandbox{Kind: "srt"},
		Inputs:  []agentdef.Input{{Provider: "prior-reports", Args: map[string]any{"agent": "observer"}}},
		Outputs: agentdef.Outputs{File: &agentdef.FileOutput{Dir: "observer", Max: 1}},
	}}
	return p, def, root, calls
}

func TestPreflightFollowsDeclaredRequirementsBeforeSpend(t *testing.T) {
	ctx := context.Background()
	p, def, dir, calls := preflightFixture(t)
	names, err := p.Check(ctx, def, dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(names, ","), "email") {
		t.Fatalf("unrelated checks: %v", names)
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatalf("local checks called model: %v", err)
	}
	tick := &AuthTick{}
	for i := 0; i < 2; i++ {
		if _, err := tick.Check(ctx, p, def, dir); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(calls)
	if err != nil || string(data) != "model\n" {
		t.Fatalf("calls %q, %v", data, err)
	}
	if _, err := (&AuthTick{}).Check(ctx, p, def, dir); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(calls)
	if string(data) != "model\nmodel\n" {
		t.Fatalf("new tick did not recheck: %q", data)
	}
}

func TestPreflightMissingRequirementsNeverProbeAuth(t *testing.T) {
	tests := []struct {
		name   string
		change func(Preflight, *agentdef.Definition)
	}{
		{"claude", func(p Preflight, d *agentdef.Definition) { p.Config.Paths.ClaudeBin = "/missing/claude" }},
		{"srt", func(p Preflight, d *agentdef.Definition) { p.Config.Paths.SRTBin = "/missing/srt" }},
		{"runtime", func(p Preflight, d *agentdef.Definition) { d.Manifest.Runtime.Auth = "subscription" }},
		{"anthropic.keychain_item", func(p Preflight, d *agentdef.Definition) { p.Config.Anthropic.KeychainItem = "missing" }},
		{"input.loom-sessions", func(p Preflight, d *agentdef.Definition) {
			d.Manifest.Inputs = []agentdef.Input{{Provider: "loom-sessions"}}
		}},
		{"input.loom-workreport", func(p Preflight, d *agentdef.Definition) {
			d.Manifest.Inputs = []agentdef.Input{{Provider: "loom-workreport"}}
		}},
		{"input.tk-journal", func(p Preflight, d *agentdef.Definition) {
			d.Manifest.Inputs = []agentdef.Input{{Provider: "tk-journal"}}
		}},
		{"input.tk-tickets", func(p Preflight, d *agentdef.Definition) {
			d.Manifest.Inputs = []agentdef.Input{{Provider: "tk-tickets"}}
		}},
		{"input.git-log", func(p Preflight, d *agentdef.Definition) { d.Manifest.Inputs = []agentdef.Input{{Provider: "git-log"}} }},
		{"input.prior-reports", func(p Preflight, d *agentdef.Definition) { d.Manifest.Inputs[0].Args["agent"] = "../private" }},
		{"output.email", func(p Preflight, d *agentdef.Definition) { d.Manifest.Outputs.Email = &agentdef.EmailOutput{Max: 1} }},
		{"output.file", func(p Preflight, d *agentdef.Definition) { os.Remove(filepath.Join(p.Home, "reports")) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, d, dir, calls := preflightFixture(t)
			tt.change(p, d)
			_, err := (&AuthTick{}).Check(context.Background(), p, d, dir)
			var check *CheckError
			if !errors.As(err, &check) || check.Name != tt.name {
				t.Fatalf("error %v, want %s", err, tt.name)
			}
			if _, err := os.Stat(calls); !os.IsNotExist(err) {
				t.Fatalf("model spent before %s: %v", tt.name, err)
			}
		})
	}
}

func TestUnsafeRunDoesNotRequireOrLaunchSRT(t *testing.T) {
	p, def, dir, calls := preflightFixture(t)
	if err := os.Remove(p.Config.Paths.SRTBin); err != nil {
		t.Fatal(err)
	}
	p.UnsafeNoSandbox = true
	if _, err := (&AuthTick{}).Check(context.Background(), p, def, dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(calls)
	if err != nil || string(data) != "model\n" {
		t.Fatalf("direct auth probe calls = %q, %v", data, err)
	}
}

func TestDailyPreflightSchemaConfigAndMail(t *testing.T) {
	p, d, dir, _ := preflightFixture(t)
	p.Config.Paths.LoomHome = dir
	db, err := sql.Open("sqlite", filepath.Join(dir, "summaries.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT); INSERT INTO schema_meta VALUES('schema_version','4')`); err != nil {
		t.Fatal(err)
	}
	p.Config.Paths.LoomBin = p.Config.Paths.ClaudeBin
	p.Config.Paths.TKConfig = filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p.Config.Paths.TKConfig, []byte("central_root: "+dir+"\nprojects: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	d.Manifest.Inputs = nil
	for _, name := range []string{"loom-sessions", "loom-workreport", "tk-journal", "tk-tickets", "git-log"} {
		d.Manifest.Inputs = append(d.Manifest.Inputs, agentdef.Input{Provider: name})
	}
	d.Manifest.Outputs.Email = &agentdef.EmailOutput{Max: 1}
	p.Config.Email = config.Email{Provider: "cloudflare", AccountID: "account", From: "from@example.test", To: []string{"to@example.test"}, KeychainItem: "mail"}
	if _, err := p.Check(context.Background(), d, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE schema_meta SET value='5'`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Check(context.Background(), d, dir); err == nil || !strings.Contains(err.Error(), "input.loom-sessions") {
		t.Fatalf("schema guard: %v", err)
	}
	if _, err := db.Exec(`UPDATE schema_meta SET value='4'`); err != nil {
		t.Fatal(err)
	}
	p.Config.Email.KeychainItem = "missing-mail"
	if _, err := p.Check(context.Background(), d, dir); err == nil || !strings.Contains(err.Error(), "output.email") {
		t.Fatalf("mail check: %v", err)
	}
	d.Manifest.Outputs.Email.Max = 0
	if _, err := p.Check(context.Background(), d, dir); err != nil {
		t.Fatal(err)
	}
}

func TestAuthRejectionUsesExistingPausePolicy(t *testing.T) {
	p, d, dir, _ := preflightFixture(t)
	if err := os.WriteFile(p.Config.Paths.ClaudeBin, []byte(`#!/bin/sh
if [ "$1" = --version ]; then exit 0; fi
cat >/dev/null
printf '%s\n' '{"type":"system","subtype":"api_retry","error":"authentication_failed"}'
exit 1
`), 0700); err != nil {
		t.Fatal(err)
	}
	_, err := (&AuthTick{}).Check(context.Background(), p, d, dir)
	var check *CheckError
	if !errors.As(err, &check) || check.Class != ledger.ClassAuth {
		t.Fatalf("auth: %v", err)
	}
	store, err := ledger.Open(filepath.Join(dir, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, ok, err := store.Claim(d.Name, time.Now(), ledger.TriggerManual)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Fail(run.ID, check.Class, check.Error(), ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	agent, err := store.Agent(d.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !agent.Paused || agent.PausedReason.String != ledger.PauseAuth || agent.FailureStreak != 1 {
		t.Fatalf("agent: %+v", agent)
	}
}

func TestWorkreportAloneRequiresDatabaseBeforeAuth(t *testing.T) {
	p, d, dir, calls := preflightFixture(t)
	p.Config.Paths.LoomHome = dir
	p.Config.Paths.LoomBin = p.Config.Paths.ClaudeBin
	d.Manifest.Inputs = []agentdef.Input{{Provider: "loom-workreport"}}
	_, err := (&AuthTick{}).Check(context.Background(), p, d, dir)
	var check *CheckError
	if !errors.As(err, &check) || check.Name != "input.loom-workreport" {
		t.Fatalf("missing database: %v", err)
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatalf("model call before database check: %v", err)
	}
}

func TestAuthSetupFailureDoesNotPoisonAnotherAgent(t *testing.T) {
	p, d, root, calls := preflightFixture(t)
	first := filepath.Join(root, "first")
	if err := os.MkdirAll(filepath.Join(first, "auth"), 0700); err != nil {
		t.Fatal(err)
	}
	// Block the retained transcript in either layout without touching runtime
	// credentials or the second agent's directory.
	for _, file := range []string{filepath.Join(first, "auth", "transcript.ndjson"), filepath.Join(first, "auth-transcript.ndjson")} {
		if err := os.Mkdir(file, 0700); err != nil {
			t.Fatal(err)
		}
	}
	tick := &AuthTick{}
	if _, err := tick.Check(context.Background(), p, d, first); err == nil {
		t.Fatal("local setup unexpectedly succeeded")
	}
	second := *d
	second.Name = "second"
	if _, err := tick.Check(context.Background(), p, &second, root); err != nil {
		t.Fatalf("valid agent inherited local failure: %v", err)
	}
	data, err := os.ReadFile(calls)
	if err != nil || string(data) != "model\n" {
		t.Fatalf("second agent did not probe: %q/%v", data, err)
	}
}

func TestAuthFitsRunDirectoryAcceptedByPreflight(t *testing.T) {
	p, d, root, _ := preflightFixture(t)
	dir := filepath.Join(root, strings.Repeat("x", 77-len(root)-1))
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Check(context.Background(), d, dir); err != nil {
		t.Fatalf("local check: %v", err)
	}
	if _, err := (&AuthTick{}).Check(context.Background(), p, d, dir); err != nil {
		t.Fatalf("auth rejected an accepted run directory: %v", err)
	}
}
