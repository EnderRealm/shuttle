package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
	"shuttle/internal/runtime/claude"
	"shuttle/internal/sandbox"
)

// AuthTick is scoped to a single daemon tick. Construct one for each tick and
// share it across that tick's claimed runs; no claim means no authentication
// call. Manual runs use their own instance. Local checks always precede spend.
type AuthTick struct {
	mu      sync.Mutex
	results map[string]authResult
}

type authResult struct {
	err error
}

// Check checks the agent's requirements, then probes each runtime credential
// once in this tick. A rejected credential is an auth-class failure so the
// caller's normal failure policy pauses every affected agent immediately.
func (t *AuthTick) Check(ctx context.Context, p Preflight, def *agentdef.Definition, runDir string) ([]string, error) {
	names, err := p.Check(ctx, def, runDir)
	if err != nil {
		return names, err
	}
	names = append(names, "auth")
	t.mu.Lock()
	defer t.mu.Unlock()
	key := p.Config.Paths.ClaudeBin + "\x00" + p.Config.Anthropic.KeychainItem
	if result, ok := t.results[key]; ok {
		return names, result.err
	}
	probed, err := p.auth(ctx, def, runDir)
	if probed {
		if t.results == nil {
			t.results = map[string]authResult{}
		}
		t.results[key] = authResult{err: err}
	}
	return names, err
}

// The bool reports a shared probe outcome. Setup and evidence-write failures
// belong to this run directory and must not poison another agent's credential.
func (p Preflight) auth(ctx context.Context, def *agentdef.Definition, runDir string) (bool, error) {
	fail := func(class string, err error) (bool, error) {
		return false, &CheckError{Name: "auth", Class: class, Err: err}
	}
	key, err := p.key(p.Config.Anthropic.KeychainItem)
	if err != nil {
		return fail(ledger.ClassPreflight, err)
	}
	// Reuse the checked run directory: a nested directory can exceed srt's
	// socket limit even when the local preflight accepted its parent.
	dir := runDir
	var launcher []string
	if !p.UnsafeNoSandbox {
		so := p.sandbox(def, dir)
		if _, err := sandbox.WriteProfile(so); err != nil {
			return fail(ledger.ClassPreflight, err)
		}
		launcher, err = sandbox.Launcher(so)
		if err != nil {
			return fail(ledger.ClassPreflight, err)
		}
	}
	transcript, err := os.OpenFile(filepath.Join(dir, "auth-transcript.ndjson"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fail(ledger.ClassPreflight, err)
	}
	defer transcript.Close()
	stderr, err := os.OpenFile(filepath.Join(dir, "auth-stderr.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fail(ledger.ClassPreflight, err)
	}
	defer stderr.Close()
	o, err := claude.Run(ctx, claude.Options{
		Bin: p.Config.Paths.ClaudeBin, Launcher: launcher, RunDir: dir,
		Auth: claude.AuthAPIKey, Model: "haiku", MaxTurns: 1, MaxBudgetUSD: 0.05,
		Tools: []string{"Read"}, TimeoutSeconds: 60, APIKey: key, Prompt: "ping",
		PATH: p.PATH, HomeDir: p.HomeDir, Timezone: "UTC",
	}, transcript, stderr)
	if err != nil {
		return fail(ledger.ClassPreflight, err)
	}
	if o.TranscriptErr != nil {
		return fail(ledger.ClassPreflight, o.TranscriptErr)
	}
	if o.StderrErr != nil {
		return fail(ledger.ClassPreflight, o.StderrErr)
	}
	if o.Class != "" {
		class := ledger.ClassPreflight
		if o.Class == ledger.ClassAuth {
			class = ledger.ClassAuth
		}
		return ctx.Err() == nil, &CheckError{Name: "auth", Class: class, Err: fmt.Errorf("%s: %s", o.Class, o.Detail)}
	}
	return true, nil
}
