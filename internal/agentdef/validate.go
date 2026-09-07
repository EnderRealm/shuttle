package agentdef

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	runtimeKindClaude = "claude"
	authAPIKey        = "api_key"
	authSubscription  = "subscription"
	triggerKindCron   = "cron"
	sandboxKindSRT    = "srt"
	maxTimeoutSeconds = 3600
)

var (
	// NameRE is the §4.1 agent-name rule. Exported because an agent name is
	// joined to a path elsewhere — prior-reports reads <reports_dir>/<agent> —
	// and that join is only safe against the same rule.
	NameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

	allowedTools = []string{"Read", "Glob", "Grep", "Write"}

	// AllowedProviders is the registered set (§8). Exported so the provider
	// registry is held against exactly the names a manifest may ask for.
	AllowedProviders = []string{"loom-sessions", "loom-workreport", "tk-journal", "tk-tickets", "git-log", "prior-reports"}

	// Reserved for a future phase (§16): rejected by name so the error says
	// "not yet" rather than "unknown".
	reservedRuntimeKinds = []string{"codex", "ollama", "script"}
	reservedTriggerKinds = []string{"event", "fire"}
	reservedOutputs      = []string{"ticket_create", "ticket_note", "memory"}
)

// validate applies the §4.1 rules. dirName is the agent directory's base name,
// which the manifest's name must equal.
func (m *Manifest) validate(dirName string) error {
	if !NameRE.MatchString(m.Name) {
		return fmt.Errorf("name: %q must match %s", m.Name, NameRE)
	}
	if m.Name != dirName {
		return fmt.Errorf("name: %q does not match directory %q", m.Name, dirName)
	}
	if err := m.Runtime.validate(); err != nil {
		return err
	}
	if len(m.Triggers) == 0 {
		return errors.New("triggers: at least one trigger is required")
	}
	for i, t := range m.Triggers {
		if err := t.validate(i); err != nil {
			return err
		}
	}
	for i, in := range m.Inputs {
		if !slices.Contains(AllowedProviders, in.Provider) {
			return fmt.Errorf("inputs[%d].provider: %q is not a registered provider (registered: %s)",
				i, in.Provider, strings.Join(AllowedProviders, ", "))
		}
	}
	if err := m.Outputs.validate(); err != nil {
		return err
	}
	return m.Sandbox.validate()
}

func (r *Runtime) validate() error {
	if slices.Contains(reservedRuntimeKinds, r.Kind) {
		return fmt.Errorf("runtime.kind: %q is reserved for a future phase", r.Kind)
	}
	if r.Kind != runtimeKindClaude {
		return fmt.Errorf("runtime.kind: must be %q, got %q", runtimeKindClaude, r.Kind)
	}
	if r.Auth != authAPIKey && r.Auth != authSubscription {
		return fmt.Errorf("runtime.auth: must be %q or %q, got %q", authAPIKey, authSubscription, r.Auth)
	}
	if r.Model == "" {
		return errors.New("runtime.model: required")
	}
	if r.MaxTurns <= 0 {
		return fmt.Errorf("runtime.max_turns: must be positive, got %d", r.MaxTurns)
	}
	if r.MaxBudgetUSD <= 0 {
		return fmt.Errorf("runtime.max_budget_usd: required and must be positive, got %v", r.MaxBudgetUSD)
	}
	if r.TimeoutSeconds <= 0 || r.TimeoutSeconds > maxTimeoutSeconds {
		return fmt.Errorf("runtime.timeout_seconds: required, must be positive and at most %d, got %d", maxTimeoutSeconds, r.TimeoutSeconds)
	}
	if len(r.Tools) == 0 {
		return errors.New("runtime.tools: at least one tool is required")
	}
	for _, t := range r.Tools {
		if !slices.Contains(allowedTools, t) {
			return fmt.Errorf("runtime.tools: %q is not allowed (allowed: %s)", t, strings.Join(allowedTools, ", "))
		}
	}
	return nil
}

func (t *Trigger) validate(i int) error {
	if slices.Contains(reservedTriggerKinds, t.Kind) {
		return fmt.Errorf("triggers[%d].kind: %q is reserved for a future phase", i, t.Kind)
	}
	if t.Kind != triggerKindCron {
		return fmt.Errorf("triggers[%d].kind: must be %q, got %q", i, triggerKindCron, t.Kind)
	}
	// robfig/cron strips a TZ=/CRON_TZ= prefix before counting fields, so such
	// a prefix would smuggle a zone past triggers[].timezone, and one with no
	// space after it panics inside the library. Count the fields first.
	if len(strings.Fields(t.Expression)) != 5 {
		return fmt.Errorf("triggers[%d].expression: %q is not a five-field cron expression", i, t.Expression)
	}
	if _, err := cronParser.Parse(t.Expression); err != nil {
		return fmt.Errorf("triggers[%d].expression: %q is not a five-field cron expression: %w", i, t.Expression, err)
	}
	// LoadLocation accepts "Local", which is not an IANA name and would make
	// the slot depend on the daemon's host zone.
	if t.Timezone == "" || t.Timezone == "Local" {
		return fmt.Errorf("triggers[%d].timezone: %q is not an IANA timezone name", i, t.Timezone)
	}
	if _, err := time.LoadLocation(t.Timezone); err != nil {
		return fmt.Errorf("triggers[%d].timezone: %q is not an IANA timezone name: %w", i, t.Timezone, err)
	}
	return nil
}

func (o *Outputs) validate() error {
	if o.Email != nil && o.Email.Max != 0 && o.Email.Max != 1 {
		return fmt.Errorf("outputs.email.max: must be 0 or 1, got %d", o.Email.Max)
	}
	if o.File != nil {
		dir := o.File.Dir
		if dir == "" || dir == "." || dir == ".." || dir != filepath.Base(dir) {
			return fmt.Errorf("outputs.file.dir: %q must be a single path segment", dir)
		}
	}
	return nil
}

func (s *Sandbox) validate() error {
	// No "none": running unsandboxed is `shuttle run --unsafe-no-sandbox`,
	// never something a manifest can assert.
	if s.Kind != sandboxKindSRT {
		return fmt.Errorf("sandbox.kind: must be %q, got %q", sandboxKindSRT, s.Kind)
	}
	return nil
}
