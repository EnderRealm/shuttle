package agentdef

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/robfig/cron/v3"
)

const (
	promptFile   = "AGENT.md"
	manifestFile = "agent.json"
)

// cronParser accepts exactly the five standard fields. cron.ParseStandard is
// deliberately not used: it also accepts @daily-style descriptors, which the
// §4.1 schema does not allow.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// ParseCron parses a trigger expression. Exported so the scheduler fires on
// exactly the grammar validation accepts, rather than a second parser that
// could drift from this one.
func ParseCron(expression string) (cron.Schedule, error) {
	return cronParser.Parse(expression)
}

// Manifest is agent.json, the §4.1 schema. Unknown keys refuse the load, so
// every key the harness honours is declared here.
type Manifest struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Runtime     Runtime   `json:"runtime"`
	Triggers    []Trigger `json:"triggers"`
	Inputs      []Input   `json:"inputs"`
	Outputs     Outputs   `json:"outputs"`
	Sandbox     Sandbox   `json:"sandbox"`
	Observer    *Observer `json:"observer,omitempty"`
}

type Runtime struct {
	Kind           string   `json:"kind"`
	Auth           string   `json:"auth"`
	Model          string   `json:"model"`
	MaxTurns       int      `json:"max_turns"`
	MaxBudgetUSD   float64  `json:"max_budget_usd"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	Tools          []string `json:"tools"`
}

type Trigger struct {
	Kind       string `json:"kind"`
	Expression string `json:"expression"`
	Timezone   string `json:"timezone"`
}

// Input args are per-provider; the provider owns their schema, so they stay
// untyped here.
type Input struct {
	Provider string         `json:"provider"`
	Args     map[string]any `json:"args"`
}

type Outputs struct {
	Email *EmailOutput `json:"email"`
	File  *FileOutput  `json:"file"`
}

type EmailOutput struct {
	Max int `json:"max"`
}

type FileOutput struct {
	Dir string `json:"dir"`
	Max int    `json:"max"`
}

type Sandbox struct {
	Kind           string   `json:"kind"`
	AllowedDomains []string `json:"allowed_domains"`
}

// Observer selects one operator-configured domain observer. Executable paths
// stay in config.json; an agent definition can select a registration but
// cannot introduce a command for the harness to execute.
type Observer struct {
	Config string `json:"config"`
}

// Definition is one agent directory: its manifest and its prompt body.
type Definition struct {
	Dir      string
	Name     string
	Prompt   string
	Manifest Manifest
}

// Load reads and validates the agent definition in dir. Every error names the
// directory and the offending field.
func Load(dir string) (*Definition, error) {
	prompt, err := os.ReadFile(filepath.Join(dir, promptFile))
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", dir, promptFile, err)
	}
	if !utf8.Valid(prompt) {
		return nil, fmt.Errorf("%s: %s: not valid UTF-8", dir, promptFile)
	}
	if strings.TrimSpace(string(prompt)) == "" {
		return nil, fmt.Errorf("%s: %s: empty", dir, promptFile)
	}

	data, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", dir, manifestFile, err)
	}
	// Reserved output kinds are named as reserved rather than surfacing as a
	// plain unknown key from the strict decode below. A malformed manifest
	// falls through so the decoder reports the syntax error.
	var probe struct {
		Outputs map[string]json.RawMessage `json:"outputs"`
	}
	if json.Unmarshal(data, &probe) == nil {
		for _, k := range reservedOutputs {
			if _, ok := probe.Outputs[k]; ok {
				return nil, fmt.Errorf("%s: %s: outputs.%s: reserved for a future phase", dir, manifestFile, k)
			}
		}
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%s: %s: %w", dir, manifestFile, err)
	}
	// Decode reads one JSON value; anything after it would otherwise slip past
	// the strict decode above.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %s: trailing content after the manifest object", dir, manifestFile)
	}
	if err := m.validate(filepath.Base(filepath.Clean(dir))); err != nil {
		return nil, fmt.Errorf("%s: %s: %w", dir, manifestFile, err)
	}

	return &Definition{Dir: dir, Name: m.Name, Prompt: string(prompt), Manifest: m}, nil
}

// LoadAll loads every agent subdirectory of agentsDir, sorted by name and
// skipping dot entries. Definitions that load are returned alongside a joined
// error carrying one entry per agent that failed, so a caller can report the
// failures and still list the rest.
func LoadAll(agentsDir string) ([]*Definition, error) {
	entries, err := os.ReadDir(agentsDir)
	if err != nil {
		return nil, fmt.Errorf("agents dir %s: %w", agentsDir, err)
	}
	var (
		defs []*Definition
		errs []error
	)
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		def, err := Load(filepath.Join(agentsDir, e.Name()))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		defs = append(defs, def)
	}
	return defs, errors.Join(errs...)
}

// NextSlot returns the earliest slot strictly after the given time across the
// definition's triggers. Each cron expression is literal wall clock in its own
// timezone (§5); jitter belongs to the scheduler, not here.
func (d *Definition) NextSlot(after time.Time) (time.Time, error) {
	var next time.Time
	for i, t := range d.Manifest.Triggers {
		loc, err := time.LoadLocation(t.Timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s: triggers[%d].timezone: %w", d.Dir, i, err)
		}
		sched, err := cronParser.Parse(t.Expression)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s: triggers[%d].expression: %w", d.Dir, i, err)
		}
		slot := sched.Next(after.In(loc))
		if next.IsZero() || slot.Before(next) {
			next = slot
		}
	}
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("%s: triggers: none", d.Dir)
	}
	return next, nil
}
