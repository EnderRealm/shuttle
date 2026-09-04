package claude

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// AuthAPIKey is the only runtime.auth phase 1 implements (§7.4). subscription
// needs a different flag set entirely, so it is refused rather than run as if
// it were this one.
const AuthAPIKey = "api_key"

// minBudgetUSD is the smallest cap the argv can carry: the budget is rendered
// to two decimals (§7.4), so anything under a cent would reach the CLI as
// --max-budget-usd 0.00 and stop the run before its first turn.
const minBudgetUSD = 0.01

// denyTools is the fixed deny list of §7.4: the tools no phase 1 agent may
// reach, whatever its manifest asks for.
var denyTools = []string{"Bash", "WebFetch", "WebSearch", "Agent", "Task"}

// Options is one model launch. Every path is absolute and every environment
// value is passed in: nothing here reads the ambient process environment,
// because a daemon-launched run must not inherit a login session (§7.4).
type Options struct {
	Bin            string   // absolute path to the claude binary (config paths.claude_bin)
	Launcher       []string // optional prefix the argv is launched through, e.g. the §11 srt invocation; empty is a direct launch (§12's --unsafe-no-sandbox)
	RunDir         string   // the run directory (§7.2), absolute
	Auth           string   // agentdef Runtime.Auth
	Model          string
	MaxTurns       int
	MaxBudgetUSD   float64
	Tools          []string
	TimeoutSeconds int
	APIKey         string
	Prompt         string // the rendered prompt.md text
	PATH           string // the baked PATH for the child
	HomeDir        string // HOME for the child
	Timezone       string // TZ for the child
}

// The run-directory subpaths this package owns (§7.2). The argv names the
// settings file and the environment names the other three, so they are
// derived in one place rather than joined at each use.
func (o Options) SettingsPath() string { return filepath.Join(o.RunDir, "settings.json") }
func (o Options) ConfigDir() string    { return filepath.Join(o.RunDir, "claude-home") }
func (o Options) TmpDir() string       { return filepath.Join(o.RunDir, "tmp") }
func (o Options) OutDir() string       { return filepath.Join(o.RunDir, "out") }

// Argv is the §7.4 command line, in the order it is written to run.sh, behind
// Launcher when one is set.
func Argv(o Options) ([]string, error) {
	if err := requireLaunchable(o); err != nil {
		return nil, err
	}
	return append(append([]string{}, o.Launcher...),
		o.Bin,
		"--bare", "-p",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		"--model", o.Model,
		"--max-turns", strconv.Itoa(o.MaxTurns),
		"--max-budget-usd", strconv.FormatFloat(o.MaxBudgetUSD, 'f', 2, 64),
		"--permission-mode", "dontAsk",
		"--tools", strings.Join(o.Tools, ","),
		"--settings", o.SettingsPath(),
		"--no-session-persistence",
	), nil
}

// Environ is the child's whole environment, built from scratch (§7.4): these
// eight variables and nothing else. weft inherits the login session and strips
// three git variables; a scheduled run has no reason to carry SSH_AUTH_SOCK or
// anything else from a login shell into a model run.
func Environ(o Options) ([]string, error) {
	if err := requireLaunchable(o); err != nil {
		return nil, err
	}
	return []string{
		"PATH=" + o.PATH,
		"HOME=" + o.HomeDir,
		"TMPDIR=" + o.TmpDir(),
		// Every file the CLI writes (session files, todos, debug, statsig
		// cache) lands under the run directory, and therefore inside what the
		// sandbox allows (§11).
		"CLAUDE_CONFIG_DIR=" + o.ConfigDir(),
		"ANTHROPIC_API_KEY=" + o.APIKey,
		"DISABLE_TELEMETRY=1",
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1",
		"TZ=" + o.Timezone,
	}, nil
}

// settings is the §7.4 permission document that makes --permission-mode
// dontAsk mean "only this".
type settings struct {
	Permissions permissions `json:"permissions"`
	// The daemon's own hooks would otherwise run inside the model's turns;
	// --bare already reads no user settings, so this is belt for the case
	// where a settings file is honoured anyway.
	DisableAllHooks bool `json:"disableAllHooks"`
}

type permissions struct {
	Allow []string `json:"allow"`
	Deny  []string `json:"deny"`
}

// SettingsJSON renders the document the argv points --settings at. The allow
// list is derived from the manifest's tools rather than hardcoded, so a tool
// the agent does not ask for gets no rule; a tool with no rule to derive is
// refused by requireLaunchable, before any of the three entry points emits it.
func SettingsJSON(o Options) ([]byte, error) {
	// Guarded like the other two entry points, and for a sharper reason: this
	// is the document that makes --permission-mode dontAsk mean "only this",
	// and an empty RunDir would render its allow rules as Read(/**).
	if err := requireLaunchable(o); err != nil {
		return nil, err
	}
	s := settings{Permissions: permissions{Deny: denyTools}, DisableAllHooks: true}
	for _, tool := range o.Tools {
		// requireLaunchable refused every tool without a rule above, so the
		// allow list is complete or the launch never happened.
		rule, _ := toolRule(o, tool)
		s.Permissions.Allow = append(s.Permissions.Allow, rule)
	}
	return json.MarshalIndent(s, "", "  ")
}

// toolRule is the settings.json allow rule for one manifest tool, and with it
// the set of tools phase 1 knows: a tool with no rule is one --tools would
// name with nothing allowing it, which fails mid-run rather than at launch.
func toolRule(o Options, tool string) (string, bool) {
	switch tool {
	case "Read", "Glob", "Grep":
		return fmt.Sprintf("%s(%s/**)", tool, o.RunDir), true
	case "Write":
		// out/ only: it is the one directory the model may write (§7.2).
		return fmt.Sprintf("Write(%s/**)", o.OutDir()), true
	}
	return "", false
}

// requireLaunchable is the contract the argv, the environment and the settings
// document are built against: phase 1 speaks api_key auth only, the two paths
// every other path is derived from must be absolute — a relative or empty
// RunDir would put the run's directories, and the child's cwd, wherever the
// daemon happens to be running — and every value the argv or the environment
// carries must be usable, since this package reads nothing ambient to fill a
// gap.
func requireLaunchable(o Options) error {
	if o.Auth != AuthAPIKey {
		return fmt.Errorf("runtime.auth %q: subscription auth (§7.4) is not implemented in phase 1", o.Auth)
	}
	if !filepath.IsAbs(o.Bin) {
		return fmt.Errorf("bin %q is not an absolute path", o.Bin)
	}
	if !filepath.IsAbs(o.RunDir) {
		return fmt.Errorf("run dir %q is not an absolute path", o.RunDir)
	}
	// The launcher is exec'd as argv[0], so a relative one would resolve
	// against the daemon's own PATH rather than the path the caller named.
	if len(o.Launcher) > 0 && !filepath.IsAbs(o.Launcher[0]) {
		return fmt.Errorf("launcher %q is not an absolute path", o.Launcher[0])
	}
	if o.Model == "" {
		return fmt.Errorf("model is empty: the argv interpolates it")
	}
	if len(o.Tools) == 0 {
		return fmt.Errorf("tools is empty: --tools with no value allows the model nothing")
	}
	// Checked here rather than in SettingsJSON alone, so the three entry points
	// agree on what a valid Options is: an argv naming a tool the permission
	// document cannot express is not a launchable one.
	for _, tool := range o.Tools {
		if _, ok := toolRule(o, tool); !ok {
			return fmt.Errorf("tools: %q has no permission rule in phase 1", tool)
		}
	}
	if o.MaxTurns <= 0 {
		return fmt.Errorf("max_turns %d: a run must carry a positive turn cap", o.MaxTurns)
	}
	if o.MaxBudgetUSD < minBudgetUSD {
		return fmt.Errorf("max_budget_usd %v is below the %v the two-decimal argv can express", o.MaxBudgetUSD, minBudgetUSD)
	}
	// The child's environment is built, not inherited, so an empty value here
	// reaches it as empty rather than falling back to the daemon's own. An
	// empty key in particular buys a full launch that ends in the same §7.6
	// auth class a revoked key produces, which §6.3 auto-pauses the agent on.
	if o.APIKey == "" {
		return fmt.Errorf("api key is empty: the run would launch and fail as if the credential were rejected")
	}
	if o.PATH == "" {
		return fmt.Errorf("path is empty: the child inherits nothing to fall back on")
	}
	if o.HomeDir == "" {
		return fmt.Errorf("home dir is empty: the child inherits nothing to fall back on")
	}
	if o.Timezone == "" {
		return fmt.Errorf("timezone is empty: the child inherits nothing to fall back on")
	}
	return nil
}
