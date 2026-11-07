package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"shuttle/internal/keychain"
	"shuttle/internal/sandbox"
)

const (
	// EnvHome overrides the state root; EnvAgentsDir overrides the agent
	// definitions directory. Both are carried in the launchd plist (§13).
	EnvHome      = "SHUTTLE_HOME"
	EnvAgentsDir = "SHUTTLE_AGENTS_DIR"

	configFile = "config.json"
	ledgerFile = "runs.db"
	reportsDir = "reports"

	// HostedSharedRoot is SHARED_ROOT in scripts/agent-users/common.sh, the
	// root of the two trees setup.sh makes agent-writable.
	HostedSharedRoot = "/Users/Shared/weft"
	// HostedWorktreeRoot is SHARED_ROOT/worktrees, where every hosted run's
	// worktree lives: the --worktree-root a segment's worker is launched with.
	HostedWorktreeRoot = HostedSharedRoot + "/" + sandbox.WorktreesDir
	// hostedRepositoriesRoot is SHARED_ROOT/repos, where every hosted checkout
	// lives.
	hostedRepositoriesRoot = HostedSharedRoot + "/repos"
)

// Config is config.json, the §4.2 shape. Credentials are Keychain item names
// only; no secret value is ever held here or in the file.
type Config struct {
	AgentsDir string              `json:"agents_dir"`
	Timezone  string              `json:"timezone"`
	Email     Email               `json:"email"`
	Anthropic Anthropic           `json:"anthropic"`
	Paths     Paths               `json:"paths"`
	Notify    Notify              `json:"notify"`
	Limits    Limits              `json:"limits"`
	Output    Output              `json:"output"`
	Weft      Weft                `json:"weft"`
	Observers map[string]Observer `json:"observers,omitempty"`
}

type Email struct {
	Provider     string   `json:"provider"`
	AccountID    string   `json:"account_id"`
	From         string   `json:"from"`
	To           []string `json:"to"`
	KeychainItem string   `json:"keychain_item"`
}

type Anthropic struct {
	KeychainItem string `json:"keychain_item"`
}

// Paths are absolute on purpose: a launchd job inherits nothing from a login
// shell, and a PATH lookup that finds a different binary is worse than a
// missing-file error.
type Paths struct {
	LoomHome  string `json:"loom_home"`
	TKConfig  string `json:"tk_config"`
	ClaudeBin string `json:"claude_bin"`
	SRTBin    string `json:"srt_bin"`
	LoomBin   string `json:"loom_bin"`
}

type Notify struct {
	Enabled         bool `json:"enabled"`
	CooldownMinutes int  `json:"cooldown_minutes"`
}

type Limits struct {
	DailyRunsPerAgent int     `json:"daily_runs_per_agent"`
	DailySpendUSD     float64 `json:"daily_spend_usd"`
}

// Output is §9.1's configured domain list: hosts a report may link to on top
// of github.com and the tk store's remote. Optional; an absent key allows
// neither more nor less than those two.
type Output struct {
	LinkHosts []string `json:"link_hosts"`
}

// Weft configures the private weft service hosted by the daemon:
// weft-service/v2, with weft-remote/v1's read routes kept (weft-service/v2 §14.2).
// An empty Listen disables the service, which keeps phase-1 installations
// unchanged until the block is configured.
type Weft struct {
	Listen                string `json:"listen"`
	KeychainItem          string `json:"keychain_item"`
	OAuthKeychainItem     string `json:"oauth_keychain_item"`
	WorkerBin             string `json:"worker_bin"`
	Root                  string `json:"weft_root"`
	SegmentTimeoutSeconds int    `json:"segment_timeout_seconds"`
	// AgentUsers are the hosted slots' macOS users (docs/agent-users.md).
	// Each live segment runs as one of them, never shared, so capacity is
	// never more than there are.
	AgentUsers []string `json:"agent_users,omitempty"`
	// Repositories binds each hosted tk project to its checkout, a directory
	// directly under /Users/Shared/weft/repos. tk's config.yaml decides which
	// projects exist; only this map decides where their work runs, so a
	// project it does not name has no checkout.
	Repositories map[string]string `json:"repositories,omitempty"`
}

// DefaultWeftOAuthKeychainItem names the login-Keychain generic password that
// holds the long-lived `claude setup-token` token a hosted segment inherits as
// CLAUDE_CODE_OAUTH_TOKEN, when weft.oauth_keychain_item is unset.
const DefaultWeftOAuthKeychainItem = "weft-claude-oauth-token"

// Observer is one operator-owned domain integration. Commands are argv arrays
// so they are executed directly, never through a shell, and WorkingDir is
// explicit so daemon and manual runs behave the same.
type Observer struct {
	Definition         string           `json:"definition"`
	Collector          Command          `json:"collector"`
	Notebook           Command          `json:"notebook"`
	DataPolicy         string           `json:"data_policy"`
	DenyRead           []string         `json:"deny_read"`
	MaxModelInputBytes int              `json:"max_model_input_bytes"`
	TimeoutSeconds     int              `json:"timeout_seconds"`
	Tickets            *ObserverTickets `json:"tickets,omitempty"`
}

// ObserverTickets is the ticket write scope of one observer: the tk projects
// its notes and reopens may land in and its discoveries may be created in,
// which of those a discovery is created in, and how many discoveries one run
// may create. Absent, the observer's actions are retained and refused rather
// than applied.
type ObserverTickets struct {
	Projects         []string `json:"projects"`
	DiscoveryProject string   `json:"discovery_project"`
	MaxDiscoveries   int      `json:"max_discoveries"`
}

type Command struct {
	Argv       []string `json:"argv"`
	WorkingDir string   `json:"working_dir"`
}

// Home returns the state root: $SHUTTLE_HOME, else ~/.shuttle.
func Home() (string, error) {
	if h := os.Getenv(EnvHome); h != "" {
		return h, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".shuttle"), nil
}

// LedgerPath returns Home()/runs.db, the run ledger (§6).
func LedgerPath() (string, error) {
	home, err := Home()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ledgerFile), nil
}

// ReportsDir returns Home()/reports, §9.3's repo.
func ReportsDir() (string, error) {
	home, err := Home()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, reportsDir), nil
}

// Load reads Home()/config.json. Unknown keys refuse the load, so an inline
// secret is an error rather than a silently ignored key.
func Load() (*Config, error) {
	home, err := Home()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(home, configFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Decode reads one JSON value; anything after it would otherwise slip past
	// the strict decode above.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: trailing content after the config object", path)
	}
	if err := validateRepositories(hostedRepositoriesRoot, c.Weft.Repositories); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := validateAgentUsers(c.Weft.AgentUsers); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Both trees carry the inheritable agent-write ACL (docs/agent-users.md).
	if err := validateWeftRoot(c.Weft.Root, []string{HostedWorktreeRoot, hostedRepositoriesRoot}); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

// validateWeftRoot refuses a weft_root at or inside any of trees, which agent
// users can write. Containment is judged by spelling and by file identity,
// through the deepest existing ancestor of each, so a symlink, firmlink or
// case alias into a tree is refused as the tree itself is, and a tree not on
// disk yet still refuses its own spelling. A containment that cannot be
// established refuses the load.
func validateWeftRoot(root string, trees []string) error {
	if root == "" {
		return nil
	}
	clean := filepath.Clean(root)
	for _, tree := range trees {
		inside, err := sandbox.Covers(tree, clean)
		if err != nil {
			return fmt.Errorf("weft.weft_root: %s: whether it is inside %s, which agent users can write, cannot be established: %v", root, tree, err)
		}
		if inside {
			return fmt.Errorf("weft.weft_root: %s is at or inside %s, which agent users can write; the run logs, summaries and base records under it must be the operator's alone",
				root, tree)
		}
	}
	return nil
}

// refusedAgentGroups are the groups an agent user may not be in: staff can
// read the operator's 0750 home and admin is one sudo away from everything,
// so either undoes the boundary (docs/agent-users.md). weft-worker refuses
// the same two at launch.
var refusedAgentGroups = []string{"admin", "staff"}

// lookupAccount answers a user's uid and the name of every group it is in,
// primary and supplementary. A var so a test can stand in for the directory.
var lookupAccount = func(name string) (string, []string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return "", nil, err
	}
	gids, err := u.GroupIds()
	if err != nil {
		return "", nil, err
	}
	groups := make([]string, 0, len(gids))
	for _, gid := range gids {
		g, err := user.LookupGroupId(gid)
		if err != nil {
			return "", nil, err
		}
		groups = append(groups, g.Name)
	}
	return u.Uid, groups, nil
}

// validateAgentUsers refuses an agent user list that would not bound a
// segment: a name sudo could read as something else, a duplicate name or
// uid, which two live segments would share as one containment domain, the
// operator, and a member of a refused group. An account whose groups cannot be read is refused rather than
// assumed outside them.
func validateAgentUsers(names []string) error {
	operator := strconv.Itoa(os.Getuid())
	uids := map[string]int{}
	for i, name := range names {
		key := fmt.Sprintf("weft.agent_users[%d]", i)
		if !WellFormedAgentUser(name) {
			return fmt.Errorf("%s: %q is not an account name: letters, digits, _, - and ., starting with a letter or _", key, name)
		}
		if j := slices.Index(names, name); j < i {
			return fmt.Errorf("%s: %s is also weft.agent_users[%d]; each live segment holds its own agent user", key, name, j)
		}
		uid, groups, err := lookupAccount(name)
		if err != nil {
			return fmt.Errorf("%s: %s: its account and groups cannot be read: %v", key, name, err)
		}
		if uid == operator {
			return fmt.Errorf("%s: %s is the operator; a segment run as the operator is bounded by nothing", key, name)
		}
		if j, ok := uids[uid]; ok {
			return fmt.Errorf("%s: %s has uid %s, as weft.agent_users[%d] %s does; each live segment holds its own uid, which is what its containment kills",
				key, name, uid, j, names[j])
		}
		uids[uid] = i
		for _, group := range refusedAgentGroups {
			if slices.Contains(groups, group) {
				return fmt.Errorf("%s: %s is a member of %s, which undoes the agent user boundary", key, name, group)
			}
		}
	}
	return nil
}

// WellFormedAgentUser reports whether name is one weft-worker takes as
// --agent-user: an account name's own characters, starting with a letter or
// _. sudo reads -u -x as a flag and -u #501 as a uid, so the first character
// matters as much as the rest.
func WellFormedAgentUser(name string) bool {
	for i, r := range name {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		if i == 0 && !letter && r != '_' {
			return false
		}
		if !letter && !(r >= '0' && r <= '9') && r != '_' && r != '-' && r != '.' {
			return false
		}
	}
	return name != ""
}

// validateRepositories refuses a hosted checkout binding agent users could not
// reach: anything but a path directly under root, and anything under the
// operator's home. The check is lexical only: agents can write root, so
// whatever is on disk there is the catalog's to judge per project on every
// read, never a reason for load to fail. Bindings are checked in key order,
// so the error reported is the same on every load.
func validateRepositories(root string, repos map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(repos)) {
		key, path := "weft.repositories."+name, repos[name]
		if !filepath.IsAbs(path) {
			return fmt.Errorf("%s: %q is not an absolute path; a hosted checkout must be a directory directly under %s", key, path, root)
		}
		clean := filepath.Clean(path)
		for _, home := range operatorHomes() {
			if clean == home || strings.HasPrefix(clean, home+string(filepath.Separator)) {
				return fmt.Errorf("%s: %s is under the operator's home %s, which agent users cannot traverse; a hosted checkout must be a directory directly under %s, where agent users can reach it",
					key, path, home, root)
			}
		}
		if base := filepath.Base(clean); filepath.Dir(clean) != root || base == "." || base == ".." {
			return fmt.Errorf("%s: %s is not a directory directly under %s, where agent users can reach a hosted checkout", key, path, root)
		}
	}
	return nil
}

// operatorHomes is the operator's home as spelled and as resolved. / is left
// out: everything is under it.
func operatorHomes() []string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return nil
	}
	homes := []string{filepath.Clean(home)}
	if resolved, err := filepath.EvalSymlinks(home); err == nil {
		homes = append(homes, resolved)
	}
	return slices.DeleteFunc(homes, func(h string) bool { return h == "/" })
}

// AgentsDir resolves the agent definitions directory: $SHUTTLE_AGENTS_DIR,
// then config.json's agents_dir, then ~/code/shuttle/agents.
func AgentsDir() (string, error) {
	if d := os.Getenv(EnvAgentsDir); d != "" {
		return d, nil
	}
	c, err := Load()
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if c != nil && c.AgentsDir != "" {
		return c.AgentsDir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "code", "shuttle", "agents"), nil
}

// AnthropicKey resolves the model API key from the Keychain at use time.
func (c *Config) AnthropicKey() (string, error) {
	return keychain.Find(c.Anthropic.KeychainItem)
}

// EmailToken resolves the delivery token from the Keychain at use time.
func (c *Config) EmailToken() (string, error) {
	return keychain.Find(c.Email.KeychainItem)
}

// WeftToken resolves the remote service bearer token at daemon start.
func (c *Config) WeftToken() (string, error) {
	return keychain.Find(c.Weft.KeychainItem)
}

// WeftOAuthKeychainItem is the item the hosted segments' Claude OAuth token is
// read from at launch time: the configured name, else
// DefaultWeftOAuthKeychainItem.
func (c *Config) WeftOAuthKeychainItem() string {
	if c.Weft.OAuthKeychainItem != "" {
		return c.Weft.OAuthKeychainItem
	}
	return DefaultWeftOAuthKeychainItem
}
