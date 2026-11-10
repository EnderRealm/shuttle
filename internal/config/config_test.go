package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// sampleConfig's weft_root is outside every home: Load judges it against the
// shared trees by file identity, which stats it, and an agent user running
// this suite cannot search the operator's home.
const sampleConfig = `{
  "agents_dir": "/Users/steve/code/shuttle/agents",
  "timezone": "America/Los_Angeles",
  "email": {
    "provider": "cloudflare",
    "account_id": "acct-1",
    "from": "shuttle@example.com",
    "to": ["steve@example.com"],
    "keychain_item": "shuttle-cloudflare-token"
  },
  "anthropic": {
    "keychain_item": "shuttle-anthropic-api-key"
  },
  "paths": {
    "loom_home": "/Users/steve/.loom",
    "tk_config": "/Users/steve/.ticket/config.yaml",
    "claude_bin": "/opt/homebrew/bin/claude",
    "srt_bin": "/opt/homebrew/bin/srt",
    "loom_bin": "/Users/steve/.local/bin/loom"
  },
  "notify": { "enabled": true, "cooldown_minutes": 60 },
  "limits": { "daily_runs_per_agent": 3, "daily_spend_usd": 10.0 },
  "weft": {
    "listen": "100.101.102.103:7431",
    "keychain_item": "weft-remote-token",
    "oauth_keychain_item": "weft-claude-oauth-token",
    "worker_bin": "/Users/steve/.local/bin/weft-worker",
    "weft_root": "/opt/shuttle/weft",
    "segment_timeout_seconds": 21600,
    "agent_users": ["weftagent1", "weftagent2"]
  }
}`

func writeConfig(t *testing.T, body string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, configFile), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHome, dir)
	// Cleared so an exported override in the ambient environment cannot decide
	// what AgentsDir resolves to.
	t.Setenv(EnvAgentsDir, "")
	accounts(t, map[string]account{
		"weftagent1": {uid: "9001", groups: []string{"weftagents"}},
		"weftagent2": {uid: "9002", groups: []string{"weftagents"}},
	})
}

type account struct {
	uid    string
	groups []string
}

// accounts stands in for the directory: a name it does not hold does not
// exist.
func accounts(t *testing.T, known map[string]account) {
	t.Helper()
	prior := lookupAccount
	lookupAccount = func(name string) (string, []string, error) {
		a, ok := known[name]
		if !ok {
			return "", nil, fmt.Errorf("user: unknown user %s", name)
		}
		return a.uid, a.groups, nil
	}
	t.Cleanup(func() { lookupAccount = prior })
}

func TestLoad(t *testing.T) {
	writeConfig(t, sampleConfig)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AgentsDir != "/Users/steve/code/shuttle/agents" {
		t.Errorf("agents_dir = %q", c.AgentsDir)
	}
	if c.Timezone != "America/Los_Angeles" {
		t.Errorf("timezone = %q", c.Timezone)
	}
	if c.Email.Provider != "cloudflare" || c.Email.AccountID != "acct-1" || c.Email.From != "shuttle@example.com" {
		t.Errorf("email = %+v", c.Email)
	}
	if len(c.Email.To) != 1 || c.Email.To[0] != "steve@example.com" {
		t.Errorf("email.to = %v", c.Email.To)
	}
	if c.Paths.ClaudeBin != "/opt/homebrew/bin/claude" || c.Paths.SRTBin != "/opt/homebrew/bin/srt" {
		t.Errorf("paths = %+v", c.Paths)
	}
	if !c.Notify.Enabled || c.Notify.CooldownMinutes != 60 {
		t.Errorf("notify = %+v", c.Notify)
	}
	if c.Limits.DailyRunsPerAgent != 3 || c.Limits.DailySpendUSD != 10.0 {
		t.Errorf("limits = %+v", c.Limits)
	}
	// Credentials are item names, resolved through the Keychain at use time.
	if c.Anthropic.KeychainItem != "shuttle-anthropic-api-key" {
		t.Errorf("anthropic.keychain_item = %q", c.Anthropic.KeychainItem)
	}
	if c.Email.KeychainItem != "shuttle-cloudflare-token" {
		t.Errorf("email.keychain_item = %q", c.Email.KeychainItem)
	}
	if c.Weft.Listen != "100.101.102.103:7431" || c.Weft.KeychainItem != "weft-remote-token" ||
		c.Weft.OAuthKeychainItem != "weft-claude-oauth-token" ||
		c.Weft.SegmentTimeoutSeconds != 21600 || !slices.Equal(c.Weft.AgentUsers, []string{"weftagent1", "weftagent2"}) {
		t.Errorf("weft = %+v", c.Weft)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "unknown key",
			body: strings.Replace(sampleConfig, `"timezone":`, `"timezone_name": "UTC", "timezone":`, 1),
			want: "timezone_name",
		},
		{
			name: "inline secret",
			body: strings.Replace(sampleConfig, `"keychain_item": "shuttle-anthropic-api-key"`, `"api_key": "sk-live-secret"`, 1),
			want: "api_key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeConfig(t, tc.body)
			_, err := Load()
			if err == nil {
				t.Fatal("Load succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

func TestLoadNamesARetiredKeyAndItsRemedy(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
		not  string
	}{
		{
			name: "weft.allow_write",
			body: strings.Replace(sampleConfig, `"segment_timeout_seconds": 21600,`,
				`"segment_timeout_seconds": 21600, "allow_write": ["/Users/Shared/weft"],`, 1),
			want: []string{"weft.allow_write", "delete it"},
		},
		{
			name: "top-level repositories",
			body: strings.Replace(sampleConfig, `"timezone":`,
				`"repositories": {"shuttle": "/Users/Shared/weft/repos/shuttle"}, "timezone":`, 1),
			want: []string{"repositories", "move it under weft, as weft.repositories"},
		},
		{
			// allow_write is retired only where Shuttle once read it; anywhere
			// else it is a key Shuttle never accepted.
			name: "allow_write elsewhere",
			body: strings.Replace(sampleConfig, `"timezone":`, `"allow_write": [], "timezone":`, 1),
			want: []string{`unknown field "allow_write"`},
			not:  "no longer reads",
		},
		{
			name: "never accepted",
			body: strings.Replace(sampleConfig, `"segment_timeout_seconds": 21600,`,
				`"segment_timeout_seconds": 21600, "allow_writes": [],`, 1),
			want: []string{`unknown field "allow_writes"`},
			not:  "no longer reads",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeConfig(t, tc.body)
			_, err := Load()
			if err == nil {
				t.Fatal("Load succeeded, want an error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not carry %q", err, want)
				}
			}
			if tc.not != "" && strings.Contains(err.Error(), tc.not) {
				t.Errorf("error %q reports a retired key", err)
			}
		})
	}
}

func TestLoadRejectsTrailingJSON(t *testing.T) {
	writeConfig(t, sampleConfig+"\n{\"anthropic\": {\"api_key\": \"sk-live-secret\"}}\n")
	_, err := Load()
	if err == nil {
		t.Fatal("Load succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "trailing content") {
		t.Errorf("error %q does not report trailing content", err)
	}
}

func TestAgentsDirPrecedence(t *testing.T) {
	writeConfig(t, sampleConfig)
	got, err := AgentsDir()
	if err != nil {
		t.Fatalf("AgentsDir: %v", err)
	}
	if got != "/Users/steve/code/shuttle/agents" {
		t.Errorf("AgentsDir = %q, want the configured dir", got)
	}
	t.Setenv(EnvAgentsDir, "/tmp/agents")
	got, err = AgentsDir()
	if err != nil {
		t.Fatalf("AgentsDir: %v", err)
	}
	if got != "/tmp/agents" {
		t.Errorf("AgentsDir = %q, want the environment override", got)
	}
}

func TestWeftOAuthKeychainItemDefaults(t *testing.T) {
	c := &Config{}
	if got := c.WeftOAuthKeychainItem(); got != DefaultWeftOAuthKeychainItem {
		t.Errorf("unset item = %q, want %q", got, DefaultWeftOAuthKeychainItem)
	}
	c.Weft.OAuthKeychainItem = "custom-item"
	if got := c.WeftOAuthKeychainItem(); got != "custom-item" {
		t.Errorf("configured item = %q", got)
	}
}

func repositoriesConfig(binding string) string {
	return strings.Replace(sampleConfig, `"segment_timeout_seconds": 21600,`,
		`"segment_timeout_seconds": 21600, "repositories": {"shuttle": "/Users/Shared/weft/repos/shuttle", "weft": `+strconv.Quote(binding)+`},`, 1)
}

func TestLoadAcceptsAHostedRepositoryBinding(t *testing.T) {
	writeConfig(t, repositoriesConfig("/Users/Shared/weft/repos/weft"))
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.Weft.Repositories["weft"]; got != "/Users/Shared/weft/repos/weft" {
		t.Errorf("weft.repositories.weft = %q", got)
	}
}

func TestLoadRefusesRepositoryBindingsAgentsCannotReach(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		binding string
		want    string
	}{
		{"operator home", filepath.Join(home, "code", "weft"), "under the operator's home"},
		{"worktrees tree", "/Users/Shared/weft/worktrees/x", "not a directory directly under /Users/Shared/weft/repos"},
		{"the root itself", "/Users/Shared/weft/repos", "not a directory directly under /Users/Shared/weft/repos"},
		{"nested", "/Users/Shared/weft/repos/a/b", "not a directory directly under /Users/Shared/weft/repos"},
		{"relative", "repos/weft", "not an absolute path"},
		{"dot-dot escape", "/Users/Shared/weft/repos/../worktrees", "not a directory directly under /Users/Shared/weft/repos"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeConfig(t, repositoriesConfig(tc.binding))
			_, err := Load()
			if err == nil {
				t.Fatal("Load succeeded, want an error")
			}
			for _, want := range []string{configFile, "weft.repositories.weft", tc.binding, tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
		})
	}
}

// Load judges a binding by its spelling alone: agents can write repos/, so
// nothing they plant there may stop shuttle from starting. A symlink child
// pointing outside the root and a child not yet provisioned both load; the
// catalog refuses them per project.
func TestValidateRepositoriesIsLexical(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repos")
	outside := filepath.Join(t.TempDir(), "elsewhere")
	for _, dir := range []string{filepath.Join(root, "real"), outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	repos := map[string]string{"real": filepath.Join(root, "real"), "missing": filepath.Join(root, "missing"), "linked": link}
	if err := validateRepositories(root, repos); err != nil {
		t.Errorf("bindings directly under the root, whatever is on disk there: %v", err)
	}
}

func agentUsersConfig(users string) string {
	return strings.Replace(sampleConfig, `"agent_users": ["weftagent1", "weftagent2"]`, `"agent_users": `+users, 1)
}

// An agent user that would not bound its segment refuses the load, naming
// the entry and why.
func TestLoadRefusesAgentUsersThatBoundNothing(t *testing.T) {
	operator := strconv.Itoa(os.Getuid())
	cases := []struct {
		name, users string
		want        []string
	}{
		{"duplicate", `["weftagent1", "weftagent2", "weftagent1"]`, []string{"weft.agent_users[2]", "weftagent1", "weft.agent_users[0]"}},
		{"operator", `["weftagent1", "steve"]`, []string{"weft.agent_users[1]", "steve", "is the operator"}},
		{"admin", `["weftagent1", "admin1"]`, []string{"weft.agent_users[1]", "admin1", "member of admin"}},
		{"staff", `["staff1"]`, []string{"weft.agent_users[0]", "staff1", "member of staff"}},
		{"unknown", `["weftagent9"]`, []string{"weft.agent_users[0]", "weftagent9", "cannot be read"}},
		{"flag", `["-x"]`, []string{"weft.agent_users[0]", `"-x"`, "not an account name"}},
		{"uid", `["#501"]`, []string{"weft.agent_users[0]", `"#501"`, "not an account name"}},
		{"empty", `[""]`, []string{"weft.agent_users[0]", "not an account name"}},
		{"same uid", `["weftagent1", "weftalias"]`, []string{"weft.agent_users[1]", "weftalias", "uid 9001", "weft.agent_users[0] weftagent1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeConfig(t, agentUsersConfig(tc.users))
			accounts(t, map[string]account{
				"weftagent1": {uid: "9001", groups: []string{"weftagents"}},
				"weftagent2": {uid: "9002", groups: []string{"weftagents"}},
				"steve":      {uid: operator, groups: []string{"staff", "admin"}},
				"admin1":     {uid: "9003", groups: []string{"weftagents", "admin"}},
				"staff1":     {uid: "9004", groups: []string{"weftagents", "staff"}},
				"weftalias":  {uid: "9001", groups: []string{"weftagents"}},
			})
			_, err := Load()
			if err == nil {
				t.Fatal("Load succeeded, want an error")
			}
			for _, want := range append(tc.want, configFile) {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
		})
	}
}

// Agent users can write both hosted trees, so weft_root, which holds what
// grades and resumes a run, may not be at or inside either.
func TestLoadRefusesAWeftRootAgentsCanWrite(t *testing.T) {
	for _, tree := range []string{HostedWorktreeRoot, hostedRepositoriesRoot} {
		for _, root := range []string{tree, tree + "/state", tree + "/"} {
			t.Run(root, func(t *testing.T) {
				writeConfig(t, strings.Replace(sampleConfig, `"/opt/shuttle/weft"`, strconv.Quote(root), 1))
				_, err := Load()
				if err == nil || !strings.Contains(err.Error(), "weft.weft_root") || !strings.Contains(err.Error(), "inside "+tree+",") {
					t.Fatalf("Load = %v, want weft.weft_root refused naming %s", err, tree)
				}
			})
		}
		writeConfig(t, strings.Replace(sampleConfig, `"/opt/shuttle/weft"`, strconv.Quote(tree+"-state"), 1))
		if _, err := Load(); err != nil {
			t.Fatalf("a sibling of %s: %v", tree, err)
		}
	}
}

// An alias of a shared tree is the tree: a symlink to it or to an ancestor,
// and a case alias on a case-insensitive volume, are refused as its own
// spelling is, while a sibling is not.
func TestValidateWeftRootJudgesByIdentity(t *testing.T) {
	base := t.TempDir()
	trees := []string{filepath.Join(base, "shared", "worktrees"), filepath.Join(base, "shared", "repos")}
	for _, tree := range trees {
		if err := os.MkdirAll(tree, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	links := t.TempDir()
	for i, tree := range trees {
		name := filepath.Base(tree)
		link := filepath.Join(links, name)
		if err := os.Symlink(tree, link); err != nil {
			t.Fatal(err)
		}
		shared := filepath.Join(links, "shared-"+name)
		if err := os.Symlink(filepath.Dir(tree), shared); err != nil {
			t.Fatal(err)
		}
		refused := []string{link, filepath.Join(link, "state"), filepath.Join(shared, name, "state")}
		upper := filepath.Join(filepath.Dir(tree), strings.ToUpper(name))
		if _, err := os.Stat(upper); err == nil {
			refused = append(refused, upper, filepath.Join(upper, "state"))
		} else {
			t.Logf("%s: the volume is case-sensitive; no case alias to test", upper)
		}
		for _, root := range refused {
			err := validateWeftRoot(root, trees)
			if err == nil || !strings.Contains(err.Error(), "inside "+trees[i]+",") {
				t.Errorf("validateWeftRoot(%s) = %v, want it refused as inside %s", root, err, trees[i])
			}
		}
	}
	for _, root := range []string{filepath.Join(base, "shared", "state"), filepath.Join(base, "shared", "repos-state"), filepath.Join(links, "elsewhere")} {
		if err := validateWeftRoot(root, trees); err != nil {
			t.Errorf("validateWeftRoot(%s) = %v, want it accepted", root, err)
		}
	}
	// A tree not on disk refuses its own spelling.
	missing := filepath.Join(base, "missing", "worktrees")
	if err := validateWeftRoot(filepath.Join(missing, "state"), []string{missing}); err == nil {
		t.Errorf("a root inside a tree not on disk was accepted")
	}
}

// The real directory answers for the operator and for an account with no
// supplementary groups to hide.
func TestLookupAccountReadsTheDirectory(t *testing.T) {
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	uid, groups, err := lookupAccount(me.Username)
	if err != nil {
		t.Fatal(err)
	}
	if uid != strconv.Itoa(os.Getuid()) || len(groups) == 0 {
		t.Fatalf("lookupAccount(%s) = %s %v", me.Username, uid, groups)
	}
	if _, _, err := lookupAccount("no-such-agent-user-0"); err == nil {
		t.Fatal("an unknown account was read")
	}
}
