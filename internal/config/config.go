package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"shuttle/internal/keychain"
)

const (
	// EnvHome overrides the state root; EnvAgentsDir overrides the agent
	// definitions directory. Both are carried in the launchd plist (§13).
	EnvHome      = "SHUTTLE_HOME"
	EnvAgentsDir = "SHUTTLE_AGENTS_DIR"

	configFile = "config.json"
	ledgerFile = "runs.db"
	reportsDir = "reports"
)

// Config is config.json, the §4.2 shape. Credentials are Keychain item names
// only; no secret value is ever held here or in the file.
type Config struct {
	AgentsDir string    `json:"agents_dir"`
	Timezone  string    `json:"timezone"`
	Email     Email     `json:"email"`
	Anthropic Anthropic `json:"anthropic"`
	Paths     Paths     `json:"paths"`
	Notify    Notify    `json:"notify"`
	Limits    Limits    `json:"limits"`
	Output    Output    `json:"output"`
	Weft      Weft      `json:"weft"`
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

// Weft configures the private weft-remote/v1 service hosted by the daemon.
// An empty Listen disables the service, which keeps phase-1 installations
// unchanged until the block is configured.
type Weft struct {
	Listen                string      `json:"listen"`
	KeychainItem          string      `json:"keychain_item"`
	OAuthKeychainItem     string      `json:"oauth_keychain_item"`
	WorkerBin             string      `json:"worker_bin"`
	Root                  string      `json:"weft_root"`
	SegmentTimeoutSeconds int         `json:"segment_timeout_seconds"`
	Sandbox               WeftSandbox `json:"sandbox"`
}

// DefaultWeftOAuthKeychainItem names the login-Keychain generic password that
// holds the long-lived `claude setup-token` token a hosted segment inherits as
// CLAUDE_CODE_OAUTH_TOKEN, when weft.oauth_keychain_item is unset.
const DefaultWeftOAuthKeychainItem = "weft-claude-oauth-token"

type WeftSandbox struct {
	AllowedDomains []string `json:"allowed_domains"`
	AllowWrite     []string `json:"allow_write"`
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
	return &c, nil
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
