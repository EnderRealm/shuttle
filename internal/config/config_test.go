package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
    "weft_root": "/Users/steve/.shuttle/weft",
    "segment_timeout_seconds": 21600,
    "sandbox": {
      "allowed_domains": ["github.com"],
      "allow_write": ["/Users/steve/Library/Caches/org.swift.swiftpm"]
    }
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
		c.Weft.SegmentTimeoutSeconds != 21600 || len(c.Weft.Sandbox.AllowWrite) != 1 {
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
