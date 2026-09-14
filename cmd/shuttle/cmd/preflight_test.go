package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreflightCommandFitsMacOSTemporarySocketLimit(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	writeAgentDir(t, agents, "broken", "{bad json}")
	longTmp := filepath.Join(t.TempDir(), strings.Repeat("macos-temp-", 10))
	if err := os.MkdirAll(longTmp, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", longTmp)
	root := t.TempDir()
	claude := filepath.Join(root, "claude")
	if err := os.WriteFile(claude, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	srt := filepath.Join(root, "srt")
	marker := filepath.Join(root, "profile-path")
	if err := os.WriteFile(srt, []byte(fmt.Sprintf("#!/bin/sh\nprintf '%%s' \"$2\" > '%s'\n", marker)), 0700); err != nil {
		t.Fatal(err)
	}
	// The empty key name fails without accessing the real user's Keychain.
	writeConfig(t, fmt.Sprintf(`{"paths":{"claude_bin":%q,"srt_bin":%q}}`, claude, srt))
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"preflight", "daily"})
	t.Cleanup(func() { rootCmd.SetArgs(nil); rootCmd.SetOut(nil) })
	err := rootCmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "anthropic.keychain_item") {
		t.Fatalf("want key check after srt, got %v; %s", err, &out)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(string(data))
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("temporary preflight directory was not removed: %s/%v", dir, err)
	}
	if !strings.Contains(out.String(), "srt\tpass") {
		t.Fatalf("sandbox did not pass: %s", &out)
	}
}
