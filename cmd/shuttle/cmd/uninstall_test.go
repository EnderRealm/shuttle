package cmd

import (
	"bytes"
	"testing"

	"shuttle/internal/launchd"
)

func TestUninstallRemovesOnlyTheDaemonAndIsIdempotent(t *testing.T) {
	original := uninstallLaunchAgent
	var labels []string
	uninstallLaunchAgent = func(label string) error {
		labels = append(labels, label)
		return nil
	}
	t.Cleanup(func() {
		uninstallLaunchAgent = original
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
	})

	var out bytes.Buffer
	rootCmd.SetOut(&out)
	for i := 0; i < 2; i++ {
		rootCmd.SetArgs([]string{"uninstall"})
		if err := rootCmd.Execute(); err != nil {
			t.Fatalf("uninstall %d: %v", i+1, err)
		}
	}
	want := []string{
		launchd.WatchdogLabel, launchd.DaemonLabel,
		launchd.WatchdogLabel, launchd.DaemonLabel,
	}
	if len(labels) != len(want) {
		t.Fatalf("labels = %v", labels)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("labels = %v, want %v", labels, want)
		}
	}
}
