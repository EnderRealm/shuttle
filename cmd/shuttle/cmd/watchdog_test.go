package cmd

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"

	"shuttle/internal/launchd"
)

func TestWatchDaemonOnceLeavesARunningDaemonAlone(t *testing.T) {
	originalProcess, originalKickstart := watchdogProcess, watchdogKickstart
	watchdogProcess = func(label string) (launchd.Process, bool, error) {
		return launchd.Process{PID: 42}, true, nil
	}
	kicks := 0
	watchdogKickstart = func(label string) error {
		kicks++
		return nil
	}
	t.Cleanup(func() {
		watchdogProcess, watchdogKickstart = originalProcess, originalKickstart
	})

	watchDaemonOnce(log.New(&bytes.Buffer{}, "", 0))
	if kicks != 0 {
		t.Fatalf("kickstarts = %d, want none", kicks)
	}
}

func TestWatchDaemonOnceKickstartsAProcesslessDaemon(t *testing.T) {
	originalProcess, originalKickstart := watchdogProcess, watchdogKickstart
	watchdogProcess = func(label string) (launchd.Process, bool, error) {
		return launchd.Process{}, false, nil
	}
	var kicked string
	watchdogKickstart = func(label string) error {
		kicked = label
		return nil
	}
	t.Cleanup(func() {
		watchdogProcess, watchdogKickstart = originalProcess, originalKickstart
	})

	var logs bytes.Buffer
	watchDaemonOnce(log.New(&logs, "", 0))
	if kicked != launchd.DaemonLabel {
		t.Fatalf("kickstarted %q, want %q", kicked, launchd.DaemonLabel)
	}
	if !strings.Contains(logs.String(), "kickstarted") {
		t.Fatalf("log = %q, want recovery recorded", logs.String())
	}
}

func TestWatchDaemonOnceDoesNotKickOnAnUnreadableProcess(t *testing.T) {
	originalProcess, originalKickstart := watchdogProcess, watchdogKickstart
	watchdogProcess = func(label string) (launchd.Process, bool, error) {
		return launchd.Process{}, false, errors.New("ps failed")
	}
	kicks := 0
	watchdogKickstart = func(label string) error {
		kicks++
		return nil
	}
	t.Cleanup(func() {
		watchdogProcess, watchdogKickstart = originalProcess, originalKickstart
	})

	var logs bytes.Buffer
	watchDaemonOnce(log.New(&logs, "", 0))
	if kicks != 0 {
		t.Fatalf("kickstarts = %d, want none", kicks)
	}
	if !strings.Contains(logs.String(), "ps failed") {
		t.Fatalf("log = %q, want inspection error", logs.String())
	}
}
