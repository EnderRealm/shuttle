package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"testing"

	"shuttle/internal/launchd"
)

func TestWatchDaemonOnceLeavesARunningDaemonAlone(t *testing.T) {
	shuttleEnv(t)
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

	watchDaemonOnce(log.New(&bytes.Buffer{}, "", 0), &watchState{})
	if kicks != 0 {
		t.Fatalf("kickstarts = %d, want none", kicks)
	}
}

func TestWatchDaemonOnceKickstartsAProcesslessDaemon(t *testing.T) {
	shuttleEnv(t)
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
	watchDaemonOnce(log.New(&logs, "", 0), &watchState{})
	if kicked != launchd.DaemonLabel {
		t.Fatalf("kickstarted %q, want %q", kicked, launchd.DaemonLabel)
	}
	if !strings.Contains(logs.String(), "kickstarted") {
		t.Fatalf("log = %q, want recovery recorded", logs.String())
	}
}

func TestWatchDaemonOnceDoesNotKickOnAnUnreadableProcess(t *testing.T) {
	shuttleEnv(t)
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
	watchDaemonOnce(log.New(&logs, "", 0), &watchState{})
	if kicks != 0 {
		t.Fatalf("kickstarts = %d, want none", kicks)
	}
	if !strings.Contains(logs.String(), "ps failed") {
		t.Fatalf("log = %q, want inspection error", logs.String())
	}
}

// daemon.log is both the daemon's stderr and the watchdog's: launchd points
// both jobs' StandardErrorPath at it. The daemon starts here are real ones,
// each failing on config.json and printed as Execute prints it.
func TestWatchdogLogsARepeatedStartFailureOnce(t *testing.T) {
	shuttleEnv(t)
	stubPipeline(t, nil)
	var daemonLog bytes.Buffer
	logger := log.New(&daemonLog, "", 0)
	running := false
	originalProcess, originalKickstart := watchdogProcess, watchdogKickstart
	watchdogProcess = func(label string) (launchd.Process, bool, error) {
		return launchd.Process{PID: 42}, running, nil
	}
	kicks := 0
	watchdogKickstart = func(label string) error {
		kicks++
		return nil
	}
	daemonCmd.SetErr(&daemonLog)
	daemonCmd.SetContext(context.Background())
	t.Cleanup(func() {
		watchdogProcess, watchdogKickstart = originalProcess, originalKickstart
		daemonCmd.SetErr(nil)
		daemonCmd.SetContext(context.Background())
	})
	failedStart := func() {
		t.Helper()
		err := daemonCmd.RunE(daemonCmd, nil)
		if err == nil {
			t.Fatalf("daemon started on config.json it should refuse, log:\n%s", daemonLog.String())
		}
		reportError(&daemonLog, err)
	}
	count := func(s string) int { return strings.Count(daemonLog.String(), s) }
	const allowWrite, repositories = "weft.allow_write: Shuttle no longer reads", "move it under weft"
	state := &watchState{}

	writeConfig(t, `{"weft": {"allow_write": ["/Users/Shared/weft"]}}`)
	for range 3 {
		failedStart()
		watchDaemonOnce(logger, state)
		watchDaemonOnce(logger, state)
	}
	if kicks != 6 {
		t.Fatalf("kickstarts = %d, want one per tick", kicks)
	}
	if count(allowWrite) != 1 || count("kickstarted") != 0 {
		t.Fatalf("same failure three times, log:\n%s\nwant its line once and no kickstart line", daemonLog.String())
	}

	writeConfig(t, `{"repositories": {"shuttle": "/Users/Shared/weft/repos/shuttle"}}`)
	for range 3 {
		failedStart()
		watchDaemonOnce(logger, state)
	}
	if count(allowWrite) != 1 || count(repositories) != 1 || count("kickstarted") != 0 {
		t.Fatalf("a different failure, log:\n%s\nwant each failure's line once", daemonLog.String())
	}

	// A failure after the restart pass: the weft address is held, so the bind
	// ends each start. No start reached its loop, so none logs daemon start.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	writeConfig(t, fmt.Sprintf(`{"weft": {"listen": %q}}`, held.Addr().String()))
	for range 3 {
		failedStart()
		watchDaemonOnce(logger, state)
	}
	if count("address already in use") != 1 || count("daemon start") != 0 || count("kickstarted") != 0 {
		t.Fatalf("a held weft address, log:\n%s\nwant the bind failure once and no daemon start line", daemonLog.String())
	}

	// Running with the record still standing is a daemon still starting.
	running = true
	watchDaemonOnce(logger, state)
	if count("is up") != 0 {
		t.Fatalf("log:\n%s\nwant no recovery before the daemon's loop is running", daemonLog.String())
	}
	writeConfig(t, `{}`)
	// Cancelled up front: the loop runs one empty tick and returns.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	daemonCmd.SetContext(ctx)
	if err := daemonCmd.RunE(daemonCmd, nil); err != nil {
		t.Fatalf("daemon: %v\n%s", err, daemonLog.String())
	}
	if _, recorded := startFailure(); recorded {
		t.Fatal("a daemon whose loop ran left its start failure on record")
	}
	watchDaemonOnce(logger, state)
	watchDaemonOnce(logger, state)
	if count("daemon start") != 1 || count("watchdog: "+launchd.DaemonLabel+" is up after a start failure") != 1 {
		t.Fatalf("log:\n%s\nwant the daemon coming up logged once", daemonLog.String())
	}

	// After it was up, the first failure is new again.
	running = false
	writeConfig(t, `{"weft": {"allow_write": []}}`)
	failedStart()
	failedStart()
	watchDaemonOnce(logger, state)
	if count(allowWrite) != 2 || count("kickstarted") != 0 {
		t.Fatalf("log:\n%s\nwant the failure logged again, once, after the daemon was up", daemonLog.String())
	}
}
