package ledger

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// hostEvents is each host event's dispatch and reason, oldest first.
func hostEvents(t *testing.T, s *Store) [][2]any {
	t.Helper()
	events, err := s.WeftEventsAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	var out [][2]any
	for _, e := range events {
		if e.Type != "host" {
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal(e.Payload, &doc); err != nil {
			t.Fatal(err)
		}
		if doc["contract_version"] != WeftServiceVersion || doc["seq"] != float64(e.Seq) || doc["ts"] == nil {
			t.Fatalf("host event = %s", e.Payload)
		}
		out = append(out, [2]any{doc["dispatch"], doc["reason"]})
	}
	return out
}

func TestUpdatePauseMakesARowForAnAgentThatNeverRan(t *testing.T) {
	s := newStore(t)
	claimed, _, err := s.ClaimUpdatePause()
	if err != nil || !claimed {
		t.Fatalf("claim = %t/%v", claimed, err)
	}
	a, err := s.Agent(WeftAgent)
	if err != nil || !a.Paused || a.PausedReason.String != PauseUpdate {
		t.Fatalf("agent = %+v/%v", a, err)
	}
	if lifted, _, err := s.ReleaseUpdatePause(); err != nil || !lifted {
		t.Fatalf("release = %t/%v", lifted, err)
	}
}

func TestUpdatePauseTakesOverNothing(t *testing.T) {
	s := newStore(t)
	if err := s.Pause(WeftAgent); err != nil {
		t.Fatal(err)
	}
	before, err := s.Agent(WeftAgent)
	if err != nil {
		t.Fatal(err)
	}
	claimed, reason, err := s.ClaimUpdatePause()
	if err != nil || claimed || reason != PauseManual {
		t.Fatalf("claim = %t/%q/%v", claimed, reason, err)
	}
	if lifted, _, err := s.ReleaseUpdatePause(); err != nil || lifted {
		t.Fatalf("release = %t/%v", lifted, err)
	}
	after, err := s.Agent(WeftAgent)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("agent moved: %+v, was %+v", after, before)
	}
}

func TestReleaseUpdatePauseLeavesTheStreak(t *testing.T) {
	s := newStore(t)
	if _, err := s.db.Exec(`INSERT INTO agents(name,failure_streak) VALUES(?,4)`, WeftAgent); err != nil {
		t.Fatal(err)
	}
	if claimed, _, err := s.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("claim = %t/%v", claimed, err)
	}
	if lifted, _, err := s.ReleaseUpdatePause(); err != nil || !lifted {
		t.Fatalf("release = %t/%v", lifted, err)
	}
	a, err := s.Agent(WeftAgent)
	if err != nil || a.Paused || a.PausedReason.Valid || a.PausedAt.Valid || a.FailureStreak != 4 {
		t.Fatalf("agent = %+v/%v", a, err)
	}
}

// weft-service/v2 §10: a host event is written whenever the host-wide
// dispatch state or its reason changes, by every path that can change it.
func TestHostEventFollowsEveryPauseWrite(t *testing.T) {
	s := newStore(t)
	operator := WeftHostPausedDetail(PauseManual, s.timestamp())
	update := WeftHostPausedDetail(PauseUpdate, s.timestamp())
	auth := WeftHostPausedDetail(PauseAuth, weftTimestamp(s.now()))
	open := [2]any{WeftDispatchOpen, nil}

	if err := s.Pause(WeftAgent); err != nil {
		t.Fatal(err)
	}
	if err := s.Pause(WeftAgent); err != nil { // the same words: no change
		t.Fatal(err)
	}
	if err := s.Resume(WeftAgent); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(WeftAgent); err != nil { // nothing to lift
		t.Fatal(err)
	}
	if _, _, err := s.ClaimUpdatePause(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReleaseUpdatePause(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReleaseUpdatePause(); err != nil { // nothing to lift
		t.Fatal(err)
	}
	run := claimVisible(t, s, "weft/rejected-0001")
	if err := s.MarkWeftRunning(run.ID, 1, 1, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FailWeftSegment(run.ID, 1, ClassAuth, "the credential was refused"); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(WeftAgent); err != nil {
		t.Fatal(err)
	}
	want := [][2]any{
		{WeftDispatchPaused, operator}, open,
		{WeftDispatchPaused, update}, open,
		{WeftDispatchPaused, auth}, open,
	}
	if got := hostEvents(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("host events = %v\nwant %v", got, want)
	}
}

func TestHostEventIgnoresOtherAgents(t *testing.T) {
	s := newStore(t)
	if err := s.Pause("daily"); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume("daily"); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= maxFailureStreak; n++ {
		failNth(t, s, n, ClassDied)
	}
	if got := hostEvents(t, s); len(got) != 0 {
		t.Fatalf("host events = %v", got)
	}
}

// The words a client shows name the update, and offer no command: shuttle
// resume mid-update is the operator abandoning it, not something to suggest.
func TestUpdatePauseReasonNamesTheUpdate(t *testing.T) {
	detail := WeftHostPausedDetail(PauseUpdate, "2026-11-02T10:00:00Z")
	if !strings.Contains(detail, "updating") || !strings.Contains(detail, "2026-11-02T10:00:00Z") {
		t.Fatalf("detail = %q", detail)
	}
	if strings.Contains(detail, "shuttle resume") {
		t.Fatalf("detail = %q tells a reader to intervene", detail)
	}
	if got := WeftHostPausedDetail(PauseManual, "t"); !strings.Contains(got, "shuttle resume "+WeftAgent) {
		t.Fatalf("operator detail = %q", got)
	}
}

func TestRestartWorkNamesWhatARestartKills(t *testing.T) {
	s := newStore(t)
	work, err := s.RestartWork()
	if err != nil || !work.Empty() {
		t.Fatalf("idle = %+v/%v", work, err)
	}

	scheduled := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	hosted := claimVisible(t, s, "weft/running-0001")
	var terminal *WeftTerminal
	if err := s.WeftTx(func(tx *WeftTx) error {
		var err error
		terminal, err = tx.OpenTerminal(WeftTerminalOpen{Kind: "shell", TargetKind: WeftTargetFolder, TargetID: "h-1", Path: "/tmp", Cols: 80, Rows: 24})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if work, err = s.RestartWork(); err != nil {
		t.Fatal(err)
	}
	named := strings.Join(work.Runs, " ")
	if len(work.Runs) != 2 || !strings.Contains(named, scheduled.ID) || !strings.Contains(named, hosted.Segments[0].LedgerRunID) {
		t.Fatalf("runs = %v", work.Runs)
	}
	if len(work.Terminals) != 1 || !strings.Contains(work.Terminals[0], terminal.ID) {
		t.Fatalf("terminals = %v", work.Terminals)
	}

	// A detached session is as live as an attached one; a closed one is gone.
	if _, err := s.UpdateWeftTerminal(terminal.ID, WeftTerminalState{State: WeftTerminalDetached}); err != nil {
		t.Fatal(err)
	}
	if work, err = s.RestartWork(); err != nil || len(work.Terminals) != 1 {
		t.Fatalf("detached = %+v/%v", work, err)
	}
	if _, err := s.CloseWeftTerminal(terminal.ID, WeftClosedByOperator, nil, WeftTerminalState{}, "", 0); err != nil {
		t.Fatal(err)
	}
	if work, err = s.RestartWork(); err != nil || len(work.Terminals) != 0 {
		t.Fatalf("closed = %+v/%v", work, err)
	}

	// The run's row completes before its email leaves, in the same worker.
	if err := s.SetDelivery(scheduled.ID, DeliveryPending, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(scheduled.ID, Result{}); err != nil {
		t.Fatal(err)
	}
	if work, err = s.RestartWork(); err != nil || !strings.Contains(strings.Join(work.Runs, " "), "delivering") {
		t.Fatalf("delivering = %+v/%v", work, err)
	}
	// A send orphaned by an earlier restart is not coming back, and must not
	// hold every later update.
	s.now = func() time.Time { return testClock.Add(deliveryDrainWindow + time.Minute) }
	if work, err = s.RestartWork(); err != nil || strings.Contains(strings.Join(work.Runs, " "), "delivering") {
		t.Fatalf("orphaned = %+v/%v", work, err)
	}
}

func TestBackupHoldsWhatTheWALDoes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatal(err)
	}
	if err := s.Pause("daily"); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("the write is not in the WAL: %v/%v", fi, err)
	}
	backup := filepath.Join(dir, "backup.db")
	if err := s.Backup(backup); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(backup)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != ledgerMode {
		t.Fatalf("backup mode = %04o", perm)
	}
	restored, err := Open(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if a, err := restored.Agent("daily"); err != nil || !a.Paused {
		t.Fatalf("backup agent = %+v/%v", a, err)
	}
}

func TestBackupRefusesToOverwrite(t *testing.T) {
	s := newStore(t)
	path := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(path, []byte("an earlier backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(path); err == nil {
		t.Fatal("backed up over an existing file")
	}
}

func TestRestoreBackupKeepsWhatItReplaces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(dir, "backup.db")
	if err := s.Backup(backup); err != nil {
		t.Fatal(err)
	}
	if err := s.Pause("written-after-the-backup"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	aside := filepath.Join(dir, "aside.db")
	if err := RestoreBackup(path, backup, aside); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if a, err := restored.Agent("written-after-the-backup"); err != nil || a.Paused {
		t.Fatalf("restored agent = %+v/%v", a, err)
	}
	replaced, err := Open(aside)
	if err != nil {
		t.Fatal(err)
	}
	defer replaced.Close()
	if a, err := replaced.Agent("written-after-the-backup"); err != nil || !a.Paused {
		t.Fatalf("replaced agent = %+v/%v", a, err)
	}
}

// armedStore is a ledger under the restart barrier.
func armedStore(t *testing.T) *Store {
	t.Helper()
	s := newStore(t)
	if claimed, _, err := s.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("claim = %t/%v", claimed, err)
	}
	if err := s.ArmRestartBarrier(time.Hour); err != nil {
		t.Fatal(err)
	}
	return s
}

// Two handles on one file are two processes: only sqlite's write lock orders
// them. Whichever transaction takes it first, the other sees what it wrote.
func TestRestartBarrierIsAtomicWithAClaimAcrossProcesses(t *testing.T) {
	updater := newStore(t)
	var file string
	if err := updater.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&file); err != nil {
		t.Fatal(err)
	}
	daemon, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { daemon.Close() })
	daemon.now = updater.now
	armed, claimed := 0, 0
	for i := range 40 {
		if ok, _, err := updater.ClaimUpdatePause(); err != nil || !ok {
			t.Fatalf("pause = %t/%v", ok, err)
		}
		at := testClock.Add(time.Duration(i) * time.Hour)
		arm, claim := make(chan error, 1), make(chan error, 1)
		var run *Run
		go func() { arm <- updater.ArmRestartBarrier(time.Hour) }()
		go func() {
			var err error
			run, _, err = daemon.ClaimGated("daily", at, TriggerCron, Limits{})
			claim <- err
		}()
		armErr, claimErr := <-arm, <-claim
		switch {
		case armErr == nil && errors.Is(claimErr, ErrRestarting):
			armed++
		case claimErr == nil && armErr != nil && strings.Contains(armErr.Error(), "work appeared"):
			claimed++
			if err := daemon.Complete(run.ID, Result{}); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("round %d: arm = %v, claim = %v", i, armErr, claimErr)
		}
		if _, _, err := updater.ReleaseUpdatePause(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("barrier first %d times, claim first %d times", armed, claimed)
}

func TestRestartBarrierNeedsThePauseAndAnEmptyHost(t *testing.T) {
	t.Run("no update pause", func(t *testing.T) {
		s := newStore(t)
		if err := s.Pause(WeftAgent); err != nil {
			t.Fatal(err)
		}
		if err := s.ArmRestartBarrier(time.Hour); err == nil || !strings.Contains(err.Error(), "no longer stands") {
			t.Fatalf("arm = %v", err)
		}
		if held, err := s.RestartBarrier(); err != nil || held {
			t.Fatalf("barrier = %t/%v", held, err)
		}
	})
	for name, start := range map[string]func(*testing.T, *Store){
		"a claimed run": func(t *testing.T, s *Store) {
			if _, ok, err := s.Claim("daily", testClock, TriggerCron); err != nil || !ok {
				t.Fatalf("claim = %t/%v", ok, err)
			}
		},
		"an open terminal": func(t *testing.T, s *Store) {
			if _, err := openTerminal(t, s, WeftTerminalOpen{Kind: "shell", TargetKind: WeftTargetFolder, TargetID: "h",
				Path: "/tmp", Cols: 80, Rows: 24}); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			if claimed, _, err := s.ClaimUpdatePause(); err != nil || !claimed {
				t.Fatalf("claim = %t/%v", claimed, err)
			}
			start(t, s)
			if err := s.ArmRestartBarrier(time.Hour); err == nil || !strings.Contains(err.Error(), "work appeared") {
				t.Fatalf("arm = %v", err)
			}
			if held, err := s.RestartBarrier(); err != nil || held {
				t.Fatalf("barrier = %t/%v", held, err)
			}
		})
	}
}

// A refusal writes nothing, skipped row included: the slot is still the
// scheduler's to claim once the barrier is down.
func TestRestartBarrierRefusesAClaimAndKeepsItsSlot(t *testing.T) {
	s := armedStore(t)
	for _, kind := range []string{TriggerCron, TriggerManual} {
		if run, reason, err := s.ClaimGated("daily", testClock, kind, Limits{}); !errors.Is(err, ErrRestarting) || run != nil || reason != "" {
			t.Fatalf("%s claim = %+v/%q/%v", kind, run, reason, err)
		}
	}
	if _, _, err := s.Claim("daily", testClock, TriggerCron); !errors.Is(err, ErrRestarting) {
		t.Fatalf("ungated claim = %v", err)
	}
	if runs, err := s.Runs("daily", 10); err != nil || len(runs) != 0 {
		t.Fatalf("runs = %+v/%v", runs, err)
	}
	if lifted, barrier, err := s.ReleaseUpdatePause(); err != nil || !lifted || !barrier {
		t.Fatalf("release = %t/%t/%v", lifted, barrier, err)
	}
	if run, reason, err := s.ClaimGated("daily", testClock, TriggerCron, Limits{}); err != nil || run == nil || reason != "" {
		t.Fatalf("claim after the update = %+v/%q/%v", run, reason, err)
	}
}

func TestRestartBarrierRefusesATerminalNamingTheUpdate(t *testing.T) {
	s := armedStore(t)
	_, err := openTerminal(t, s, WeftTerminalOpen{Kind: "shell", TargetKind: WeftTargetFolder, TargetID: "h",
		Path: "/tmp", Cols: 80, Rows: 24})
	var refusal *WeftRefusal
	if !errors.As(err, &refusal) || refusal.Code != "repository_reserved" || !strings.Contains(refusal.Message, "update") {
		t.Fatalf("open = %v", err)
	}
	if work, err := s.RestartWork(); err != nil || !work.Empty() {
		t.Fatalf("work = %+v/%v", work, err)
	}
}

// Raising the barrier says nothing to a client, and a resume under it moves
// nothing a client reads unless the words change: an operator's pause handed
// back to the update is the one case that does.
func TestResumeUnderTheBarrierHoldsThePauseAndItsIntent(t *testing.T) {
	s := armedStore(t)
	if _, err := s.db.Exec(`UPDATE agents SET failure_streak=3 WHERE name=?`, WeftAgent); err != nil {
		t.Fatal(err)
	}
	updating := hostEvents(t, s)
	if len(updating) != 1 || updating[0][0] != WeftDispatchPaused {
		t.Fatalf("host events = %v", updating)
	}
	if err := s.Resume(WeftAgent); err != nil {
		t.Fatal(err)
	}
	a, err := s.Agent(WeftAgent)
	if err != nil || !a.Paused || a.PausedReason.String != PauseUpdate || a.FailureStreak != 0 {
		t.Fatalf("agent = %+v/%v", a, err)
	}
	if got := hostEvents(t, s); !reflect.DeepEqual(got, updating) {
		t.Fatalf("host events = %v", got)
	}

	if err := s.Pause(WeftAgent); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(WeftAgent); err != nil {
		t.Fatal(err)
	}
	if a, err := s.Agent(WeftAgent); err != nil || !a.Paused || a.PausedReason.String != PauseUpdate {
		t.Fatalf("agent = %+v/%v", a, err)
	}
	got := hostEvents(t, s)
	if len(got) != 3 || got[2] != updating[0] {
		t.Fatalf("host events = %v", got)
	}

	if lifted, _, err := s.ReleaseUpdatePause(); err != nil || !lifted {
		t.Fatalf("release = %t/%v", lifted, err)
	}
	if h, err := s.WeftHost(); err != nil || h.Dispatch != WeftDispatchOpen {
		t.Fatalf("host = %+v/%v", h, err)
	}
}

func TestPauseUnderTheBarrierOutlivesTheUpdate(t *testing.T) {
	s := armedStore(t)
	if err := s.Pause(WeftAgent); err != nil {
		t.Fatal(err)
	}
	if lifted, barrier, err := s.ReleaseUpdatePause(); err != nil || lifted || !barrier {
		t.Fatalf("release = %t/%t/%v", lifted, barrier, err)
	}
	if a, err := s.Agent(WeftAgent); err != nil || !a.Paused || a.PausedReason.String != PauseManual {
		t.Fatalf("agent = %+v/%v", a, err)
	}
	if held, err := s.RestartBarrier(); err != nil || held {
		t.Fatalf("barrier = %t/%v", held, err)
	}
}

// An updater that died with no successor leaves nothing to take the barrier
// down, so it ends by itself and a resume works again.
func TestRestartBarrierLapses(t *testing.T) {
	s := armedStore(t)
	s.now = func() time.Time { return testClock.Add(time.Hour) }
	if held, err := s.RestartBarrier(); err != nil || held {
		t.Fatalf("barrier = %t/%v", held, err)
	}
	if run, _, err := s.ClaimGated("daily", testClock, TriggerCron, Limits{}); err != nil || run == nil {
		t.Fatalf("claim = %+v/%v", run, err)
	}
	if err := s.Resume(WeftAgent); err != nil {
		t.Fatal(err)
	}
	if a, err := s.Agent(WeftAgent); err != nil || a.Paused {
		t.Fatalf("agent = %+v/%v", a, err)
	}
}

// A value only a bad write could leave must not hold anything.
func TestAnUnreadableRestartBarrierHoldsNothing(t *testing.T) {
	s := armedStore(t)
	if _, err := s.db.Exec(`UPDATE weft_settings SET value=? WHERE key=?`, "not a time", restartBarrierKey); err != nil {
		t.Fatal(err)
	}
	if held, err := s.RestartBarrier(); err != nil || held {
		t.Fatalf("barrier = %t/%v", held, err)
	}
}
