package updater

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"shuttle/internal/ledger"
)

const (
	oldVersion = "v1.0.0"
	newTag     = "v1.1.0"
	oldBuild   = "shuttle v1.0.0"
	newBuild   = "shuttle v1.1.0"
	// proxiedAsset resolves nowhere, so only a proxy can answer for it.
	proxiedAsset = "http://releases.invalid/asset"
	proxiedBody  = "via proxy"
)

func versionScript(v string) []byte {
	return []byte("#!/bin/sh\necho " + v + "\n")
}

// archive is a gzip+tar of entries in order; an entry whose data is nil is a
// symlink.
func archive(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Mode: 0o755, Size: int64(len(e.data)), Typeflag: tar.TypeReg}
		if e.data == nil {
			hdr.Typeflag, hdr.Linkname, hdr.Size = tar.TypeSymlink, "/bin/sh", 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type tarEntry struct {
	name string
	data []byte
}

// fakeSource publishes one release from memory.
type fakeSource struct {
	mu        sync.Mutex
	tag       string
	assets    map[string][]byte
	latestErr error
	downloads int
}

func (f *fakeSource) Latest(context.Context) (Release, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.latestErr != nil {
		return Release{}, f.latestErr
	}
	r := Release{Tag: f.tag, Assets: map[string]string{}}
	for name := range f.assets {
		r.Assets[name] = "mem://" + name
	}
	return r, nil
}

func (f *fakeSource) Download(_ context.Context, url string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downloads++
	data, ok := f.assets[strings.TrimPrefix(url, "mem://")]
	if !ok {
		return nil, fmt.Errorf("no asset at %s", url)
	}
	return data, nil
}

// publish replaces the release with tag: tarball and manifest, and a
// checksums.txt covering both.
func (f *fakeSource) publish(tag string, tarball []byte, manifest string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tag = tag
	f.assets = map[string][]byte{assetName(tag): tarball, manifestAsset: []byte(manifest)}
	var sums strings.Builder
	for name, data := range f.assets {
		sum := sha256.Sum256(data)
		fmt.Fprintf(&sums, "%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	f.assets[checksumsAsset] = []byte(sums.String())
}

// fakeDaemon stands in for the launchd jobs and /v2/service. A restart
// reports whatever the binary it was given says it is, as a real daemon does,
// and the hooks let a test fail a step or inspect the host at that moment.
type fakeDaemon struct {
	mu       sync.Mutex
	build    string
	restarts []string
	stops    int
	selves   int

	onBuild   func()
	onRestart func(version string) error
	onStop    func()
	onSelf    func()
	// serving decides what a daemon of this version reports; the default is
	// its own version.
	serving func(version string) string
}

func (d *fakeDaemon) Build(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	d.mu.Lock()
	hook, build := d.onBuild, d.build
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	if build == "" {
		return "", errors.New("connection refused")
	}
	return build, nil
}

func (d *fakeDaemon) Restart(ctx context.Context, bin string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	out, err := exec.Command(bin, "version").Output()
	if err != nil {
		return err
	}
	version := strings.TrimSpace(string(out))
	d.mu.Lock()
	d.restarts = append(d.restarts, version)
	hook, serving := d.onRestart, d.serving
	d.build = ""
	d.mu.Unlock()
	if hook != nil {
		if err := hook(version); err != nil {
			return err
		}
	}
	build := buildPrefix + version
	if serving != nil {
		build = serving(version)
	}
	d.mu.Lock()
	d.build = build
	d.mu.Unlock()
	return nil
}

func (d *fakeDaemon) Stop(context.Context) error {
	d.mu.Lock()
	d.stops++
	d.build = ""
	hook := d.onStop
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (d *fakeDaemon) RestartSelf(string) error {
	d.mu.Lock()
	d.selves++
	hook := d.onSelf
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (d *fakeDaemon) restarted() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.restarts...)
}

// host is a state root, an installed binary, and a ledger handle of its own:
// to the updater it is another process, as the daemon is.
type host struct {
	t      *testing.T
	home   string
	bin    string
	store  *ledger.Store
	source *fakeSource
	daemon *fakeDaemon
	u      *Updater
}

func newHost(t *testing.T) *host {
	t.Helper()
	home := t.TempDir()
	bin := filepath.Join(t.TempDir(), binaryName)
	if err := os.WriteFile(bin, versionScript(oldVersion), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(filepath.Join(home, ledgerName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	logFile, err := OpenLog(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logFile.Close() })
	h := &host{t: t, home: home, bin: bin, store: store, source: &fakeSource{}, daemon: &fakeDaemon{build: oldBuild}}
	h.source.publish(newTag, archive(t, tarEntry{"README.md", []byte("readme")}, tarEntry{binaryName, versionScript(newTag)}),
		`{"min_worker_version":"0.2.0"}`)
	h.u = &Updater{
		Home: home, Bin: bin, Current: oldVersion, Source: h.source, Services: h.daemon,
		WorkerVersion: func() (string, error) { return "weft-worker 0.2.0 (weft@dev)", nil },
		Log:           log.New(logFile, "", log.LstdFlags),
		Interval:      time.Minute, DrainBound: 10 * time.Second, DrainPoll: 5 * time.Millisecond,
		DrainBackoff: time.Hour, ConfirmBound: 200 * time.Millisecond, ConfirmPoll: 5 * time.Millisecond,
	}
	return h
}

func (h *host) binary() string {
	h.t.Helper()
	data, err := os.ReadFile(h.bin)
	if err != nil {
		h.t.Fatal(err)
	}
	return string(data)
}

func (h *host) log() string {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.home, logFile))
	if err != nil {
		h.t.Fatal(err)
	}
	return string(data)
}

func (h *host) wantLog(want string) {
	h.t.Helper()
	if !strings.Contains(h.log(), want) {
		h.t.Fatalf("updater.log lacks %q:\n%s", want, h.log())
	}
}

func (h *host) agent() *ledger.Agent {
	h.t.Helper()
	a, err := h.store.Agent(ledger.WeftAgent)
	if err != nil {
		h.t.Fatal(err)
	}
	return a
}

func (h *host) wantOldBinary() {
	h.t.Helper()
	if got := h.binary(); got != string(versionScript(oldVersion)) {
		h.t.Fatalf("binary = %q, want the previous one", got)
	}
}

func (h *host) wantNewBinary() {
	h.t.Helper()
	if got := h.binary(); got != string(versionScript(newTag)) {
		h.t.Fatalf("binary = %q, want the release", got)
	}
}

func (h *host) wantDispatchOpen() {
	h.t.Helper()
	if a := h.agent(); a.Paused {
		h.t.Fatalf("dispatch left paused (%s)", a.PausedReason.String)
	}
	h.wantBarrier(false)
}

func (h *host) wantBarrier(want bool) {
	h.t.Helper()
	if held, err := h.store.RestartBarrier(); err != nil || held != want {
		h.t.Fatalf("restart barrier = %t/%v, want %t", held, err, want)
	}
}

// hostEvents is every host event the ledger carries, oldest first.
func (h *host) hostEvents() []string {
	h.t.Helper()
	events, err := h.store.WeftEventsAfter(0)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []string
	for _, e := range events {
		if e.Type == "host" {
			out = append(out, string(e.Payload))
		}
	}
	return out
}

// wantUndisturbed fails if dispatch was ever paused.
func (h *host) wantUndisturbed() {
	h.t.Helper()
	if events := h.hostEvents(); len(events) != 0 {
		h.t.Fatalf("dispatch was disturbed: %v", events)
	}
	if got := h.daemon.restarted(); len(got) != 0 {
		h.t.Fatalf("daemon restarted: %v", got)
	}
}

func (h *host) backups() []string {
	h.t.Helper()
	found, err := filepath.Glob(filepath.Join(h.home, backupsDir, ledgerName+".*.pre-*"))
	if err != nil {
		h.t.Fatal(err)
	}
	return found
}

func (h *host) exec(query string, args ...any) {
	h.t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(h.home, ledgerName))
	if err != nil {
		h.t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(query, args...); err != nil {
		h.t.Fatal(err)
	}
}

// scheduled claims a run of a scheduled agent and marks it running under pid.
func (h *host) scheduled(agent string, pid int) *ledger.Run {
	h.t.Helper()
	run, ok, err := h.store.Claim(agent, time.Now(), ledger.TriggerCron)
	if err != nil || !ok {
		h.t.Fatalf("claim %s: %v/%t", agent, err, ok)
	}
	if err := h.store.MarkRunning(run.ID, pid, h.t.TempDir()); err != nil {
		h.t.Fatal(err)
	}
	return run
}

// segment claims a hosted run and marks its first segment running under pid.
func (h *host) segment(ticket string, pid int) *ledger.WeftRun {
	h.t.Helper()
	run, err := h.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticket, Project: "weft", Repo: "/repos/" + ticket, Branch: ticket,
		Worktree: filepath.Join(h.home, "worktrees", ticket), RunDirRoot: filepath.Join(h.home, "runs", ledger.WeftAgent),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.store.MarkWeftRunning(run.ID, 1, pid, "weft-worker/v2"); err != nil {
		h.t.Fatal(err)
	}
	return run
}

func (h *host) openTerminal() string {
	h.t.Helper()
	var id string
	err := h.store.WeftTx(func(tx *ledger.WeftTx) error {
		terminal, err := tx.OpenTerminal(ledger.WeftTerminalOpen{
			Kind: "shell", TargetKind: ledger.WeftTargetFolder, TargetID: "h-1", Path: h.home, Cols: 80, Rows: 24,
		})
		if err != nil {
			return err
		}
		id = terminal.ID
		return nil
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return id
}

// process is a live child standing in for a run's worker: if the updater
// signalled it, exited closes.
func process(t *testing.T) (pid int, exited <-chan struct{}) {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-done
	})
	return cmd.Process.Pid, done
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestUpdaterDrainsBeforeInstall(t *testing.T) {
	t.Run("finished work, then backup, install, confirm, resume", func(t *testing.T) {
		h := newHost(t)
		pid, exited := process(t)
		daily := h.scheduled("daily", pid)
		hosted := h.segment("weft/running-0001", pid)

		var atRestart []string
		h.daemon.onRestart = func(version string) error {
			// The moment that matters: what the host looks like as the
			// daemon goes down.
			a := h.agent()
			if !a.Paused || a.PausedReason.String != ledger.PauseUpdate {
				t.Errorf("restarted with dispatch %t/%s", a.Paused, a.PausedReason.String)
			}
			work, err := h.store.RestartWork()
			if err != nil || !work.Empty() {
				t.Errorf("restarted over %+v/%v", work, err)
			}
			if held, err := h.store.RestartBarrier(); err != nil || !held {
				t.Errorf("restarted with no barrier: %t/%v", held, err)
			}
			if got := len(h.backups()); got != 1 {
				t.Errorf("restarted with %d backups", got)
			}
			h.wantNewBinary()
			atRestart = append(atRestart, version)
			return nil
		}
		resumedFirst := false
		h.daemon.onSelf = func() { resumedFirst = !h.agent().Paused }

		done := make(chan error, 1)
		go func() { done <- h.u.Tick(context.Background()) }()

		waitUntil(t, "the update pause", func() bool { return h.agent().Paused })
		if _, err := h.store.ClaimWeft(ledger.WeftLaunch{Ticket: "weft/late-0003", Project: "weft", Repo: "/repos/late", Branch: "b",
			Worktree: "/w", RunDirRoot: h.home}); !errors.Is(err, ledger.ErrWeftHostPaused) {
			t.Fatalf("dispatch during the drain = %v", err)
		}
		// One of two still running: nothing moves yet.
		if err := h.store.Complete(daily.ID, ledger.Result{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		h.wantOldBinary()
		if got := h.daemon.restarted(); len(got) != 0 {
			t.Fatalf("restarted with a segment running: %v", got)
		}
		select {
		case <-exited:
			t.Fatal("the updater ended a run's process")
		default:
		}

		if err := h.store.RecordWeftEnded(hosted.ID, 1, "finished", false, "", "", "", "", "", "head", nil); err != nil {
			t.Fatal(err)
		}
		if err := h.store.CompleteWeftSegment(hosted.ID, 1); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("tick: %v\n%s", err, h.log())
		}

		select {
		case <-exited:
			t.Fatal("the updater ended a run's process")
		default:
		}
		if !reflect.DeepEqual(atRestart, []string{newTag}) {
			t.Fatalf("restarts = %v", atRestart)
		}
		h.wantNewBinary()
		if h.daemon.build != newBuild {
			t.Fatalf("serving %q", h.daemon.build)
		}
		h.wantDispatchOpen()
		if !resumedFirst || h.daemon.selves != 1 {
			t.Fatalf("self restart: resumed first %t, %d restarts", resumedFirst, h.daemon.selves)
		}
		events := h.hostEvents()
		if len(events) != 2 || !strings.Contains(events[0], `"dispatch":"paused"`) || !strings.Contains(events[1], `"dispatch":"open"`) {
			t.Fatalf("host events = %v", events)
		}
		// The ledger as it stood after the drain, every row finished.
		backup, err := ledger.Open(h.backups()[0])
		if err != nil {
			t.Fatal(err)
		}
		defer backup.Close()
		if row, err := backup.Get(hosted.Segments[0].LedgerRunID); err != nil || row.Status != ledger.StatusCompleted {
			t.Fatalf("backup = %+v/%v", row, err)
		}
		if data, err := os.ReadFile(h.bin + previousSuffix); err != nil || string(data) != string(versionScript(oldVersion)) {
			t.Fatalf("previous binary = %q/%v", data, err)
		}
		for _, want := range []string{"step 1", "step 2", "step 3", "step 4", "step 5", "step 6", "step 7", "step 8", "waiting on:"} {
			h.wantLog(want)
		}
	})

	t.Run("a run past the bound leaves the old binary and dispatch resumed", func(t *testing.T) {
		h := newHost(t)
		h.u.DrainBound = 30 * time.Millisecond
		pid, exited := process(t)
		run := h.scheduled("daily", pid)
		if err := h.u.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "drain bound") {
			t.Fatalf("tick = %v", err)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
		if got := h.daemon.restarted(); len(got) != 0 {
			t.Fatalf("restarted: %v", got)
		}
		if len(h.backups()) != 0 {
			t.Fatal("backed up a ledger for an update that did not happen")
		}
		h.wantLog("drain bound")
		select {
		case <-exited:
			t.Fatal("the updater ended a run's process")
		default:
		}
		if got, err := h.store.Get(run.ID); err != nil || got.Status != ledger.StatusRunning {
			t.Fatalf("run = %+v/%v", got, err)
		}
		// Dispatch stays open through the backoff, and the attempt after it
		// goes through once the run has finished.
		events := len(h.hostEvents())
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := len(h.hostEvents()); got != events {
			t.Fatalf("paused during the backoff: %d events, had %d", got, events)
		}
		h.wantLog("backing off")
		if err := h.store.Complete(run.ID, ledger.Result{}); err != nil {
			t.Fatal(err)
		}
		h.u.retryAt = time.Time{}
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		h.wantNewBinary()
	})

	t.Run("a segment past the bound is left running", func(t *testing.T) {
		h := newHost(t)
		h.u.DrainBound = 30 * time.Millisecond
		pid, _ := process(t)
		h.segment("weft/long-0001", pid)
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick installed over a running segment")
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
	})

	t.Run("work that starts after the drain stops the attempt at the barrier", func(t *testing.T) {
		h := newHost(t)
		pid, _ := process(t)
		original := beforeBarrier
		t.Cleanup(func() { beforeBarrier = original })
		beforeBarrier = func() { h.scheduled("daily", pid) }
		if err := h.u.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "work appeared") {
			t.Fatalf("tick = %v", err)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
		if got := h.daemon.restarted(); len(got) != 0 {
			t.Fatalf("restarted: %v", got)
		}
		if got := h.backups(); len(got) != 0 {
			t.Fatalf("backed up: %v", got)
		}
	})

	t.Run("nothing starts between the final check and the restart", func(t *testing.T) {
		h := newHost(t)
		original := beforeInstall
		t.Cleanup(func() { beforeInstall = original })
		attempted := false
		beforeInstall = func() {
			attempted = true
			for _, kind := range []string{ledger.TriggerCron, ledger.TriggerManual} {
				if _, _, err := h.store.ClaimGated("daily", time.Now(), kind, ledger.Limits{}); !errors.Is(err, ledger.ErrRestarting) {
					t.Errorf("%s claim = %v", kind, err)
				}
			}
			// The operator's resume is what the barrier has to outlast.
			if err := h.store.Resume(ledger.WeftAgent); err != nil {
				t.Error(err)
			}
			launch := ledger.WeftLaunch{Ticket: "weft/late-0001", Project: "weft", Repo: "/repos/late", Branch: "b",
				Worktree: "/w", RunDirRoot: h.home}
			if _, err := h.store.ClaimWeft(launch); !errors.Is(err, ledger.ErrWeftHostPaused) {
				t.Errorf("hosted claim = %v", err)
			}
			err := h.store.WeftTx(func(tx *ledger.WeftTx) error {
				_, err := tx.OpenTerminal(ledger.WeftTerminalOpen{Kind: "shell", TargetKind: ledger.WeftTargetFolder,
					TargetID: "h-1", Path: h.home, Cols: 80, Rows: 24})
				return err
			})
			var refusal *ledger.WeftRefusal
			if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "update") {
				t.Errorf("terminal = %v", err)
			}
		}
		h.daemon.onRestart = func(string) error {
			if work, err := h.store.RestartWork(); err != nil || !work.Empty() {
				t.Errorf("restarted over %+v/%v", work, err)
			}
			return nil
		}
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatalf("tick: %v\n%s", err, h.log())
		}
		if !attempted {
			t.Fatal("nothing attempted admission")
		}
		h.wantNewBinary()
		h.wantDispatchOpen()
		if run, _, err := h.store.ClaimGated("daily", time.Now(), ledger.TriggerCron, ledger.Limits{}); err != nil || run == nil {
			t.Fatalf("claim after the update = %+v/%v", run, err)
		}
		if _, err := h.store.ClaimWeft(ledger.WeftLaunch{Ticket: "weft/late-0001", Project: "weft", Repo: "/repos/late", Branch: "b",
			Worktree: "/w", RunDirRoot: h.home}); err != nil {
			t.Fatalf("hosted claim after the update = %v", err)
		}
	})

	t.Run("an operator's pause under the barrier stands after the update", func(t *testing.T) {
		h := newHost(t)
		original := beforeInstall
		t.Cleanup(func() { beforeInstall = original })
		beforeInstall = func() {
			if err := h.store.Pause(ledger.WeftAgent); err != nil {
				t.Error(err)
			}
		}
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatalf("tick: %v\n%s", err, h.log())
		}
		h.wantNewBinary()
		h.wantBarrier(false)
		if a := h.agent(); !a.Paused || a.PausedReason.String != ledger.PauseManual {
			t.Fatalf("agent = %+v", a)
		}
	})

	t.Run("an email its run is still sending holds the restart", func(t *testing.T) {
		h := newHost(t)
		h.u.DrainBound = 30 * time.Millisecond
		run := h.scheduled("daily", 1)
		if err := h.store.SetDelivery(run.ID, ledger.DeliveryPending, ""); err != nil {
			t.Fatal(err)
		}
		if err := h.store.Complete(run.ID, ledger.Result{}); err != nil {
			t.Fatal(err)
		}
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick installed over a delivery")
		}
		h.wantOldBinary()
	})

	t.Run("an open terminal rules the poll out with dispatch untouched", func(t *testing.T) {
		h := newHost(t)
		id := h.openTerminal()
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		h.wantOldBinary()
		h.wantUndisturbed()
		h.wantLog(id)
		if h.source.downloads != 0 {
			t.Fatalf("downloaded %d assets for a release it could not install", h.source.downloads)
		}
	})

	t.Run("a terminal opened during the drain ends the attempt at once", func(t *testing.T) {
		h := newHost(t)
		h.u.DrainBound = time.Minute
		pid, _ := process(t)
		h.scheduled("daily", pid)
		done := make(chan error, 1)
		go func() { done <- h.u.Tick(context.Background()) }()
		waitUntil(t, "the update pause", func() bool { return h.agent().Paused })
		h.openTerminal()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "open terminals") {
				t.Fatalf("tick = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the attempt kept holding dispatch for a terminal")
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
	})

	for _, standing := range []struct {
		name, reason string
		failures     int
	}{
		{"an operator's pause", ledger.PauseManual, 0},
		{"an auth pause", ledger.PauseAuth, 0},
		{"a streak pause", ledger.PauseStreak, 5},
	} {
		t.Run(standing.name+" is neither taken over nor lifted", func(t *testing.T) {
			h := newHost(t)
			h.exec(`INSERT INTO agents(name,paused,paused_reason,paused_at,failure_streak) VALUES(?,1,?,'2026-11-02T00:00:00Z',?)`,
				ledger.WeftAgent, standing.reason, standing.failures)
			if err := h.u.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			a := h.agent()
			if !a.Paused || a.PausedReason.String != standing.reason || a.PausedAt.String != "2026-11-02T00:00:00Z" ||
				a.FailureStreak != standing.failures {
				t.Fatalf("agent = %+v", a)
			}
			h.wantOldBinary()
			h.wantUndisturbed()
			h.wantLog("already paused (" + standing.reason + ")")
		})
	}

	t.Run("a pause an operator sets mid-drain outlives the attempt", func(t *testing.T) {
		h := newHost(t)
		pid, _ := process(t)
		run := h.scheduled("daily", pid)
		done := make(chan error, 1)
		go func() { done <- h.u.Tick(context.Background()) }()
		waitUntil(t, "the update pause", func() bool { return h.agent().Paused })
		if err := h.store.Pause(ledger.WeftAgent); err != nil {
			t.Fatal(err)
		}
		if err := h.store.Complete(run.ID, ledger.Result{}); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err == nil || !strings.Contains(err.Error(), "no longer stands") {
			t.Fatalf("tick = %v", err)
		}
		if a := h.agent(); !a.Paused || a.PausedReason.String != ledger.PauseManual {
			t.Fatalf("agent = %+v", a)
		}
		h.wantOldBinary()
	})

	t.Run("a resume mid-drain abandons the update", func(t *testing.T) {
		h := newHost(t)
		pid, _ := process(t)
		h.scheduled("daily", pid)
		done := make(chan error, 1)
		go func() { done <- h.u.Tick(context.Background()) }()
		waitUntil(t, "the update pause", func() bool { return h.agent().Paused })
		if err := h.store.Resume(ledger.WeftAgent); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err == nil || !strings.Contains(err.Error(), "no longer stands") {
			t.Fatalf("tick = %v", err)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
	})

	t.Run("an idle host with no ledger row for the agent updates", func(t *testing.T) {
		h := newHost(t)
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatalf("tick: %v\n%s", err, h.log())
		}
		h.wantNewBinary()
		h.wantDispatchOpen()
	})

	t.Run("lifting the update pause leaves the failure streak", func(t *testing.T) {
		h := newHost(t)
		h.exec(`INSERT INTO agents(name,failure_streak) VALUES(?,3)`, ledger.WeftAgent)
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatalf("tick: %v\n%s", err, h.log())
		}
		h.wantDispatchOpen()
		if a := h.agent(); a.FailureStreak != 3 {
			t.Fatalf("streak = %d", a.FailureStreak)
		}
	})

	t.Run("a stopped updater lifts its pause on the way out", func(t *testing.T) {
		h := newHost(t)
		pid, _ := process(t)
		h.scheduled("daily", pid)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- h.u.Tick(ctx) }()
		waitUntil(t, "the update pause", func() bool { return h.agent().Paused })
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("tick = %v", err)
		}
		h.wantDispatchOpen()
		h.wantOldBinary()
	})

	t.Run("a stop that arrives once the install is reached does not leave it half done", func(t *testing.T) {
		h := newHost(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		original := beforeInstall
		t.Cleanup(func() { beforeInstall = original })
		beforeInstall = cancel
		if err := h.u.Tick(ctx); err != nil {
			t.Fatalf("tick: %v\n%s", err, h.log())
		}
		h.wantNewBinary()
		h.wantDispatchOpen()
		if h.daemon.build != newBuild {
			t.Fatalf("serving %q", h.daemon.build)
		}
	})

	t.Run("a crashed updater's pause is recovered, even with GitHub down", func(t *testing.T) {
		h := newHost(t)
		if claimed, _, err := h.store.ClaimUpdatePause(); err != nil || !claimed {
			t.Fatalf("claim = %t/%v", claimed, err)
		}
		h.exec(`UPDATE agents SET failure_streak=2 WHERE name=?`, ledger.WeftAgent)
		staged := filepath.Join(filepath.Dir(h.bin), ".shuttle-update-left")
		if err := os.WriteFile(staged, []byte("x"), 0o755); err != nil {
			t.Fatal(err)
		}
		h.source.latestErr = errors.New("502")
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick with no release source succeeded")
		}
		h.wantDispatchOpen()
		h.wantLog("did not finish")
		if a := h.agent(); a.FailureStreak != 2 {
			t.Fatalf("streak = %d", a.FailureStreak)
		}
		if _, err := os.Stat(staged); !os.IsNotExist(err) {
			t.Fatalf("staged binary left behind: %v", err)
		}
	})

	t.Run("a crashed updater's barrier is recovered, even with GitHub down", func(t *testing.T) {
		h := newHost(t)
		if claimed, _, err := h.store.ClaimUpdatePause(); err != nil || !claimed {
			t.Fatalf("claim = %t/%v", claimed, err)
		}
		if err := h.store.ArmRestartBarrier(restartLease); err != nil {
			t.Fatal(err)
		}
		h.source.latestErr = errors.New("502")
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick with no release source succeeded")
		}
		h.wantDispatchOpen()
		h.wantLog("restart barrier left by an updater")
	})

	t.Run("a crashed updater's barrier goes even under an operator's pause", func(t *testing.T) {
		h := newHost(t)
		if claimed, _, err := h.store.ClaimUpdatePause(); err != nil || !claimed {
			t.Fatalf("claim = %t/%v", claimed, err)
		}
		if err := h.store.ArmRestartBarrier(restartLease); err != nil {
			t.Fatal(err)
		}
		if err := h.store.Pause(ledger.WeftAgent); err != nil {
			t.Fatal(err)
		}
		h.source.latestErr = errors.New("502")
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick with no release source succeeded")
		}
		h.wantBarrier(false)
		if a := h.agent(); !a.Paused || a.PausedReason.String != ledger.PauseManual {
			t.Fatalf("agent = %+v", a)
		}
	})

	t.Run("a second updater leaves a running cycle's pause alone", func(t *testing.T) {
		h := newHost(t)
		pid, _ := process(t)
		run := h.scheduled("daily", pid)
		done := make(chan error, 1)
		go func() { done <- h.u.Tick(context.Background()) }()
		waitUntil(t, "the update pause", func() bool { return h.agent().Paused })
		second := *h.u
		if err := second.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !h.agent().Paused {
			t.Fatal("a second updater lifted a live cycle's pause as stale")
		}
		h.wantLog("another updater holds")
		if err := h.store.Complete(run.ID, ledger.Result{}); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		h.wantNewBinary()
	})
}

func TestUpdaterFailsBackToRunningBinary(t *testing.T) {
	t.Run("a checksum mismatch never reaches dispatch", func(t *testing.T) {
		h := newHost(t)
		h.source.assets[assetName(newTag)] = archive(t, tarEntry{binaryName, versionScript("v6.6.6")})
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick accepted a tarball its checksum refuses")
		}
		h.wantOldBinary()
		h.wantUndisturbed()
		h.wantLog("sha256 mismatch")
		// Refused now, and held to the same check at the next poll.
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("second tick accepted it")
		}
	})

	t.Run("a failed install leaves the old binary running and dispatch resumed", func(t *testing.T) {
		h := newHost(t)
		dir := filepath.Dir(h.bin)
		t.Cleanup(func() { os.Chmod(dir, 0o755) })
		// The staging succeeded and the directory then stops taking writes,
		// which is what a full or remounted volume does to the install.
		h.daemon.onBuild = func() {
			if err := os.Chmod(dir, 0o555); err != nil {
				t.Error(err)
			}
		}
		if err := h.u.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "install:") {
			t.Fatalf("tick = %v", err)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
		if got := h.daemon.restarted(); len(got) != 0 {
			t.Fatalf("restarted after a failed install: %v", got)
		}
		h.wantLog("install:")
		if len(h.hostEvents()) != 2 {
			t.Fatalf("host events = %v", h.hostEvents())
		}
	})

	t.Run("a restart that fails restores the previous binary", func(t *testing.T) {
		h := newHost(t)
		h.daemon.onRestart = func(version string) error {
			if version == newTag {
				return errors.New("launchctl bootstrap: Input/output error")
			}
			return nil
		}
		err := h.u.Tick(context.Background())
		if err == nil || !strings.Contains(err.Error(), "previous build is running") {
			t.Fatalf("tick = %v", err)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
		if got := h.daemon.restarted(); !reflect.DeepEqual(got, []string{newTag, oldVersion}) {
			t.Fatalf("restarts = %v", got)
		}
		if h.daemon.build != oldBuild {
			t.Fatalf("serving %q", h.daemon.build)
		}
		if h.daemon.selves != 0 {
			t.Fatal("handed the updater's job to a binary that was rolled back")
		}
		h.wantLog("launchctl bootstrap: Input/output error")
		h.wantLog(`back on "` + oldBuild + `"`)
	})

	t.Run("the barrier holds through the fall-back's restart", func(t *testing.T) {
		h := newHost(t)
		h.daemon.serving = func(version string) string {
			if version == newTag {
				return "" // up, but never confirmed: a build that can claim work
			}
			return buildPrefix + version
		}
		restarts := 0
		h.daemon.onRestart = func(string) error {
			restarts++
			if _, _, err := h.store.ClaimGated("daily", time.Now(), ledger.TriggerCron, ledger.Limits{}); !errors.Is(err, ledger.ErrRestarting) {
				t.Errorf("claim at restart %d = %v", restarts, err)
			}
			return nil
		}
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick confirmed nothing")
		}
		if restarts != 2 {
			t.Fatalf("restarts = %d", restarts)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
	})

	t.Run("a new build that never reports restores the previous binary", func(t *testing.T) {
		h := newHost(t)
		h.daemon.serving = func(version string) string {
			if version == newTag {
				return oldBuild // launchd came up on the old image
			}
			return buildPrefix + version
		}
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick confirmed a build that was not the new one")
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
		if h.daemon.build != oldBuild {
			t.Fatalf("serving %q", h.daemon.build)
		}
		h.wantLog(`still reports "` + oldBuild + `"`)
	})

	t.Run("a build that migrated the ledger hands back the pre-update one", func(t *testing.T) {
		h := newHost(t)
		schema, err := h.store.SchemaVersion()
		if err != nil {
			t.Fatal(err)
		}
		const marker = "2026-11-07T00:00:00Z"
		h.daemon.serving = func(version string) string {
			if version != newTag {
				return buildPrefix + version
			}
			// The new build ran its migration and then did not come up.
			h.exec(`UPDATE schema_meta SET value=? WHERE key='schema_version'`, fmt.Sprint(schema+1))
			h.exec(`INSERT INTO agents(name,last_fired_slot) VALUES('after-the-backup',?)`, marker)
			return ""
		}
		// The test's own handle goes with the daemon: nothing else may hold a
		// ledger that is being replaced.
		h.daemon.onStop = func() { h.store.Close() }
		h.daemon.onRestart = func(version string) error {
			if version == newTag {
				return nil
			}
			reopened, err := ledger.Open(filepath.Join(h.home, ledgerName))
			if err != nil {
				return fmt.Errorf("the previous build cannot open the ledger: %w", err)
			}
			defer reopened.Close()
			// The backup was taken under the barrier, so the restored ledger
			// holds new work through this restart as the replaced one did.
			if held, err := reopened.RestartBarrier(); err != nil || !held {
				t.Errorf("restored ledger's barrier = %t/%v", held, err)
			}
			return nil
		}
		if err := h.u.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "previous build is running") {
			t.Fatalf("tick = %v\n%s", err, h.log())
		}
		h.wantOldBinary()
		if h.daemon.stops != 1 {
			t.Fatalf("stops = %d: the ledger was replaced with the daemon up", h.daemon.stops)
		}
		reopened, err := ledger.Open(filepath.Join(h.home, ledgerName))
		if err != nil {
			t.Fatalf("the restored ledger: %v", err)
		}
		h.store = reopened
		t.Cleanup(func() { reopened.Close() })
		h.wantDispatchOpen()
		if got, err := reopened.SchemaVersion(); err != nil || got != schema {
			t.Fatalf("schema = %d/%v, want %d", got, err, schema)
		}
		if a, err := reopened.Agent("after-the-backup"); err != nil || a.LastFiredSlot.Valid {
			t.Fatalf("the restored ledger kept a row from after the backup: %+v/%v", a, err)
		}
		aside, err := filepath.Glob(filepath.Join(h.home, backupsDir, ledgerName+".*.failed-"+newTag))
		if err != nil || len(aside) != 1 {
			t.Fatalf("replaced ledger = %v/%v", aside, err)
		}
		db, err := sql.Open("sqlite", aside[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var kept string
		if err := db.QueryRow(`SELECT last_fired_slot FROM agents WHERE name='after-the-backup'`).Scan(&kept); err != nil || kept != marker {
			t.Fatalf("the replaced ledger's row = %q/%v", kept, err)
		}
		h.wantLog("restored the ledger from")
	})

	t.Run("a fall-back that fails still lifts the pause and says so", func(t *testing.T) {
		h := newHost(t)
		h.daemon.onRestart = func(string) error { return errors.New("launchd refused") }
		err := h.u.Tick(context.Background())
		if err == nil || !strings.Contains(err.Error(), "needs an operator") {
			t.Fatalf("tick = %v", err)
		}
		h.wantOldBinary()
		h.wantDispatchOpen()
		h.wantLog("needs an operator")
	})

	t.Run("a release that fell back is not retried, and a newer one is", func(t *testing.T) {
		h := newHost(t)
		h.daemon.serving = func(version string) string {
			if version == newTag {
				return ""
			}
			return buildPrefix + version
		}
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick confirmed nothing")
		}
		restarts := len(h.daemon.restarted())
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := len(h.daemon.restarted()); got != restarts {
			t.Fatalf("restarted again for the same release: %d", got)
		}
		h.wantLog("fell back after its install")
		h.daemon.serving = nil
		fixed := "v1.1.1"
		h.source.publish(fixed, archive(t, tarEntry{binaryName, versionScript(fixed)}), `{"min_worker_version":"0.2.0"}`)
		if err := h.u.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := h.binary(); got != string(versionScript(fixed)) {
			t.Fatalf("binary = %q", got)
		}
	})

	t.Run("a daemon that cannot be read refuses before pausing", func(t *testing.T) {
		h := newHost(t)
		h.daemon.build = ""
		if err := h.u.Tick(context.Background()); err == nil {
			t.Fatal("tick went ahead with nothing to confirm against")
		}
		h.wantOldBinary()
		h.wantUndisturbed()
	})
}

// Everything that can refuse a release does so before dispatch moves.
func TestUpdaterRefusesBeforePausing(t *testing.T) {
	manifest := `{"min_worker_version":"0.2.0"}`
	good := func(t *testing.T) []byte { return archive(t, tarEntry{binaryName, versionScript(newTag)}) }
	for _, tc := range []struct {
		name     string
		tarball  func(*testing.T) []byte
		manifest string
		worker   func() (string, error)
		want     string
	}{
		{name: "a newer worker required", manifest: `{"min_worker_version":"0.3.0"}`, want: "needs worker 0.3.0"},
		{name: "a worker that will not probe", worker: func() (string, error) { return "", errors.New("exited 64") }, want: "exited 64"},
		{name: "a worker with no semver", worker: func() (string, error) { return "weft-worker dev", nil }, want: "no semver"},
		{name: "a manifest with no minimum", manifest: `{}`, want: "min_worker_version"},
		{name: "a manifest that is not JSON", manifest: `not json`, want: manifestAsset},
		{name: "a binary stamped with another version", want: "not release",
			tarball: func(t *testing.T) []byte { return archive(t, tarEntry{binaryName, versionScript("v9.9.9")}) }},
		{name: "a binary that will not run", want: "run staged binary",
			tarball: func(t *testing.T) []byte { return archive(t, tarEntry{binaryName, []byte("\x00not an executable")}) }},
		{name: "a path traversal in the archive", want: "leaves the archive",
			tarball: func(t *testing.T) []byte {
				return archive(t, tarEntry{"../../evil", []byte("x")}, tarEntry{binaryName, versionScript(newTag)})
			}},
		{name: "an archive with no binary", want: "no shuttle binary",
			tarball: func(t *testing.T) []byte { return archive(t, tarEntry{"README.md", []byte("x")}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHost(t)
			tarball := good(t)
			if tc.tarball != nil {
				tarball = tc.tarball(t)
			}
			published := manifest
			if tc.manifest != "" {
				published = tc.manifest
			}
			h.source.publish(newTag, tarball, published)
			if tc.worker != nil {
				h.u.WorkerVersion = tc.worker
			}
			err := h.u.Tick(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("tick = %v, want %q", err, tc.want)
			}
			h.wantOldBinary()
			h.wantUndisturbed()
			h.wantLog(tc.want)
			entries, err := filepath.Glob(filepath.Join(filepath.Dir(h.bin), stagedPattern))
			if err != nil || len(entries) != 0 {
				t.Fatalf("left staged: %v/%v", entries, err)
			}
		})
	}

	t.Run("a manifest outside checksums.txt", func(t *testing.T) {
		h := newHost(t)
		h.source.assets[manifestAsset] = []byte(`{"min_worker_version":"0.1.0"}`)
		if err := h.u.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
			t.Fatalf("tick = %v", err)
		}
		h.wantUndisturbed()
	})
}

func TestExtractBinaryTakesOnlyTheBinary(t *testing.T) {
	binary := []byte("the binary")
	got, err := extractBinary(archive(t, tarEntry{"README.md", []byte("x")}, tarEntry{"./" + binaryName, binary}))
	if err != nil || !bytes.Equal(got, binary) {
		t.Fatalf("extract = %q/%v", got, err)
	}
	for name, tarball := range map[string][]byte{
		"traversal": archive(t, tarEntry{"a/../../" + binaryName, []byte("x")}, tarEntry{binaryName, binary}),
		"absolute":  archive(t, tarEntry{"/etc/" + binaryName, []byte("x")}, tarEntry{binaryName, binary}),
		"symlink":   archive(t, tarEntry{binaryName, nil}),
		"twice":     archive(t, tarEntry{binaryName, binary}, tarEntry{binaryName, binary}),
		"nested":    archive(t, tarEntry{"sub/" + binaryName, binary}),
	} {
		if got, err := extractBinary(tarball); err == nil {
			t.Errorf("%s: extracted %q", name, got)
		}
	}
}

// loom's behaviour for a dev build holds: it always updates, which is how the
// first release reaches a host running `make install` output. A release only
// moves forward, because a lower one could not open a migrated ledger.
func TestWanted(t *testing.T) {
	for _, tc := range []struct {
		current, tag string
		want         bool
	}{
		{"dev", "v1.0.0", true},
		{"dev", "v0.0.1", true},
		{"not-a-version", "v1.0.0", true},
		{"v1.0.0", "v1.0.1", true},
		{"1.0.0", "v1.0.1", true},
		{"v1.9.0", "v1.10.0", true},
		{"v1.0.0", "v1.0.0", false},
		{"1.0.0", "v1.0.0", false},
		{"v1.1.0", "v1.0.9", false},
	} {
		got, err := wanted(tc.current, tc.tag)
		if err != nil || got != tc.want {
			t.Errorf("wanted(%q, %q) = %t/%v", tc.current, tc.tag, got, err)
		}
	}
	if _, err := wanted("dev", "nightly"); err == nil {
		t.Error("accepted a tag that is not a semver")
	}
}

func TestDevBuildUpdatesToTheLatestRelease(t *testing.T) {
	h := newHost(t)
	h.u.Current = "dev"
	h.daemon.build = "shuttle dev (1ecc88f)"
	if err := h.u.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v\n%s", err, h.log())
	}
	h.wantNewBinary()
	if h.daemon.build != newBuild {
		t.Fatalf("serving %q", h.daemon.build)
	}
}

func TestBackupsKeepTheNewest(t *testing.T) {
	h := newHost(t)
	dir := filepath.Join(h.home, backupsDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"20260101T000000.000Z.pre-v0.1.0", "20260102T000000.000Z.pre-v0.2.0", "20260103T000000.000Z.pre-v0.3.0"} {
		if err := os.WriteFile(filepath.Join(dir, ledgerName+"."+name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.u.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v\n%s", err, h.log())
	}
	kept := h.backups()
	if len(kept) != backupsKept || !strings.HasSuffix(kept[0], "pre-v0.2.0") || !strings.HasSuffix(kept[2], "pre-"+newTag) {
		t.Fatalf("backups = %v", kept)
	}
}

func TestReadBuildSendsTheBearerAndReadsHostBuild(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/service" || r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprint(w, `{"contract_version":"weft-service/v2","service":{"host_build":"shuttle v1.1.0"}}`)
	}))
	defer server.Close()
	if got, err := readBuild(context.Background(), server.URL, "secret"); err != nil || got != newBuild {
		t.Fatalf("build = %q/%v", got, err)
	}
	_, err := readBuild(context.Background(), server.URL, "wrong")
	if err == nil {
		t.Fatal("read a build with the wrong token")
	}
	if strings.Contains(err.Error(), "wrong") {
		t.Fatalf("error carries the token: %v", err)
	}
}

// The token goes over plain HTTP, so an address the daemon would refuse to bind
// gets neither the credential lookup nor a request. localhost reaches the test
// server, so only the refusal keeps the request from arriving.
func TestBuildRefusesAListenerTheDaemonWouldBeforeTheToken(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		fmt.Fprintf(w, `{"service":{"host_build":%q}}`, newBuild)
	}))
	t.Cleanup(server.Close)
	address := strings.TrimPrefix(server.URL, "http://")
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	lookups := 0
	token := func() (string, error) {
		lookups++
		return "secret", nil
	}
	for _, listen := range []string{"localhost:" + port, "203.0.113.7:" + port, "studio.example.com:" + port, "nonsense"} {
		if _, err := buildAt(context.Background(), listen, token); err == nil || !strings.Contains(err.Error(), "weft.listen") {
			t.Fatalf("%s: build = %v", listen, err)
		}
	}
	if lookups != 0 || requests != 0 {
		t.Fatalf("lookups = %d, requests = %d", lookups, requests)
	}
	if got, err := buildAt(context.Background(), address, token); err != nil || got != newBuild {
		t.Fatalf("build at %s = %q/%v", address, got, err)
	}
	if lookups != 1 || requests != 1 {
		t.Fatalf("lookups = %d, requests = %d", lookups, requests)
	}
}

// net/http reads the proxy environment once per process and any test in this
// package could be first, so the variables go to a helper process. The asset it
// fetches proves the proxy was live there: the only request the proxy may see.
func TestBuildBypassesAnInheritedProxy(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, r.URL.String()+" "+r.Header.Get("Authorization"))
		fmt.Fprint(w, proxiedBody)
	}))
	defer proxy.Close()
	helper := exec.Command(os.Args[0], "-test.run=^TestInheritedProxyHelper$", "--")
	helper.Env = append(os.Environ(), "HTTP_PROXY="+proxy.URL, "NO_PROXY=", "no_proxy=")
	out, err := helper.CombinedOutput()
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		t.Fatalf("helper: %v, proxy saw %q\n%s", err, seen, out)
	}
	if want := []string{proxiedAsset + " "}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("proxy saw %q, want %q", seen, want)
	}
}

// TestInheritedProxyHelper runs under TestBuildBypassesAnInheritedProxy. The
// dialer stands in for the tailnet, so the listener is reached only by dialing
// it directly and nothing leaves the machine.
func TestInheritedProxyHelper(t *testing.T) {
	if !slices.Contains(os.Args, "--") {
		return
	}
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprintf(w, `{"service":{"host_build":%q}}`, newBuild)
	}))
	defer daemon.Close()
	const listen = "100.64.0.1:8080"
	serviceClient.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == listen {
			address = daemon.Listener.Addr().String()
		}
		return new(net.Dialer).DialContext(ctx, network, address)
	}
	token := func() (string, error) { return "secret", nil }
	if got, err := buildAt(context.Background(), listen, token); err != nil || got != newBuild {
		t.Fatalf("build = %q/%v", got, err)
	}
	data, err := githubSource{}.Download(context.Background(), proxiedAsset)
	if err != nil || string(data) != proxiedBody {
		t.Fatalf("download = %q/%v", data, err)
	}
}

func TestGitHubSourceReadsTheLatestRelease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("sent a credential to %s", r.URL.Path)
		}
		switch r.URL.Path {
		case "/repos/EnderRealm/shuttle/releases/latest":
			fmt.Fprintf(w, `{"tag_name":"v1.1.0","assets":[{"name":"checksums.txt","browser_download_url":"http://%s/asset"}]}`, r.Host)
		case "/asset":
			fmt.Fprint(w, "sums")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	original := githubAPIBase
	githubAPIBase = server.URL
	t.Cleanup(func() { githubAPIBase = original })
	source := githubSource{}
	release, err := source.Latest(context.Background())
	if err != nil || release.Tag != newTag {
		t.Fatalf("latest = %+v/%v", release, err)
	}
	data, err := source.Download(context.Background(), release.Assets[checksumsAsset])
	if err != nil || string(data) != "sums" {
		t.Fatalf("download = %q/%v", data, err)
	}
}

// The manifest a release publishes is the one the updater reads.
func TestReleaseManifestParses(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "release", manifestAsset))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseManifest(data); err != nil {
		t.Fatal(err)
	}
}

// A long drain must not leave a tick queued to fire the moment it ends: the
// wait starts over, so dispatch gets its interval.
func TestDaemonWaitsAFullIntervalAfterACycle(t *testing.T) {
	h := newHost(t)
	h.source.latestErr = errors.New("502")
	h.u.Interval = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := h.u.Daemon(ctx); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(h.log(), "cycle ended"); got != 1 {
		t.Fatalf("%d cycles inside one interval", got)
	}
}
