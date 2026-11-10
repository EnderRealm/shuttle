package updater

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
	"shuttle/internal/ledger"
	"shuttle/internal/version"
)

const (
	logFile  = "updater.log"
	lockFile = "updater.lock"
	// backupsDir sits under the state root and outside runs/, so nothing that
	// prunes run directories reaches it.
	backupsDir = "backups"
	// backupsKept is how many pre-update ledgers stay: enough to step back
	// across a run of bad releases, no more, since each is a full ledger.
	backupsKept = 3
	// previousSuffix names the binary an install replaced, beside it.
	previousSuffix = ".previous"
	// ledgerName is the file config.LedgerPath resolves under the state root.
	ledgerName = "runs.db"

	// defaultDrainBound is how long dispatch stays paused for running work to
	// finish. Pausing costs every queued entry that long, so it is a good deal
	// less than weft.segment_timeout_seconds; a longer segment keeps running
	// across the backoff and is drained on a later attempt.
	defaultDrainBound = 2 * time.Hour
	// defaultDrainBackoff is how long dispatch stays open after the bound. It
	// outlasts the bound because the work that ran it out may never end, a row
	// the restart pass could not close for one, and a release can wait where
	// a queue paused most of the time cannot.
	defaultDrainBackoff = 6 * time.Hour
	defaultDrainPoll    = 5 * time.Second
	// defaultConfirmBound outlasts a daemon start: the restart pass, the
	// worker probe, and launchd's 30s throttle on a job that exits once.
	defaultConfirmBound = 2 * time.Minute
	defaultConfirmPoll  = 2 * time.Second
	liftAttempts        = 3
	// restartLease is when the restart barrier lapses if nothing takes it down.
	// It has to outlast the last restart a cycle can make, which comes under
	// five minutes after the backup: a restart, a confirmation and the
	// fall-back's restart, each bounded. An updater that dies with no successor
	// holds new work this long at worst.
	restartLease = 30 * time.Minute
)

// Updater is one host's updater. A cycle runs under a lock, against a ledger
// it opens for that cycle, so a binary that migrated the ledger is never
// held to a handle opened before it.
type Updater struct {
	// Home is the state root.
	Home string
	// Bin is the install target, the path the plists pin.
	Bin string
	// Current is the version this host runs: the binary's version.Current at
	// start, advanced to a release's tag once its install is confirmed.
	Current string

	Source        Source
	Services      Services
	WorkerVersion func() (string, error)
	Log           *log.Logger

	Interval, DrainBound, DrainPoll, DrainBackoff, ConfirmBound, ConfirmPoll time.Duration

	store *ledger.Store
	// retryAt holds off the next attempt after a drain ran out its bound.
	retryAt time.Time
	// failed is a release that fell back after its install. It is not tried
	// again in this process: each try would stop the daemon to fail the same
	// way, and a fix ships as a newer tag.
	failed string
}

// New is the host's updater, logging to out and installing over bin.
func New(bin string, out io.Writer) (*Updater, error) {
	home, err := config.Home()
	if err != nil {
		return nil, err
	}
	return &Updater{
		Home: home, Bin: bin, Current: version.Current,
		Source: githubSource{}, Services: launchdServices{}, WorkerVersion: probeWorker,
		Log:      log.New(out, "", log.LstdFlags),
		Interval: intervalFromEnv(), DrainBound: defaultDrainBound, DrainPoll: defaultDrainPoll,
		DrainBackoff: defaultDrainBackoff, ConfirmBound: defaultConfirmBound, ConfirmPoll: defaultConfirmPoll,
	}, nil
}

func intervalFromEnv() time.Duration {
	if n, err := strconv.Atoi(os.Getenv(launchd.EnvUpdaterInterval)); err == nil && n > 0 {
		return time.Duration(n) * time.Minute
	}
	return launchd.DefaultUpdaterIntervalMinutes * time.Minute
}

// Daemon polls until ctx ends. A failed cycle is logged and the next poll
// tries again; KeepAlive in the plist covers a crash. Each wait starts when
// its cycle ends: a ticker would hold a tick from during a long drain and
// fire it at once, closing the window the pause was just lifted to open.
func (u *Updater) Daemon(ctx context.Context) error {
	u.Log.Printf("updater starting interval=%s current=%s bin=%s", u.Interval, version.String(), u.Bin)
	for {
		_ = u.Tick(ctx)
		select {
		case <-ctx.Done():
			u.Log.Print("updater shutdown requested")
			return nil
		case <-time.After(u.Interval):
		}
	}
}

// Tick is one cycle. Whatever ended it early is logged here and returned.
func (u *Updater) Tick(ctx context.Context) error {
	err := u.cycle(ctx)
	if err != nil {
		u.Log.Printf("cycle ended: %v", err)
	}
	return err
}

func (u *Updater) cycle(ctx context.Context) error {
	unlock, held, err := Lock(u.Home)
	if err != nil {
		return err
	}
	if !held {
		u.Log.Printf("another updater holds %s; skipping this poll", lockFile)
		return nil
	}
	defer unlock()
	if err := u.openLedger(); err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer func() {
		if u.store != nil {
			u.store.Close()
			u.store = nil
		}
	}()
	u.recoverStale()
	if time.Now().Before(u.retryAt) {
		u.Log.Printf("backing off until %s after a drain that ran out its bound", u.retryAt.Format(time.RFC3339))
		return nil
	}

	release, err := u.Source.Latest(ctx)
	if err != nil {
		return fmt.Errorf("latest release: %w", err)
	}
	want, err := wanted(u.Current, release.Tag)
	if err != nil {
		return err
	}
	if !want {
		u.Log.Printf("up to date: running %s, latest %s", u.Current, release.Tag)
		return nil
	}
	if release.Tag == u.failed {
		u.Log.Printf("skipping %s: it fell back after its install, and only a newer release or an updater restart tries again", release.Tag)
		return nil
	}
	u.Log.Printf("update available: %s to %s", version.String(), release.Tag)

	// Ahead of the download, which each poll would otherwise repeat for as
	// long as the terminal or the pause stands.
	if skip, err := u.held(); skip != "" || err != nil {
		if err == nil {
			u.Log.Printf("skipping this poll: %s", skip)
		}
		return err
	}
	s, err := u.prepare(ctx, release)
	if err != nil {
		return fmt.Errorf("refused %s: %w", release.Tag, err)
	}
	defer os.Remove(s.path)
	baseline, err := u.Services.Build(ctx)
	if err != nil {
		return fmt.Errorf("refused %s: the running build cannot be read, so the new one could not be confirmed: %w", release.Tag, err)
	}
	u.Log.Printf("step 1: %s verified and staged, the daemon reports %q", release.Tag, baseline)

	claimed, reason, err := u.store.ClaimUpdatePause()
	if err != nil {
		return fmt.Errorf("pause %s: %w", ledger.WeftAgent, err)
	}
	if !claimed {
		u.Log.Printf("skipping this poll: %s is already paused (%s), which is not the updater's to take over or lift",
			ledger.WeftAgent, reason)
		return nil
	}
	u.Log.Printf("step 2: paused %s for the update to %s", ledger.WeftAgent, release.Tag)
	defer u.lift()
	return u.apply(ctx, s, baseline)
}

// held names what rules this poll out with dispatch left as it is: a pause
// that is not the updater's to take over, or an open terminal, which only a
// person can end and so would hold the queue for nothing.
func (u *Updater) held() (string, error) {
	agent, err := u.store.Agent(ledger.WeftAgent)
	if err != nil {
		return "", err
	}
	if agent.Paused {
		return fmt.Sprintf("%s is already paused (%s)", ledger.WeftAgent, agent.PausedReason.String), nil
	}
	work, err := u.store.RestartWork()
	if err != nil {
		return "", err
	}
	if len(work.Terminals) > 0 {
		return "a restart would kill open terminals: " + strings.Join(work.Terminals, ", "), nil
	}
	return "", nil
}

// Lock takes the host's one updater lock under the state root home, held is
// false when another process has it. Holding it proves no updater is
// mid-cycle, which is what makes an update pause found here a stale one, so
// anything else that claims the update pause takes it first.
func Lock(home string) (unlock func(), held bool, err error) {
	if err := ledger.EnsureHome(home); err != nil {
		return nil, false, err
	}
	f, err := os.OpenFile(filepath.Join(home, lockFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() { f.Close() }, true, nil
}

func (u *Updater) openLedger() error {
	store, err := ledger.Open(filepath.Join(u.Home, ledgerName))
	if err != nil {
		return err
	}
	u.store = store
	return nil
}

// recoverStale clears what an updater that did not finish left behind. It
// runs first on every poll, ahead of anything that can fail on the network,
// so a crash cannot leave dispatch paused past the updater's next start.
func (u *Updater) recoverStale() {
	lifted, barrier, err := u.store.ReleaseUpdatePause()
	if err != nil {
		u.Log.Printf("recover a stale update pause: %v", err)
	}
	if lifted {
		u.Log.Printf("lifted an update pause left by an updater that did not finish; dispatch resumed")
	}
	if barrier {
		u.Log.Printf("took down a restart barrier left by an updater that did not finish; new work may start")
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(u.Bin), stagedPattern))
	if err != nil {
		u.Log.Printf("list staged binaries: %v", err)
	}
	for _, path := range leftovers {
		if err := os.Remove(path); err != nil {
			u.Log.Printf("remove staged binary %s: %v", path, err)
		}
	}
}

// prepare is every check that can refuse a release, all before dispatch is
// touched. A refusal is retried at the next poll, which disturbs nothing.
func (u *Updater) prepare(ctx context.Context, r Release) (*staged, error) {
	tarballName := assetName(r.Tag)
	assets := map[string][]byte{}
	for _, name := range []string{tarballName, checksumsAsset, manifestAsset} {
		url, ok := r.Assets[name]
		if !ok {
			return nil, fmt.Errorf("release has no asset %s", name)
		}
		data, err := u.Source.Download(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("download %s: %w", name, err)
		}
		u.Log.Printf("downloaded %s, %d bytes", name, len(data))
		assets[name] = data
	}
	for _, name := range []string{tarballName, manifestAsset} {
		if err := verifyChecksum(assets[name], assets[checksumsAsset], name); err != nil {
			return nil, err
		}
	}
	if err := checkWorker(assets[manifestAsset], u.WorkerVersion); err != nil {
		return nil, err
	}
	binary, err := extractBinary(assets[tarballName])
	if err != nil {
		return nil, err
	}
	return stage(ctx, binary, u.Bin, r.Tag)
}

// apply is design steps 3 to 8, under the pause. Until the install, ending
// early changes nothing. From the install on it either confirms the new
// build or restores the old one, ignoring cancellation: a SIGTERM here must
// not leave a half-changed host.
func (u *Updater) apply(ctx context.Context, s *staged, baseline string) error {
	if err := u.drain(ctx); err != nil {
		return err
	}
	beforeBarrier()
	// Ahead of the backup, so a restored ledger comes back with the barrier
	// up. From here the update runs to its end: nothing starts, and neither
	// a resume nor a pause abandons it.
	if err := u.store.ArmRestartBarrier(restartLease); err != nil {
		return err
	}
	u.Log.Print("step 3: drained, and the restart barrier holds new work until the update ends")

	backup, err := u.backup(s.tag)
	if err != nil {
		return fmt.Errorf("back up the ledger: %w", err)
	}
	u.Log.Printf("step 4: ledger backed up to %s", backup)

	schema, err := u.store.SchemaVersion()
	if err != nil {
		return err
	}
	beforeInstall()
	if err := u.install(s); err != nil {
		return fmt.Errorf("install: %w", err)
	}
	u.Log.Printf("step 5: installed %s over %s", s.tag, u.Bin)

	ctx = context.WithoutCancel(ctx)
	if err := u.bringUp(ctx, u.Bin, s.build); err != nil {
		return u.fallBack(ctx, s, baseline, schema, backup, err)
	}
	u.Log.Printf("step 7: /v2/service reports %q", s.build)
	// The install is confirmed, so this process is no longer behind the
	// release even if the job replacement below never lands: a later poll
	// must not repeat the update, rotating out its backup and .previous.
	u.Current = s.tag
	u.lift()
	u.Log.Printf("step 8: update to %s complete", s.tag)
	if err := u.Services.RestartSelf(u.Bin); err != nil {
		return fmt.Errorf("restart the updater under %s: %w", s.tag, err)
	}
	u.Log.Printf("handed the updater's job to %s", s.tag)
	return nil
}

// beforeBarrier and beforeInstall are seams for a test to act on the ledger
// either side of the barrier going up.
var (
	beforeBarrier = func() {}
	beforeInstall = func() {}
)

// drain waits for what a restart would kill to end by itself. It only reads:
// nothing here signals a process. Running work ends on its own and gets the
// bound; a terminal opened during the drain will not, so it ends the attempt
// at once rather than holding the queue.
func (u *Updater) drain(ctx context.Context) error {
	deadline := time.Now().Add(u.DrainBound)
	reported := ""
	for {
		if err := u.store.RequireUpdatePause(); err != nil {
			return err
		}
		work, err := u.store.RestartWork()
		if err != nil {
			return err
		}
		if len(work.Terminals) > 0 {
			return fmt.Errorf("a restart would kill open terminals: %s", strings.Join(work.Terminals, ", "))
		}
		if len(work.Runs) == 0 {
			return nil
		}
		if names := strings.Join(work.Runs, ", "); names != reported {
			u.Log.Printf("waiting on: %s", names)
			reported = names
		}
		if !time.Now().Before(deadline) {
			u.retryAt = time.Now().Add(u.DrainBackoff)
			return fmt.Errorf("drain bound %s passed with %s still running; dispatch resumes and the next attempt is after %s",
				u.DrainBound, reported, u.retryAt.Format(time.RFC3339))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(u.DrainPoll):
		}
	}
}

// backup takes a consistent copy of the ledger, named so the names sort by
// age, and keeps the newest backupsKept.
func (u *Updater) backup(tag string) (string, error) {
	dir := filepath.Join(u.Home, backupsDir)
	if err := ledger.EnsureHome(dir); err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s.%s.pre-%s", ledgerName, stamp(), tag))
	if err := u.store.Backup(path); err != nil {
		return "", err
	}
	kept, err := filepath.Glob(filepath.Join(dir, ledgerName+".*.pre-*"))
	if err != nil {
		return "", err
	}
	slices.Sort(kept)
	for _, old := range kept[:max(0, len(kept)-backupsKept)] {
		if err := os.Remove(old); err != nil {
			u.Log.Printf("remove old backup %s: %v", old, err)
		}
	}
	return path, nil
}

func stamp() string { return time.Now().UTC().Format("20060102T150405.000Z") }

// install renames the staged binary over Bin. The path gets a new inode, so
// the running processes keep their pages: macOS kills a process whose signed
// executable's pages change under it. The old inode stays linked as
// .previous, which is what a fall-back renames back.
func (u *Updater) install(s *staged) error {
	previous := u.Bin + previousSuffix
	if err := os.Remove(previous); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Link(u.Bin, previous); err != nil {
		return err
	}
	return os.Rename(s.path, u.Bin)
}

// bringUp restarts the jobs under bin and holds /v2/service to want, exactly.
func (u *Updater) bringUp(ctx context.Context, bin, want string) error {
	if err := u.Services.Restart(ctx, bin); err != nil {
		return fmt.Errorf("restart: %w", err)
	}
	u.Log.Printf("step 6: restarted the daemon and watchdog under %s", bin)
	deadline := time.Now().Add(u.ConfirmBound)
	for {
		got, err := u.Services.Build(ctx)
		if err == nil && got == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				return fmt.Errorf("/v2/service did not report %q within %s: %w", want, u.ConfirmBound, err)
			}
			return fmt.Errorf("/v2/service still reports %q, not %q, after %s", got, want, u.ConfirmBound)
		}
		time.Sleep(u.ConfirmPoll)
	}
}

// fallBack restores the previous binary and brings it back up, returning why
// the update failed. A new build that started may have migrated the ledger,
// and the old binary refuses a newer schema, so that ledger goes too.
func (u *Updater) fallBack(ctx context.Context, s *staged, baseline string, schema int, backup string, cause error) error {
	u.failed = s.tag
	u.Log.Printf("falling back from %s: %v", s.tag, cause)
	if err := os.Rename(u.Bin+previousSuffix, u.Bin); err != nil {
		return u.stranded(s, cause, fmt.Errorf("restore the previous binary: %w", err))
	}
	u.Log.Printf("restored the previous binary at %s", u.Bin)
	current, err := u.store.SchemaVersion()
	if err != nil {
		return u.stranded(s, cause, fmt.Errorf("read the schema version: %w", err))
	}
	if current != schema {
		u.Log.Printf("%s moved the ledger from schema %d to %d, which the previous binary refuses", s.tag, schema, current)
		if err := u.restoreLedger(ctx, s, backup); err != nil {
			return u.stranded(s, cause, fmt.Errorf("restore the ledger: %w", err))
		}
	}
	if err := u.bringUp(ctx, u.Bin, baseline); err != nil {
		return u.stranded(s, cause, err)
	}
	u.Log.Printf("back on %q", baseline)
	return fmt.Errorf("update to %s failed and the previous build is running: %w", s.tag, cause)
}

// stranded is a fall-back that itself failed: the one outcome the updater
// cannot repair, and what a person reading the log has to act on.
func (u *Updater) stranded(s *staged, cause, err error) error {
	return fmt.Errorf("update to %s failed and so did the fall-back, the host needs an operator (binary %s, previous %s%s): %w; update failed because: %w",
		s.tag, u.Bin, u.Bin, previousSuffix, err, cause)
}

// restoreLedger puts the pre-update backup back with nothing else holding
// the ledger. The backup was taken under the update pause and the restart
// barrier, so both come back with it and end as usual. What the ledger took in
// between stays readable beside the backups.
func (u *Updater) restoreLedger(ctx context.Context, s *staged, backup string) error {
	if err := u.Services.Stop(ctx); err != nil {
		return err
	}
	if err := u.store.Close(); err != nil {
		u.Log.Printf("close ledger: %v", err)
	}
	u.store = nil
	aside := filepath.Join(u.Home, backupsDir, fmt.Sprintf("%s.%s.failed-%s", ledgerName, stamp(), s.tag))
	restored := ledger.RestoreBackup(filepath.Join(u.Home, ledgerName), backup, aside)
	reopened := u.openLedger()
	if err := errors.Join(restored, reopened); err != nil {
		return err
	}
	u.Log.Printf("restored the ledger from %s; the ledger it replaced is at %s", backup, aside)
	return nil
}

// lift ends the update pause and the restart barrier. A failure here is a busy
// ledger, so it retries; past that both are stale, and the next poll's recovery
// lifts them. It lifts no pause but the updater's own, and finding that gone is
// fine. With no store the ledger could not be reopened after a restore, so no
// poll can reach the recovery either.
func (u *Updater) lift() {
	if u.store == nil {
		u.Log.Print("no ledger to lift the update pause through; an operator must repair the ledger before the pause can lift")
		return
	}
	for attempt := 1; attempt <= liftAttempts; attempt++ {
		lifted, barrier, err := u.store.ReleaseUpdatePause()
		if err == nil {
			if barrier {
				u.Log.Printf("restart barrier down")
			}
			if lifted {
				u.Log.Printf("dispatch resumed")
			}
			return
		}
		u.Log.Printf("lift the update pause, attempt %d of %d: %v", attempt, liftAttempts, err)
		time.Sleep(time.Second)
	}
	u.Log.Print("the update pause still stands; the next poll recovers it")
}
