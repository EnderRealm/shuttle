package cmd

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/runner"
	"shuttle/internal/schedule"
	"shuttle/internal/weftremote"
)

// tickInterval is §5's loop: one pass every 60 seconds, plus the immediate one
// at startup.
const tickInterval = 60 * time.Second

// startFailureFile, under the state root, holds the text of the error that
// last ended the daemon before its loop was running. launchd restarts a failed
// daemon every 30 seconds and the watchdog kickstarts it in between, so the
// record is what holds daemon.log to one line per distinct failure: the daemon
// prints a start failure only when it differs from the record, the watchdog
// kickstarts silently while one stands, and `shuttle status` shows it.
const startFailureFile = "daemon-start-failure"

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run the scheduler; launchd runs this",
	Long: `Run the scheduler loop in the foreground (docs/phase1-design.md §5).

One pass at startup and one every 60 seconds: each due slot is claimed, and the
slots a sleeping machine missed are recorded as skipped. Decisions are logged to
stderr, which is what launchd captures into ~/.shuttle/daemon.log.

Agent definitions are read once at startup, so an agent added or edited under
SHUTTLE_AGENTS_DIR is invisible until the daemon is restarted.

The restart pass runs once, at startup: a row it leaves in flight is re-checked
only at the next daemon start, and until then that agent's slots skip on overlap.
Claimed slots execute through the same §7 pipeline as shuttle run, with at most
two run workers active across agents.

An error that ends the daemon before its loop is running is recorded in
~/.shuttle/daemon-start-failure until a start succeeds. The same error again
exits 1 without printing it a second time; shuttle status shows the record.`,
	// The load and ledger errors name the agent and the row; cobra printing
	// them a second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		// Every error returned before the loop is running is a start failure.
		looping := false
		defer func() {
			if err != nil && !looping {
				err = recordStartFailure(err)
			}
		}()
		// Cobra's error writer is stderr, which is what launchd captures into
		// ~/.shuttle/daemon.log, and what a test can redirect.
		logger := log.New(cmd.ErrOrStderr(), "", log.LstdFlags)
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		sched, err := newScheduler(store, logger)
		if err != nil {
			return err
		}
		// §5's restart rule, before the first tick so no slot is evaluated
		// against a stale in-flight row. The rows the pass accounts for become
		// unknown, killed by process group first when the pid is established as
		// the run's own; the ones it cannot — kill_failed, unresolved — stay in
		// flight and keep the agent's slots skipping on overlap. See
		// reap.Restart for the rule, and the log lines for what each row got.
		outcomes, err := reap.Restart(store)
		// Logged before err is checked: a pass stopped by a MarkUnknown
		// failure still signalled the processes behind the outcomes it did
		// record, and these lines are the only account of that.
		inFlight, unsignalled := 0, 0
		for _, o := range outcomes {
			logger.Print(restartLine(o))
			switch o.Action {
			case reap.ActionKillFailed, reap.ActionUnresolved:
				// Left in flight, holding the overlap guard for the
				// processes it could not account for.
				inFlight++
				unsignalled++
			case reap.ActionForeign:
				// Closed, but its pid was never signalled either.
				unsignalled++
			}
			if o.Agent == ledger.WeftAgent && o.Action != reap.ActionKillFailed && o.Action != reap.ActionUnresolved {
				if err := store.ReconcileWeftUnknown(o.RunID); err != nil {
					return fmt.Errorf("reconcile hosted weft run %s: %w", o.RunID, err)
				}
			}
		}
		if err != nil {
			return err
		}
		pipeline, err := buildPipeline(store, logger)
		if err != nil {
			return err
		}

		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		var serviceErr <-chan error
		cfg, err := config.Load()
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if cfg != nil && cfg.Weft.Listen != "" {
			// Bound before Restart, which can spawn segments, so a daemon that
			// cannot take its address ends its start before touching the queue.
			// Serve's shutdown closes it too; the second Close is harmless.
			listener, err := weftremote.Listen(cfg.Weft.Listen)
			if err != nil {
				return err
			}
			defer listener.Close()
			token, err := cfg.WeftToken()
			if err != nil {
				return fmt.Errorf("weft token: %w", err)
			}
			home, err := config.Home()
			if err != nil {
				return err
			}
			service, err := weftremote.New(cfg, store, token, home, logger)
			if err != nil {
				return err
			}
			// weft-service/v2 §12 steps 3, 4 and 5, after the restart pass above
			// (step 1) and before the listener accepts or anything dispatches.
			if err := service.Restart(); err != nil {
				return fmt.Errorf("weft restart: %w", err)
			}
			ch := make(chan error, 1)
			serviceErr = ch
			go func() { ch <- service.Serve(ctx, listener) }()
			logger.Printf("weft service listening on %s", cfg.Weft.Listen)
		}
		if err := clearStartFailure(); err != nil {
			logger.Printf("start failure record: %v", err)
		}
		// Logged once nothing is left to end the start: a crash loop on a
		// later startup step would otherwise read as a string of good starts.
		logger.Printf("daemon start: closed %d row(s) left in flight, %d not signalled (kill_failed/unresolved/foreign), watching %d agent(s)",
			len(outcomes)-inFlight, unsignalled, len(sched.Defs))
		looping = true
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		sem := make(chan struct{}, 2)
		var workers sync.WaitGroup
		for {
			auth := &runner.AuthTick{}
			sched.Launch = func(def *agentdef.Definition, run *ledger.Run) error {
				workers.Add(1)
				go func() {
					defer workers.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					if _, err := pipeline.Run(ctx, def, run, runner.Options{Auth: auth}); err != nil {
						logger.Printf("run=%s agent=%s: %v", run.ID, def.Name, err)
					}
				}()
				return nil
			}
			// A tick that failed for one agent is logged and the loop carries
			// on; the next pass is a minute away.
			if err := sched.Tick(); err != nil {
				logger.Printf("tick: %v", err)
			}
			select {
			case <-ctx.Done():
				workers.Wait()
				logger.Print("daemon stop")
				return nil
			case err := <-serviceErr:
				if err != nil {
					return fmt.Errorf("weft service: %w", err)
				}
				serviceErr = nil
			case <-ticker.C:
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(daemonCmd)
}

func startFailurePath() (string, error) {
	home, err := config.Home()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, startFailureFile), nil
}

// startFailure returns the standing start-failure record. One that cannot be
// read reads as none, which leaves the daemon printing every start failure and
// the watchdog logging every kickstart: noisy, never silent.
func startFailure() (string, bool) {
	path, err := startFailurePath()
	if err != nil {
		return "", false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return string(data), true
}

// recordStartFailure records err as the daemon's start failure. One that
// matches the standing record comes back quiet: daemon.log already carries it
// from the start that recorded it.
func recordStartFailure(err error) error {
	text := err.Error()
	if prior, ok := startFailure(); ok && prior == text {
		return quietError{err}
	}
	path, recordErr := startFailurePath()
	if recordErr == nil {
		recordErr = os.WriteFile(path, []byte(text), 0o600)
	}
	if recordErr != nil {
		return errors.Join(err, fmt.Errorf("record start failure: %w", recordErr))
	}
	return err
}

func clearStartFailure() error {
	path, err := startFailurePath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// restartLine renders one restart outcome for the daemon log. A row that
// carried no pid prints pid=- rather than pid=0, which is a real process group
// on the signalling side and would read as one here.
func restartLine(o reap.Outcome) string {
	pid := "-"
	if o.PID != 0 {
		pid = strconv.Itoa(o.PID)
	}
	line := fmt.Sprintf("restart: run=%s agent=%s pid=%s action=%s", o.RunID, o.Agent, pid, o.Action)
	if o.Detail != "" {
		line += fmt.Sprintf(" detail=%q", o.Detail)
	}
	return line
}

// newScheduler wires the §5 scheduler from config.json, the agent definitions
// and the ledger. `shuttle run` and `shuttle status` build the same one, so a
// manual run cannot drift from the gate the daemon applies.
func newScheduler(store *ledger.Store, logger *log.Logger) (*schedule.Scheduler, error) {
	// A missing config.json is not fatal: agents_dir has its own default and
	// the limits are simply unset, which is what a checkout is before
	// `shuttle install` has written the file.
	cfg, err := config.Load()
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if cfg == nil {
		cfg = &config.Config{}
	}
	loc := time.Local
	if cfg.Timezone != "" {
		if loc, err = time.LoadLocation(cfg.Timezone); err != nil {
			return nil, fmt.Errorf("config timezone: %w", err)
		}
	}
	dir, err := config.AgentsDir()
	if err != nil {
		return nil, err
	}
	defs, loadErr := agentdef.LoadAll(dir)
	if loadErr != nil {
		// A definition that will not load is reported and the agents that did
		// load still run: one broken manifest must not stop the estate.
		logger.Printf("agent definitions: %v", loadErr)
	}
	return &schedule.Scheduler{
		Ledger:        store,
		Defs:          defs,
		Loc:           loc,
		MaxRunsPerDay: cfg.Limits.DailyRunsPerAgent,
		MaxSpendUSD:   cfg.Limits.DailySpendUSD,
		Now:           time.Now,
		// The daemon and manual command attach the live pipeline after the
		// scheduler is built; tests may leave it nil to inspect claims alone.
		Log: logger,
	}, nil
}
