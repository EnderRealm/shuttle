package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/schedule"
)

// tickInterval is §5's loop: one pass every 60 seconds, plus the immediate one
// at startup.
const tickInterval = 60 * time.Second

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

There is no runner yet (§7), so a claimed slot is recorded and nothing is
executed. A claimed row therefore never reaches a terminal status, which means
that after the first slot is claimed every later slot for that agent is recorded
as skipped/overlap until the daemon is restarted and reaps it.`,
	// The load and ledger errors name the agent and the row; cobra printing
	// them a second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
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
		}
		if err != nil {
			return err
		}
		logger.Printf("daemon start: closed %d row(s) left in flight, %d not signalled (kill_failed/unresolved/foreign), watching %d agent(s)",
			len(outcomes)-inFlight, unsignalled, len(sched.Defs))

		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for {
			// A tick that failed for one agent is logged and the loop carries
			// on; the next pass is a minute away.
			if err := sched.Tick(); err != nil {
				logger.Printf("tick: %v", err)
			}
			select {
			case <-ctx.Done():
				logger.Print("daemon stop")
				return nil
			case <-ticker.C:
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(daemonCmd)
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
		// Launch stays nil until the runtime lands (§7.4): a due slot is
		// claimed and recorded, and no model is started.
		Log: logger,
	}, nil
}
