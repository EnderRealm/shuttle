package cmd

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
	"shuttle/internal/ledger"
	"shuttle/internal/schedule"
	"shuttle/internal/weftremote"
)

// statusProcess reads the daemon's launchd process; a var so a test never
// reaches the real launchd.
var statusProcess = launchd.ProcessFor

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Daemon, agents, next slot, last outcome, streak, paused, open incidents, today's spend",
	// The ledger errors name the agent and the row; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// First and before any load: a config.json the daemon cannot start on
		// is one status cannot list agents from either, and the daemon row is
		// what says so.
		row, ok := daemonRow()
		fmt.Println(row)
		failed := !ok
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		sched, err := newScheduler(store, log.New(os.Stderr, "", 0))
		if err != nil {
			return err
		}
		now := time.Now()
		for _, def := range sched.Defs {
			agent, err := store.Agent(def.Name)
			if err != nil {
				return err
			}
			next, err := def.NextSlot(now)
			if err != nil {
				// One definition whose triggers will not resolve must not hide
				// every other agent's row.
				fmt.Fprintln(os.Stderr, err)
				failed = true
				continue
			}
			// Both instants, labelled: the slot is what the ledger records as
			// scheduled_for and what `shuttle agents` prints, and the jittered
			// one is the minute the daemon actually acts on (§5 step 6).
			fires := next.Add(schedule.Jitter(def.Name))
			paused := "active"
			if agent.Paused {
				paused = "paused (" + nullOr(agent.PausedReason) + ")"
			}
			last, ok := lastOutcome(store, def.Name)
			if !ok {
				failed = true
			}
			fmt.Printf("%s\tnext %s (fires %s)\tlast %s\tstreak %d\t%s\n",
				escapeControl(def.Name), next.Format("2006-01-02 15:04:05 MST"),
				fires.Format("15:04:05"), last, agent.FailureStreak, paused)
		}
		// weft-worker is the ledger's hosted agent, not a definition, so it has
		// no slot of its own. Its row is printed wherever the daemon would host
		// the service: the same empty-Listen rule.
		cfg, err := config.Load()
		switch {
		case err != nil && !os.IsNotExist(err):
			fmt.Fprintln(os.Stderr, err)
			failed = true
		case cfg != nil && cfg.Weft.Listen != "":
			row, err := weftRow(store, cfg.Weft.WorkerBin)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				failed = true
				break
			}
			fmt.Println(row)
		}
		// The window the daily limits use: midnight in the configured zone.
		spent, err := store.SpendSince(sched.DayStart())
		if err != nil {
			return err
		}
		// A sum over the runs that reported a cost. A window whose runs all
		// reported none sums to NULL and prints as unpriced: $0.00 would claim
		// the day was free (§12).
		fmt.Printf("today's spend\t%s\n", formatCost(spent))
		incidents, err := store.OpenIncidents()
		if err != nil {
			return err
		}
		fmt.Printf("open incidents\t%d\n", len(incidents))
		if failed {
			return errors.New("one or more rows could not be listed")
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

// daemonRow renders the daemon's launchd process and, while one stands, the
// start failure it last exited on. ok is false when the process cannot be
// read, so the command still exits non-zero.
func daemonRow() (string, bool) {
	proc, running, err := statusProcess(launchd.DaemonLabel)
	state, ok := "not running", true
	switch {
	case err != nil:
		state, ok = "unknown: "+escapeControl(err.Error()), false
	case running:
		state = fmt.Sprintf("running (pid %d)", proc.PID)
	}
	row := "daemon\t" + state
	if failure, recorded := startFailure(); recorded {
		row += "\tstart failed: " + escapeControl(failure)
	}
	return row, ok
}

// lastOutcome renders the agent's most recent ledger row: its status, the slot
// it covered and what it cost. A ledger read that fails is reported in the
// column rather than aborting the whole status, and ok is false so the command
// still exits non-zero.
func lastOutcome(store *ledger.Store, agent string) (string, bool) {
	runs, err := store.Runs(agent, 1)
	if err != nil {
		return "error: " + escapeControl(err.Error()), false
	}
	if len(runs) == 0 {
		return "never run", true
	}
	r := runs[0]
	status := escapeControl(r.Status)
	if r.SkipReason.Valid {
		status += "/" + escapeControl(r.SkipReason.String)
	}
	return fmt.Sprintf("%s %s %s", status, escapeControl(r.ScheduledFor), formatCost(r.CostUSD)), true
}

// weftRow renders the hosted worker: its pause from the ledger's agents row,
// the queue's running and waiting against the cap (the figures /v2/dashboard
// reports), and worker_bin's answer to the daemon's version probe. The probe
// is run by this command, now, and labelled so: status answers with the
// daemon down and never claims the daemon's view.
func weftRow(store *ledger.Store, bin string) (string, error) {
	agent, err := store.Agent(ledger.WeftAgent)
	if err != nil {
		return "", fmt.Errorf("%s: %w", ledger.WeftAgent, err)
	}
	counts, err := store.WeftDashboardCounts()
	if err != nil {
		return "", fmt.Errorf("%s: %w", ledger.WeftAgent, err)
	}
	paused := "active"
	if agent.Paused {
		paused = "paused (" + nullOr(agent.PausedReason) + ") since " + nullOr(agent.PausedAt)
	}
	version, contracts, capabilities, failure := weftremote.ProbeWorker(bin)
	worker := "worker (probed now) "
	if failure != "" {
		worker += escapeControl(bin) + ": " + escapeControl(failure)
	} else {
		worker += fmt.Sprintf("%s contracts [%s] capabilities [%s]", escapeControl(version),
			escapeControl(strings.Join(contracts, " ")), escapeControl(strings.Join(capabilities, " ")))
	}
	return fmt.Sprintf("%s\trunning %d waiting %d cap %d\t%s\t%s",
		ledger.WeftAgent, counts.Running, counts.Waiting, counts.Capacity, paused, worker), nil
}
