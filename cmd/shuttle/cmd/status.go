package cmd

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/ledger"
	"shuttle/internal/schedule"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Agents, next slot, last outcome, streak, paused, open incidents, today's spend",
	// The ledger errors name the agent and the row; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
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
		failed := false
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
			return errors.New("one or more agents could not be listed")
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
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
