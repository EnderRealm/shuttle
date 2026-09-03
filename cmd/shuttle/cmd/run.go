package cmd

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
	"shuttle/internal/schedule"
)

var runSlot string

var runCmd = &cobra.Command{
	Use:   "run <agent>",
	Short: "Claim a slot for an agent by hand",
	Long: `Claim a slot for an agent by hand (docs/phase1-design.md §5).

A manual run bypasses the schedule but not the overlap check, the daily limits
or the ledger: a slot the checks refuse is recorded as skipped with its reason.
--slot resolves that day's slot in the agent's trigger zone, so a re-run reports
the same day whatever the wall clock says.

There is no runner yet (§7): this command records the claim and executes
nothing.`,
	Args: cobra.ExactArgs(1),
	// The errors name the agent and the slot; cobra printing them a second
	// time, with usage, would bury them.
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
		name := args[0]
		var def *agentdef.Definition
		for _, d := range sched.Defs {
			if d.Name == name {
				def = d
				break
			}
		}
		if def == nil {
			return fmt.Errorf("agent %s: no definition loaded", name)
		}
		now := time.Now()
		var at time.Time
		if runSlot != "" {
			at, err = schedule.ManualSlot(def, runSlot)
		} else {
			at, err = schedule.ManualSlotNow(def, now)
		}
		if err != nil {
			return err
		}
		slot := at.Format(time.RFC3339)
		// --slot exists so a re-run reports the same day (§5). A day that has
		// not happened has no window for the providers to derive (§8.1), so it
		// is refused rather than reported over nothing.
		if at.After(now) {
			return fmt.Errorf("agent %s: slot %s: in the future", name, slot)
		}
		run, reason, err := sched.Claim(name, at, ledger.TriggerManual)
		if err != nil {
			return err
		}
		if reason != "" {
			return fmt.Errorf("agent %s: slot %s: skipped (%s)", name, slot, reason)
		}
		if run == nil {
			return fmt.Errorf("agent %s: slot %s: already in the ledger", name, slot)
		}
		fmt.Printf("%s\t%s\n", run.ID, run.ScheduledFor)
		fmt.Println("claimed only: there is no runner yet, so nothing was executed")
		return nil
	},
}

func init() {
	runCmd.Flags().StringVar(&runSlot, "slot", "", "The day to run for, YYYY-MM-DD in the agent's trigger zone")
	rootCmd.AddCommand(runCmd)
}
