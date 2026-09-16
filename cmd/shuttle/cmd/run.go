package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/provider"
	"shuttle/internal/runner"
	"shuttle/internal/schedule"
)

var (
	runSlot            string
	runDryRun          bool
	runNoDeliver       bool
	runUnsafeNoSandbox bool
)

type claimedPipeline interface {
	Run(context.Context, *agentdef.Definition, *ledger.Run, runner.Options) (*runner.RunResult, error)
}

var buildPipeline = newPipeline
var manualRunContext = func(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

var runCmd = &cobra.Command{
	Use:   "run <agent>",
	Short: "Claim a slot for an agent by hand",
	Long: `Claim a slot for an agent by hand (docs/phase1-design.md §5).

A manual run bypasses the schedule but not the overlap check, the daily limits
or the ledger: a slot the checks refuse is recorded as skipped with its reason.
--slot resolves that day's slot in the agent's trigger zone, so a re-run reports
the same day whatever the wall clock says.

--dry-run runs the input providers and renders prompt.md into a run directory
and stops before the model (§8), which is the fastest way to iterate on a prompt
against real data. It claims no ledger row: a dry run is repeatable and leaves
no history. config.json is required, since the providers read the paths in it.

--unsafe-no-sandbox is the only way to run a model outside the §11 srt sandbox:
no manifest kind, no config key and no environment variable can ask for it. The
claim is recorded either way; the flag warns, naming the run.

Without --dry-run this command records the claim, executes the shared §7
pipeline and waits for its terminal ledger state.`,
	Args: cobra.ExactArgs(1),
	// The errors name the agent and the slot; cobra printing them a second
	// time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if runDryRun {
			return dryRun(cmd, args[0])
		}
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
		def, err := findDef(sched.Defs, name)
		if err != nil {
			return err
		}
		at, err := resolveSlot(def, time.Now())
		if err != nil {
			return err
		}
		slot := at.Format(time.RFC3339)
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
		if runUnsafeNoSandbox {
			// The single, explicit bypass the runner will consume: this flag is
			// the only path to an empty claude.Options.Launcher, since §4.1 has
			// no "none" sandbox kind, config.json no knob and the daemon no
			// environment variable for it. It names the run so the warning can
			// be matched to the ledger row afterwards.
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: --unsafe-no-sandbox: run %s is exempt from the srt sandbox\n", run.ID)
		}
		pipeline, err := buildPipeline(store, log.New(cmd.ErrOrStderr(), "", log.LstdFlags))
		if err != nil {
			_ = store.Fail(run.ID, ledger.ClassPreflight, err.Error(), ledger.Result{})
			return err
		}
		ctx, stop := manualRunContext(cmd.Context())
		defer stop()
		result, err := pipeline.Run(ctx, def, run, runner.Options{
			UnsafeNoSandbox: runUnsafeNoSandbox,
			NoDeliver:       runNoDeliver,
		})
		if err != nil {
			return fmt.Errorf("run %s: %w", run.ID, err)
		}
		fmt.Printf("completed\t%s\n", result.RunDir)
		return nil
	},
}

// dryRun is §5's `--dry-run`: the providers and the prompt, no model and no
// ledger row, so it opens neither the ledger nor the scheduler's gate.
func dryRun(cmd *cobra.Command, name string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("dry run: config.json is needed for the providers' paths (§4.2): %w", err)
	}
	home, err := config.Home()
	if err != nil {
		return err
	}
	dir, err := config.AgentsDir()
	if err != nil {
		return err
	}
	logger := log.New(cmd.ErrOrStderr(), "", 0)
	defs, loadErr := agentdef.LoadAll(dir)
	if loadErr != nil {
		logger.Printf("agent definitions: %v", loadErr)
	}
	def, err := findDef(defs, name)
	if err != nil {
		return err
	}
	at, err := resolveSlot(def, time.Now())
	if err != nil {
		return err
	}
	result, err := runner.DryRun(cmd.Context(), def, at, cfg, home, logger)
	if err != nil {
		// A provider failure names the class and the provider, so the message
		// reads as the ledger row a live run would have written (§7.6).
		var perr *provider.Error
		if errors.As(err, &perr) {
			return fmt.Errorf("dry-run failed (class %s): %w", provider.Class, perr)
		}
		return err
	}
	fmt.Printf("%s\t%s\n", result.RunID, result.Dir)
	for _, file := range result.Inputs {
		fmt.Printf("  inputs/%s\n", file)
	}
	fmt.Printf("  prompt.md\t%d bytes\n", result.PromptBytes)
	fmt.Println("dry run: no model was called and no ledger row was written")
	return nil
}

// findDef picks the named agent out of the loaded definitions.
func findDef(defs []*agentdef.Definition, name string) (*agentdef.Definition, error) {
	for _, d := range defs {
		if d.Name == name {
			return d, nil
		}
	}
	return nil, fmt.Errorf("agent %s: no definition loaded", name)
}

// resolveSlot applies §5's manual-slot rules. --slot exists so a re-run reports
// the same day; a day that has not happened has no window for the providers to
// derive (§8.1), so it is refused rather than reported over nothing.
func resolveSlot(def *agentdef.Definition, now time.Time) (time.Time, error) {
	var (
		at  time.Time
		err error
	)
	if runSlot != "" {
		at, err = schedule.ManualSlot(def, runSlot)
	} else {
		at, err = schedule.ManualSlotNow(def, now)
	}
	if err != nil {
		return time.Time{}, err
	}
	if at.After(now) {
		return time.Time{}, fmt.Errorf("agent %s: slot %s: in the future", def.Name, at.Format(time.RFC3339))
	}
	return at, nil
}

func newPipeline(store *ledger.Store, logger *log.Logger) (claimedPipeline, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	home, err := config.Home()
	if err != nil {
		return nil, err
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	runner.ConfigureOutcomes(store, cfg, home, logger)
	return &runner.Pipeline{
		Ledger: store, Config: cfg, Home: home, HomeDir: homeDir,
		PATH: os.Getenv("PATH"), Logger: logger,
	}, nil
}

func init() {
	runCmd.Flags().StringVar(&runSlot, "slot", "", "The day to run for, YYYY-MM-DD in the agent's trigger zone")
	runCmd.Flags().BoolVar(&runDryRun, "dry-run", false, "Run the providers and render the prompt, stopping before the model (§8)")
	runCmd.Flags().BoolVar(&runNoDeliver, "no-deliver", false, "Retain valid output without sending it")
	runCmd.Flags().BoolVar(&runUnsafeNoSandbox, "unsafe-no-sandbox", false, "Run the model outside the srt sandbox (§11)")
	rootCmd.AddCommand(runCmd)
}
