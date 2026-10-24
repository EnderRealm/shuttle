package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/runner"
)

var reconcileCmd = &cobra.Command{
	Use:   "reconcile <run-id>",
	Short: "Resume a failed observer run's notebook records and ticket actions",
	Long: `Resume a failed observer run from the stage its applied.json records
(docs/observer-contract.md §11): an assessment without its record receipt is
replayed under its original request id, and each action short of settled or
refused is driven on from its stage under the ids the run persisted, so a step
that landed is answered with its outcome rather than performed twice.

Only a failed observer run is reconciled; any other is refused before a
notebook call. A failed run whose records are all settled or refused makes no
notebook call and goes straight to the finish, so running reconcile again heals
one whose finish failed part-way. When every record converges the run is
finished the way a live one is: the report is prepared, observer-result.json is
rewritten with the reconciled counts, the ledger row moves to completed and the
report is delivered. The failure streak, the incident and any pause the failure
caused are left as they are. A reconciliation that does not converge, or whose
finish fails, records its counts, leaves the row failed and exits non-zero with
the reason.`,
	Args: cobra.ExactArgs(1),
	// The errors name the run; cobra printing them a second time, with usage,
	// would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		row, err := store.Get(args[0])
		if err != nil {
			return err
		}
		logger := log.New(cmd.ErrOrStderr(), "", log.LstdFlags)
		dir, err := config.AgentsDir()
		if err != nil {
			return err
		}
		defs, loadErr := agentdef.LoadAll(dir)
		if loadErr != nil {
			logger.Printf("agent definitions: %v", loadErr)
		}
		def, err := findDef(defs, row.Agent)
		if err != nil {
			return err
		}
		built, err := newPipeline(store, logger)
		if err != nil {
			return err
		}
		pipeline, ok := built.(*runner.Pipeline)
		if !ok {
			return fmt.Errorf("reconcile %s: the pipeline does not reconcile observer runs", row.ID)
		}
		ctx, stop := manualRunContext(cmd.Context())
		defer stop()
		if err := pipeline.ReconcileObserverActions(ctx, def, row); err != nil {
			return fmt.Errorf("reconcile %s: %w", row.ID, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "completed\t%s\n", row.RunDir.String)
		return nil
	},
}

func init() { rootCmd.AddCommand(reconcileCmd) }
