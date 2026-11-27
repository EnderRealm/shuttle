package cmd

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/export"
)

var stateCmd = &cobra.Command{
	Use:   "state",
	Short: "Operate on the state root",
}

var stateExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Copy the ledger, run directories and reports repo to export.destination",
	Long: `Copy the run state to config.json's export.destination, a mounted share.

The ledger is copied through SQLite's online backup, so the copy is consistent
while the daemon writes, and replaces the previous runs.db there only once it
is whole. The run directories and the reports repository are copied beside it
as runs/ and reports/; a file the same size and modification time as its copy
is left alone. The destination is never created: one that is missing or
unwritable fails the export and leaves the previous copy as it was.

The com.shuttle.export launchd job runs this daily; what each export did is
in export.log under the state root.`,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		destination := cfg.Export.Destination
		if destination == "" {
			return errors.New("state export: export.destination is not set in config.json")
		}
		home, err := config.Home()
		if err != nil {
			return err
		}
		ledgerPath, err := config.LedgerPath()
		if err != nil {
			return err
		}
		runs, err := config.RunsDir()
		if err != nil {
			return err
		}
		reports, err := config.ReportsDir()
		if err != nil {
			return err
		}
		out := cmd.OutOrStdout()
		started := time.Now()
		fmt.Fprintf(out, "export\t%s\ndestination\t%s\n", started.Format(time.RFC3339), destination)
		result, err := export.Run(cmd.Context(), export.Source{
			Ledger: ledgerPath, Runs: runs, Reports: reports, Staging: filepath.Join(home, export.StagingDir),
		}, destination, out)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "ledger\t%s\nruns\t%s\nreports\t%s\nduration\t%s\n",
			result.Ledger, result.Runs, result.Reports, time.Since(started).Round(time.Millisecond))
		return nil
	},
}

func init() {
	stateCmd.AddCommand(stateExportCmd)
	rootCmd.AddCommand(stateCmd)
}
