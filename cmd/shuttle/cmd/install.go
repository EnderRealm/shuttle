package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/output"
)

// daemonArg is the argument the help advertises for the launchd install (§13).
const daemonArg = "daemon"

var installCmd = &cobra.Command{
	Use:   "install",
	Short: "Create the state root and the reports repo",
	Long: `Create the state root and the reports repo (docs/phase1-design.md §9.3).

The state root is $SHUTTLE_HOME, else ~/.shuttle. Its reports directory is
initialised as a git repository with no remote; adding one is the operator's
choice. Both steps are idempotent, so running install against a live tree
changes nothing.

The launchd job (§13) is not installed yet.`,
	// MaximumNArgs rather than NoArgs so `install daemon`, which the help
	// advertises, gets the explanation below instead of cobra's generic
	// unknown-command error.
	Args: cobra.MaximumNArgs(1),
	// The errors name the path that could not be created; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 {
			if args[0] == daemonArg {
				return errors.New("install daemon: the launchd job (§13) is not built yet")
			}
			return fmt.Errorf("install: unknown argument %q", args[0])
		}
		home, err := config.Home()
		if err != nil {
			return err
		}
		// Through the ledger so the state root install lays down has the same
		// 0700 the ledger requires of it, rather than a mode a later
		// ledger.Open has to tighten.
		if err := ledger.EnsureHome(home); err != nil {
			return err
		}
		reports, err := config.ReportsDir()
		if err != nil {
			return err
		}
		if err := output.InitReports(reports); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "state root\t%s\nreports repo\t%s\n", home, reports)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(installCmd)
}
