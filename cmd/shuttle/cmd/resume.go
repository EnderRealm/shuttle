package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"shuttle/internal/ledger"
)

var resumeCmd = &cobra.Command{
	Use:           "resume <agent>",
	Short:         "Clear an agent's pause and failure streak",
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.Resume(args[0]); err != nil {
			return err
		}
		held, err := store.RestartBarrier()
		if err != nil {
			return err
		}
		if held && args[0] == ledger.WeftAgent {
			fmt.Fprintf(cmd.OutOrStdout(), "%s stays paused while an update restarts shuttle, and resumes when the update ends\n", args[0])
		}
		return nil
	},
}

func init() { rootCmd.AddCommand(resumeCmd) }
