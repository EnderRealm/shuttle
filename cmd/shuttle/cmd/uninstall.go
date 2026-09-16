package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"shuttle/internal/launchd"
)

var uninstallLaunchAgent = launchd.Uninstall

var uninstallCmd = &cobra.Command{
	Use:           "uninstall",
	Short:         "Remove the shuttle launchd job (state preserved)",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, label := range []string{launchd.WatchdogLabel, launchd.DaemonLabel} {
			if err := uninstallLaunchAgent(label); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "removed\t%s\n", label)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "state preserved")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(uninstallCmd)
}
