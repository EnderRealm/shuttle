package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"shuttle/internal/launchd"
)

var uninstallLaunchAgent = launchd.Uninstall

var uninstallCmd = &cobra.Command{
	Use:           "uninstall",
	Short:         "Remove the shuttle launchd jobs (state preserved)",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// The updater first, so it cannot restart what the rest of this removes.
		for _, label := range []string{launchd.UpdaterLabel, launchd.WatchdogLabel, launchd.DaemonLabel} {
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
