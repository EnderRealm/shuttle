package cmd

import "github.com/spf13/cobra"

var pauseCmd = &cobra.Command{
	Use:           "pause <agent>",
	Short:         "Pause future claims for an agent",
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		return store.Pause(args[0])
	},
}

func init() { rootCmd.AddCommand(pauseCmd) }
