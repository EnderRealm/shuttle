package cmd

import "github.com/spf13/cobra"

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
		return store.Resume(args[0])
	},
}

func init() { rootCmd.AddCommand(resumeCmd) }
