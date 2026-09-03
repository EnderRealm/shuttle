package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

var ackCmd = &cobra.Command{
	Use:   "ack <incident-id>",
	Short: "Acknowledge an open incident",
	Args:  cobra.ExactArgs(1),
	// The ledger errors name the incident id; cobra printing them a second
	// time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("incident id %q: not a number", args[0])
		}
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		return store.Ack(id)
	},
}

func init() {
	rootCmd.AddCommand(ackCmd)
}
