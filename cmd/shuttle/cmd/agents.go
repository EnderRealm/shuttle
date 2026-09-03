package cmd

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
)

var agentsCmd = &cobra.Command{
	Use:   "agents",
	Short: "Load and validate every agent definition",
	// The load failures are reported per agent below; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		dir, err := config.AgentsDir()
		if err != nil {
			return err
		}
		defs, loadErr := agentdef.LoadAll(dir)
		failed := loadErr != nil
		if loadErr != nil {
			fmt.Fprintln(os.Stderr, loadErr)
		}
		now := time.Now()
		for _, d := range defs {
			slot, err := d.NextSlot(now)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				failed = true
				continue
			}
			fmt.Printf("%s\t%s\n", d.Name, slot.Format("2006-01-02 15:04:05 MST"))
		}
		if failed {
			return errors.New("one or more agent definitions failed to load")
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(agentsCmd)
}
