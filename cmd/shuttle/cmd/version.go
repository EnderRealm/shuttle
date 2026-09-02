package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"shuttle/internal/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version string",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println(version.String())
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
