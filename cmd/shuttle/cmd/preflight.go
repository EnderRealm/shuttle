package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/runner"
)

var preflightCmd = &cobra.Command{
	Use:           "preflight [agent]",
	Short:         "Check declared requirements without a model call",
	Args:          cobra.MaximumNArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		home, err := config.Home()
		if err != nil {
			return err
		}
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		agents, err := config.AgentsDir()
		if err != nil {
			return err
		}
		defs, loadErr := agentdef.LoadAll(agents)
		if loadErr != nil {
			if len(args) == 0 {
				return loadErr
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "agent definitions: %v\n", loadErr)
		}
		if len(args) == 1 {
			def, err := findDef(defs, args[0])
			if err != nil {
				return err
			}
			defs = []*agentdef.Definition{def}
		}
		for _, def := range defs {
			dir, err := os.MkdirTemp("/tmp", "shuttle-preflight-")
			if err != nil {
				return err
			}
			// Resolve /var and /tmp aliases before generating the sandbox.
			dir, err = filepath.EvalSymlinks(dir)
			if err != nil {
				return err
			}
			p := runner.Preflight{Config: cfg, Home: home, HomeDir: homeDir, PATH: os.Getenv("PATH")}
			names, checkErr := p.Check(cmd.Context(), def, dir)
			os.RemoveAll(dir)
			for i, name := range names {
				status := "pass"
				if checkErr != nil && i == len(names)-1 {
					status = "fail"
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", def.Name, name, status)
			}
			if checkErr != nil {
				return checkErr
			}
		}
		return nil
	},
}

func init() { rootCmd.AddCommand(preflightCmd) }
