package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
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

With the daemon argument, install also writes and bootstraps the phase-one
launchd job (§13).`,
	Args: cobra.MaximumNArgs(1),
	// The errors name the path that could not be created; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 {
			if args[0] != daemonArg {
				return fmt.Errorf("install: unknown argument %q", args[0])
			}
		}
		if err := installState(cmd); err != nil {
			return err
		}
		if len(args) == 1 {
			return installDaemon(cmd)
		}
		return nil
	},
}

func installState(cmd *cobra.Command) error {
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
}

var (
	installLaunchAgent = launchd.Install
	awaitLaunchAgent   = launchd.AwaitRunning
	currentExecutable  = os.Executable
)

func installDaemon(cmd *cobra.Command) error {
	binary, err := currentExecutable()
	if err != nil {
		return err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(binary); resolveErr == nil {
		binary = resolved
	}
	specs, err := launchd.DaemonSpecs(binary)
	if err != nil {
		return err
	}
	for _, spec := range specs {
		if err := installLaunchAgent(spec); err != nil {
			return err
		}
		if err := awaitLaunchAgent(spec.Label, spec.Program); err != nil {
			return fmt.Errorf("%s: %w", spec.Label, err)
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "daemon\t%s\nwatchdog\t%s\nbinary\t%s\nlog\t%s\n",
		specs[0].Label, specs[1].Label, specs[0].Program, specs[0].LogPath)
	return nil
}

func init() {
	rootCmd.AddCommand(installCmd)
}
