package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
	"shuttle/internal/ledger"
	"shuttle/internal/output"
)

// The arguments the help advertises for the launchd install (§13).
const (
	daemonArg  = "daemon"
	updaterArg = "updater"
)

var installCmd = &cobra.Command{
	Use:   "install",
	Short: "Create the state root and the reports repo",
	Long: `Create the state root and the reports repo (docs/phase1-design.md §9.3).

The state root is $SHUTTLE_HOME, else ~/.shuttle. Its reports directory is
initialised as a git repository with no remote; adding one is the operator's
choice. Both steps are idempotent, so running install against a live tree
changes nothing.

With the daemon argument, install also writes and bootstraps the launchd jobs
(§13): the daemon, its watchdog and the updater, and the daily state export
where config.json sets export.destination. That restarts the daemon whatever
it is running; the updater's own restart drains first. With the updater
argument, it does so for the updater's job alone, which is the recovery when an
update left the updater down.`,
	Args: cobra.MaximumNArgs(1),
	// The errors name the path that could not be created; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 {
			if args[0] != daemonArg && args[0] != updaterArg {
				return fmt.Errorf("install: unknown argument %q", args[0])
			}
		}
		if err := installState(cmd); err != nil {
			return err
		}
		switch {
		case len(args) == 0:
			return nil
		case args[0] == updaterArg:
			return installUpdater(cmd)
		}
		return installDaemon(cmd)
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
	installedProgram   = launchd.InstalledProgram
)

// resolvedExecutable is the running binary's canonical path: what install
// pins the plists to. Anything that acts on the installed jobs instead goes
// through pinnedExecutable, since a checkout or `go run` build is not them.
func resolvedExecutable() (string, error) {
	binary, err := currentExecutable()
	if err != nil {
		return "", err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(binary); resolveErr == nil {
		binary = resolved
	}
	return binary, nil
}

// pinnedExecutable is the running binary, refused unless it is the one every
// installed shuttle job pins. Run from elsewhere, an update would install over
// that binary and a restart would re-pin the jobs to it, leaving the installed
// one stale, or under `go run` pinning a temp file deleted on exit. With no
// job installed there is no pin to hold it to.
func pinnedExecutable() (string, error) {
	binary, err := resolvedExecutable()
	if err != nil {
		return "", err
	}
	for _, label := range []string{launchd.DaemonLabel, launchd.WatchdogLabel, launchd.UpdaterLabel, launchd.ExportLabel} {
		program, installed, err := installedProgram(label)
		if err != nil {
			return "", err
		}
		if !installed {
			continue
		}
		pinned := program
		if resolved, resolveErr := filepath.EvalSymlinks(program); resolveErr == nil {
			pinned = resolved
		}
		if pinned != binary {
			plist, err := launchd.PlistPath(label)
			if err != nil {
				return "", err
			}
			return "", fmt.Errorf("refusing to run as %s: %s pins %s, the installed shuttle; run %s instead", binary, plist, program, program)
		}
	}
	return binary, nil
}

// launchSpecs is the launchd jobs of the running binary that want keeps.
func launchSpecs(want func(launchd.Spec) bool) ([]launchd.Spec, error) {
	binary, err := resolvedExecutable()
	if err != nil {
		return nil, err
	}
	specs, err := launchd.DaemonSpecs(binary)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(specs, func(s launchd.Spec) bool { return !want(s) }), nil
}

// bootstrapSpecs replaces each job and waits for a process under the binary.
func bootstrapSpecs(specs []launchd.Spec) error {
	for _, spec := range specs {
		if err := installLaunchAgent(spec); err != nil {
			return err
		}
		if err := awaitLaunchAgent(spec.Label, spec.Program); err != nil {
			return fmt.Errorf("%s: %w", spec.Label, err)
		}
	}
	return nil
}

func installDaemon(cmd *cobra.Command) error {
	specs, err := launchSpecs(func(launchd.Spec) bool { return true })
	if err != nil {
		return err
	}
	// Before the others, so the updater's job is still the last replaced.
	if err := installExport(cmd, specs[0].Program); err != nil {
		return err
	}
	if err := bootstrapSpecs(specs); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "daemon\t%s\nwatchdog\t%s\nupdater\t%s\nbinary\t%s\nlog\t%s\nupdater log\t%s\n",
		specs[0].Label, specs[1].Label, specs[2].Label, specs[0].Program, specs[0].LogPath, specs[2].LogPath)
	return nil
}

// installExport is syncExport for `install daemon`, naming what it installed.
func installExport(cmd *cobra.Command, binary string) error {
	spec, destination, err := syncExport(binary)
	if err != nil || spec == nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "export\t%s\ndestination\t%s\nexport log\t%s\n", spec.Label, destination, spec.LogPath)
	return nil
}

// syncExport installs the daily export job under binary when config.json
// names an export destination, and removes one installed before when it names
// none, so the job follows the config. Both `install daemon` and the updater's
// rebootstrap run it, the latter because launchd refuses to spawn a binary
// signed differently from the one it bootstrapped, and the export job is
// spawned afresh every day. It is not awaited: between its daily starts the
// job has no process. It returns the job installed, or nil, and the
// destination.
func syncExport(binary string) (*launchd.Spec, string, error) {
	cfg, err := config.Load()
	if errors.Is(err, fs.ErrNotExist) {
		return nil, "", uninstallLaunchAgent(launchd.ExportLabel)
	}
	if err != nil {
		return nil, "", err
	}
	if cfg.Export.Destination == "" {
		return nil, "", uninstallLaunchAgent(launchd.ExportLabel)
	}
	spec, err := launchd.ExportSpec(binary)
	if err != nil {
		return nil, "", err
	}
	if err := installLaunchAgent(spec); err != nil {
		return nil, "", err
	}
	return &spec, cfg.Export.Destination, nil
}

func installUpdater(cmd *cobra.Command) error {
	specs, err := launchSpecs(isUpdaterSpec)
	if err != nil {
		return err
	}
	if err := bootstrapSpecs(specs); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "updater\t%s\nbinary\t%s\nlog\t%s\n", specs[0].Label, specs[0].Program, specs[0].LogPath)
	return nil
}

func isUpdaterSpec(s launchd.Spec) bool { return s.Label == launchd.UpdaterLabel }

func init() {
	rootCmd.AddCommand(installCmd)
}
