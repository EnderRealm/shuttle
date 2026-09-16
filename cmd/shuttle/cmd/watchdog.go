package cmd

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/launchd"
)

const watchdogInterval = 5 * time.Second

var (
	watchdogProcess   = launchd.ProcessFor
	watchdogKickstart = launchd.Kickstart
)

var watchdogCmd = &cobra.Command{
	Use:           "watchdog",
	Short:         "Keep the launchd scheduler process running",
	Hidden:        true,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New(cmd.ErrOrStderr(), "", log.LstdFlags)
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ticker := time.NewTicker(watchdogInterval)
		defer ticker.Stop()
		for {
			watchDaemonOnce(logger)
			select {
			case <-ctx.Done():
				logger.Print("watchdog stop")
				return nil
			case <-ticker.C:
			}
		}
	},
}

func watchDaemonOnce(logger *log.Logger) {
	_, running, err := watchdogProcess(launchd.DaemonLabel)
	if err != nil {
		logger.Printf("watchdog: inspect %s: %v", launchd.DaemonLabel, err)
		return
	}
	if running {
		return
	}
	if err := watchdogKickstart(launchd.DaemonLabel); err != nil {
		logger.Printf("watchdog: kickstart %s: %v", launchd.DaemonLabel, err)
		return
	}
	logger.Printf("watchdog: kickstarted %s", launchd.DaemonLabel)
}

func init() {
	rootCmd.AddCommand(watchdogCmd)
}
