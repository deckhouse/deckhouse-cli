package system

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"

	"github.com/deckhouse/deckhouse-cli/cmd/plugins"
	"github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var systemLong = templates.LongDesc(`
Operate system options in DKP.

© Flant JSC 2025`)

func NewCommand(logger *dkplog.Logger) *cobra.Command {
	pc := &plugins.PluginsCommand{
		Logger: logger,
	}

	systemCmd := &cobra.Command{
		Use:     "system",
		Short:   "System operations",
		Aliases: []string{"s", "ps", "platform"},
		Long:    systemLong,
		PreRun: func(_ *cobra.Command, _ []string) {
			// init plugin services for subcommands after flags are parsed
			pc.InitPluginServices()
		},
		Run: func(cmd *cobra.Command, args []string) {
			installed, err := checkInstalled()
			if err != nil {
				fmt.Println("Error checking installed:", err)
				return
			}
			if !installed {
				fmt.Println("Not installed, installing...")
				err = pc.InstallPlugin(cmd.Context(), "system", "", -1)
				if err != nil {
					fmt.Println("Error installing:", err)
					return
				}
				fmt.Println("Installed successfully")
			}

			pluginPath := path.Join(flags.DeckhousePluginsDir, "plugins", "system")
			pluginBinaryPath := path.Join(pluginPath, "current")
			command := exec.CommandContext(cmd.Context(), pluginBinaryPath, args...)
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr

			err = command.Run()
			if err != nil {
				logger.Warn("Failed to run plugin", slog.String("error", err.Error()))
			}
		},
	}

	return systemCmd
}

func checkInstalled() (bool, error) {
	installedFile := path.Join(flags.DeckhousePluginsDir, "plugins", "system", "current")
	_, err := os.Stat(installedFile)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
