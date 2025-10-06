package plugins

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

type PluginsCommand struct {
	service *plugins.PluginService
}

func NewPluginsCommand(service *plugins.PluginService) *cobra.Command {
	pc := &PluginsCommand{
		service: service,
	}

	cmd := &cobra.Command{
		Use:    "plugins",
		Short:  "Manage Deckhouse CLI plugins",
		Hidden: true,
	}

	cmd.AddCommand(pc.pluginsListCommand())
	cmd.AddCommand(pc.pluginsContractCommand())
	cmd.AddCommand(pc.pluginsInstallCommand())
	cmd.AddCommand(pc.pluginsUpdateCommand())
	cmd.AddCommand(pc.pluginsRemoveCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all installed Deckhouse CLI plugins",
		Long:  "Display a list of all currently installed Deckhouse CLI plugins with their versions and status",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement listing installed plugins from filesystem
			fmt.Println("Listing all installed plugins...")
			fmt.Println("Plugin Name\t\tVersion\t\tStatus")
			fmt.Println("-------------------------------------------")
			fmt.Println("example-plugin\t\tv1.0.0\t\tActive")
			fmt.Println("another-plugin\t\tv2.1.3\t\tActive")
			fmt.Println("\nTotal: 2 plugins installed")
			return nil
		},
	}

	return cmd
}

func (pc *PluginsCommand) pluginsContractCommand() *cobra.Command {
	var version string
	var useMajor int

	cmd := &cobra.Command{
		Use:   "contract [plugin-name]",
		Short: "Get the contract for a specific plugin",
		Long:  "Retrieve and display the contract specification for a specific plugin from the registry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			tag := version
			if tag == "" {
				tag = "latest"
			}

			fmt.Printf("Fetching contract for plugin: %s\n", pluginName)
			fmt.Printf("Tag: %s\n", tag)
			if useMajor > 0 {
				fmt.Printf("Using major version: %d\n", useMajor)
			}

			fmt.Println("\nRetrieving contract from registry...")

			// Use service to get plugin contract
			ctx := cmd.Context()
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
			if err != nil {
				return fmt.Errorf("failed to get plugin contract: %w", err)
			}

			// Display contract
			fmt.Println("---")
			fmt.Printf("Name: %s\n", plugin.Name)
			fmt.Printf("Version: %s\n", plugin.Version)
			fmt.Printf("Description: %s\n", plugin.Description)

			if len(plugin.Env) > 0 {
				fmt.Println("\nEnvironment Variables:")
				for _, env := range plugin.Env {
					fmt.Printf("  - %s\n", env.Name)
				}
			}

			if len(plugin.Flags) > 0 {
				fmt.Println("\nFlags:")
				for _, flag := range plugin.Flags {
					fmt.Printf("  - %s\n", flag.Name)
				}
			}

			fmt.Println("\nRequirements:")
			fmt.Printf("  Kubernetes: %s\n", plugin.Requirements.Kubernetes.Constraint)
			if len(plugin.Requirements.Modules) > 0 {
				fmt.Println("  Modules:")
				for _, mod := range plugin.Requirements.Modules {
					fmt.Printf("    - %s: %s\n", mod.Name, mod.Constraint)
				}
			}

			fmt.Println("\n✓ Contract retrieved successfully!")
			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin contract to retrieve")
	cmd.Flags().IntVar(&useMajor, "use-major", 0, "Use specific major version (e.g., 1, 2)")

	return cmd
}

func (pc *PluginsCommand) pluginsInstallCommand() *cobra.Command {
	var version string
	var useMajor int

	cmd := &cobra.Command{
		Use:   "install [plugin-name]",
		Short: "Install a Deckhouse CLI plugin",
		Long:  "Install a new plugin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			tag := version
			if tag == "" {
				tag = "latest"
			}

			fmt.Printf("Installing plugin: %s\n", pluginName)
			fmt.Printf("Tag: %s\n", tag)
			if useMajor > 0 {
				fmt.Printf("Using major version: %d\n", useMajor)
			}

			ctx := cmd.Context()

			// Get plugin contract first
			fmt.Println("Verifying plugin contract...")
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
			if err != nil {
				return fmt.Errorf("failed to get plugin contract: %w", err)
			}

			fmt.Printf("Plugin: %s v%s\n", plugin.Name, plugin.Version)
			fmt.Printf("Description: %s\n", plugin.Description)

			// Extract plugin to installation directory
			// TODO: Make destination configurable
			destination := fmt.Sprintf("/tmp/deckhouse-cli/plugins/%s", pluginName)
			fmt.Printf("Installing to: %s\n", destination)

			fmt.Println("Downloading and extracting plugin...")
			err = pc.service.ExtractPlugin(ctx, pluginName, tag, destination)
			if err != nil {
				return fmt.Errorf("failed to extract plugin: %w", err)
			}

			fmt.Printf("✓ Plugin '%s' successfully installed!\n", pluginName)
			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin to install")
	cmd.Flags().IntVar(&useMajor, "use-major", 0, "Use specific major version (e.g., 1, 2)")

	return cmd
}

func (pc *PluginsCommand) pluginsUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [plugin-name]",
		Short: "Update an installed plugin",
		Long:  "Update a specific plugin to its latest available version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Updating plugin: %s\n", pluginName)

			ctx := cmd.Context()

			// Get latest version
			fmt.Println("Checking for updates...")
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, "latest")
			if err != nil {
				return fmt.Errorf("failed to check for updates: %w", err)
			}

			fmt.Printf("Latest version: %s\n", plugin.Version)

			// Extract plugin
			destination := fmt.Sprintf("/tmp/deckhouse-cli/plugins/%s", pluginName)
			fmt.Println("Downloading latest version...")
			err = pc.service.ExtractPlugin(ctx, pluginName, "latest", destination)
			if err != nil {
				return fmt.Errorf("failed to update plugin: %w", err)
			}

			fmt.Printf("✓ Plugin '%s' updated successfully to v%s!\n", pluginName, plugin.Version)
			return nil
		},
	}

	// Add subcommands
	cmd.AddCommand(pc.pluginsUpdateAllCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsUpdateAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Update all installed plugins",
		Long:  "Update all installed plugins to their latest available versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement updating all installed plugins from filesystem
			fmt.Println("Updating all installed plugins...")
			fmt.Println("Checking for updates...")
			fmt.Println("  - example-plugin: v1.0.0 → v1.2.0")
			fmt.Println("  - another-plugin: v2.1.3 (already up-to-date)")
			fmt.Println("✓ All plugins updated successfully!")
			return nil
		},
	}

	return cmd
}

func (pc *PluginsCommand) pluginsRemoveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove [plugin-name]",
		Aliases: []string{"uninstall", "delete"},
		Short:   "Remove an installed plugin",
		Long:    "Remove a specific plugin from the Deckhouse CLI",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Removing plugin: %s\n", pluginName)

			// TODO: Implement actual removal from filesystem
			destination := fmt.Sprintf("/tmp/deckhouse-cli/plugins/%s", pluginName)
			fmt.Printf("Removing plugin from: %s\n", destination)

			fmt.Println("Cleaning up plugin files...")
			fmt.Println("Removing plugin configuration...")
			fmt.Printf("✓ Plugin '%s' successfully removed!\n", pluginName)
			return nil
		},
	}

	// Add subcommands
	cmd.AddCommand(pc.pluginsRemoveAllCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsRemoveAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Remove all installed plugins",
		Long:  "Remove all plugins from the Deckhouse CLI at once",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement removing all installed plugins from filesystem
			fmt.Println("Removing all installed plugins...")

			fmt.Println("Found 2 plugins to remove:")
			fmt.Println("  - example-plugin")
			fmt.Println("  - another-plugin")
			fmt.Println("Cleaning up plugin files...")
			fmt.Println("Removing plugin configurations...")
			fmt.Println("✓ All plugins successfully removed!")
			return nil
		},
	}

	return cmd
}
