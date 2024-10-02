package lifecycle

import (
	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/deckhouse/deckhouse-cli/internal/virtualization/templates"
)

func NewMigrateCommand(clientConfig clientcmd.ClientConfig) *cobra.Command {
	lifecycle := NewLifecycle(Migrate, clientConfig)
	migrate := string(Migrate)
	cmd := &cobra.Command{
		Use:     migrate + " (VirtualMachine)",
		Short:   cases.Title(language.English).String(migrate) + " a virtual machine.",
		Example: lifecycle.Usage(),
		Args:    templates.ExactArgs(migrate, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return lifecycle.Run(args)
		},
	}
	AddCommandlineArgs(cmd.Flags(), &lifecycle.opts)
	cmd.SetUsageTemplate(templates.UsageTemplate())
	return cmd
}
