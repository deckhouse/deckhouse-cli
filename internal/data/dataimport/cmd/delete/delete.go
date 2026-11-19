package delete

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "delete"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf("  ... -n NAMESPACE %s DATAIMPORT_NAME", cmdName),
	}

	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_import_name",
		Short:   "Delete dataimport kubernetes resource",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(_ *cobra.Command, args []string) error {
			_, err := parseArgs(args)
			return err
		},
	}

	dataio.AddNamespaceFlag(cmd)

	return cmd
}

func parseArgs(args []string) ( /*diName*/ string, error) {
	if len(args) == 1 {
		return args[0], nil
	}

	return "", fmt.Errorf("invalid arguments")
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	diName, err := parseArgs(args)
	if err != nil {
		return err
	}

	flags := cmd.Flags()

	safeClient, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}

	namespace, err := dataio.ResolveNamespace(cmd, safeClient)
	if err != nil {
		return err
	}

	_, rtClient, err := util.ResolveClientFunc(ctx, safeClient, namespace, log)
	if err != nil {
		return err
	}

	err = util.DeleteDataImport(ctx, diName, namespace, rtClient)
	if err != nil {
		return err
	}

	log.Info("Deleted DataImport", slog.String("name", diName), slog.String("namespace", namespace))

	return nil
}
