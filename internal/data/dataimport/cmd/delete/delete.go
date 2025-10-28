package delete

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	"github.com/spf13/cobra"
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
		Args: func(cmd *cobra.Command, args []string) error {
			_, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-storage-volume-data-manager", "data volume namespace")

	return cmd
}

func parseArgs(args []string) (diName string, err error) {
	if len(args) == 1 {
		return args[0], nil
	}

	return "", fmt.Errorf("invalid arguments")
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	namespace, _ := cmd.Flags().GetString("namespace")

	diName, err := parseArgs(args)
	if err != nil {
		return err
	}

	flags := cmd.PersistentFlags()
	safeClient, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}
	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
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
