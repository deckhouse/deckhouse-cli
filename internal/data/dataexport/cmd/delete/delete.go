/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package delete

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "delete"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf("  ... -n target-namespace %s my-volume", cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_export_name",
		Short:   "Delete dataexport kubernetes resource",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			_, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")

	return cmd
}

func parseArgs(args []string) (deName string, err error) {
	if len(args) == 1 {
		return args[0], nil
	}

	return "", fmt.Errorf("invalid arguments")
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	namespace, _ := cmd.Flags().GetString("namespace")

	deName, err := parseArgs(args)
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

	err = util.DeleteDataExport(ctx, deName, namespace, rtClient)
	if err != nil {
		return err
	}

	log.Info("Deleted DataExport", slog.String("name", deName), slog.String("namespace", namespace))
	return nil
}
