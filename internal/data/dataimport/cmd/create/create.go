/*
Copyright 2025 Flant JSC

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

package create

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "create"
)

func cmdExamples() string {
	resp := []string{
		"  # Create DataImport",
		fmt.Sprintf("    ... %s my-import -n d8-storage-volume-data-manager -f - --ttl 2m --publish --wffc", cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_import_name",
		Short:   "Create DataImport",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid arguments")
			}
			return nil
		},
	}

	cmd.Flags().StringP("namespace", "n", dataio.Namespace, "data volume namespace")
	cmd.Flags().String("ttl", "2m", "Time to live")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")
	cmd.Flags().StringP("file", "f", "", "PVC manifest file path")
	cmd.Flags().Bool("wffc", false, "Wait for first consumer")

	return cmd
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	name := args[0]
	namespace, _ := cmd.Flags().GetString("namespace")
	ttl, _ := cmd.Flags().GetString("ttl")
	pvcFilePath, _ := cmd.Flags().GetString("file")
	wffc, _ := cmd.Flags().GetBool("wffc")

	flags := cmd.PersistentFlags()
	sc, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}

	rtClient, err := sc.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return err
	}

	data, err := os.ReadFile(pvcFilePath)
	if err != nil {
		return err
	}

	pvcSpec := &v1alpha1.PersistentVolumeClaimTemplateSpec{}
	if err := yaml.Unmarshal(data, pvcSpec); err != nil {
		return fmt.Errorf("parse PVC: %w", err)
	}

	if namespace == "" {
		if pvcSpec.Namespace == "" {
			return fmt.Errorf("namespace is required")
		}
		namespace = pvcSpec.Namespace
	}

	publishFlag, err := dataio.ParsePublishFlag(cmd.Flags())
	if err != nil {
		return err
	}

	publish, err := dataio.ResolvePublish(ctx, publishFlag, rtClient, sc, log)
	if err != nil {
		return err
	}

	if err := util.CreateDataImport(ctx, name, namespace, ttl, publish, wffc, pvcSpec, rtClient); err != nil {
		return err
	}
	log.Info("DataImport created", slog.String("name", name), slog.String("namespace", namespace))
	return nil
}
