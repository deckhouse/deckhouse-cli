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
	"strings"
	"time"

	"github.com/spf13/cobra"

	api "github.com/deckhouse/deckhouse-cli/internal/dataimport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	cmdName = "create"
)

func cmdExamples() string {
	resp := []string{
		"  # Create DataImport with PVC template size 10Gi, RWO, FS",
		fmt.Sprintf("    ... %s my-import --size=10Gi --access=ReadWriteOnce --mode=Filesystem", cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_import_name",
		Short:   "Create dataimport kubernetes resource",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid arguments")
			}
			return nil
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().String("ttl", "2m", "Time to live")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")
	cmd.Flags().Bool("wffc", false, "Wait for first consumer")
	cmd.Flags().String("size", "", "PVC size, e.g. 10Gi")
	cmd.Flags().String("access", "ReadWriteOnce", "PVC access mode")
	cmd.Flags().String("class", "", "StorageClassName")
	cmd.Flags().String("mode", "Filesystem", "PVC volume mode: Filesystem|Block")

	return cmd
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	name := args[0]
	namespace, _ := cmd.Flags().GetString("namespace")
	ttl, _ := cmd.Flags().GetString("ttl")
	publish, _ := cmd.Flags().GetBool("publish")
	wffc, _ := cmd.Flags().GetBool("wffc")
	size, _ := cmd.Flags().GetString("size")
	access, _ := cmd.Flags().GetString("access")
	class, _ := cmd.Flags().GetString("class")
	mode, _ := cmd.Flags().GetString("mode")

	pvcTpl := api.PersistentVolumeClaimTemplateSpec{}
	// Minimal parsing; detailed validation can be added later
	if size != "" {
		pvcTpl.Resources.Requests = api.ResourceList{api.ResourceStorage: resource.MustParse(size)}
	}
	if access != "" {
		pvcTpl.AccessModes = []api.PersistentVolumeAccessMode{api.PersistentVolumeAccessMode(access)}
	}
	if class != "" {
		pvcTpl.StorageClassName = &class
	}
	if mode != "" {
		m := api.PersistentVolumeMode(mode)
		pvcTpl.VolumeMode = &m
	}

	flags := cmd.PersistentFlags()
	sc, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}
	rtClient, err := sc.NewRTClient(api.AddToScheme)
	if err != nil {
		return err
	}

	if err := util.CreateDataImport(ctx, name, namespace, ttl, publish, wffc, pvcTpl, rtClient); err != nil {
		return err
	}
	log.Info("DataImport created", slog.String("name", name), slog.String("namespace", namespace))
	return nil
}

// no extra helpers needed
