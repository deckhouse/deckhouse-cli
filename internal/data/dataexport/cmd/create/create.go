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

package create

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "create"
)

func cmdExamples() string {
	resp := []string{
		"  # Start data exporting for PVC 'test-pvc-name'",
		fmt.Sprintf("    ... %s export-name pvc/test-pvc-name", cmdName),
		"  # Start data exporting with extra flags",
		fmt.Sprintf("    ... %s --kubeconfig='kube_tmp.conf' -n target-namespace --ttl 17m export-name pvc/test-pvc-name", cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_export_name volume_type/volume_name",
		Short:   "Create dataexport kubernetes resource",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			_, _, _, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().String("ttl", "2m", "Time to live")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")

	return cmd
}

func parseArgs(args []string) (deName, volumeKind, volumeName string, err error) {
	if len(args) != 2 {
		err = fmt.Errorf("invalid arguments")
		return
	}
	deName = args[0]
	resourceTypeAndName := strings.Split(args[1], "/")
	if len(resourceTypeAndName) != 2 {
		err = fmt.Errorf("invalid volume format, expect: <type>/<name>")
		return
	}
	volumeKind, volumeName = strings.ToLower(resourceTypeAndName[0]), resourceTypeAndName[1]
	switch volumeKind {
	case "pvc", "persistentvolumeclaim":
		volumeKind = dataio.PersistentVolumeClaimKind
	case "vs", "volumesnapshot":
		volumeKind = dataio.VolumeSnapshotKind
	case "vd", "virtualdisk":
		volumeKind = dataio.VirtualDiskKind
	case "vds", "virtualdisksnapshot":
		volumeKind = dataio.VirtualDiskSnapshotKind
	default:
		err = fmt.Errorf("invalid volume type; valid values: pvc | persistentvolumeclaim | vs | volumesnapshot | vd | virtualdisk | vds | virtualdisksnapshot")
		return
	}

	return
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	namespace, _ := cmd.Flags().GetString("namespace")
	ttl, _ := cmd.Flags().GetString("ttl")
	publish, _ := cmd.Flags().GetBool("publish")

	deName, volumeKind, volumeName, err := parseArgs(args)
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

	err = util.CreateDataExport(ctx, deName, namespace, ttl, volumeKind, volumeName, publish, rtClient)
	if err != nil {
		return err
	}

	log.Info("DataExport created", slog.String("name", deName), slog.String("namespace", namespace))
	return nil
}
