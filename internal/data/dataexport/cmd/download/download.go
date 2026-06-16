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

package download

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	"github.com/deckhouse/deckhouse-cli/internal/data/fswalk"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "download"
)

func cmdExamples() string {
	resp := []string{
		"  # Start exporter + Download + Stop for Filesystem",
		fmt.Sprintf("    ... %s [flags] kind/volume_name path/file.ext [-o out_file.ext]", cmdName),
		fmt.Sprintf("    ... %s -n target-namespace pvc/my-file-volume mydir/testdir/file.txt -o file.txt", cmdName),
		"  # Start exporter + Download + Stop for Block",
		fmt.Sprintf("    ... %s [flags] kind/volume_name [-o out_file.ext]", cmdName),
		fmt.Sprintf("    ... %s -n target-namespace vs/my-vs-volume -o file.txt", cmdName),
		"  # Start exporter + Download + Stop for VirtualDisk (Block)",
		fmt.Sprintf("    ... %s -n target-namespace vd/my-virtualdisk -o file.img", cmdName),
		"  # Start exporter + Download + Stop for VirtualDiskSnapshot (Block)",
		fmt.Sprintf("    ... %s -n target-namespace vds/my-virtualdisk-snapshot -o file.img", cmdName),
	}

	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] [KIND/]data_export_name [path/file.ext]",
		Short:   "Download exported data",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(_ *cobra.Command, args []string) error {
			_, _, err := dataio.ParseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", dataio.Namespace, "data volume namespace")
	cmd.Flags().StringP("output", "o", "", "file to save data (default: same as resource)") // TODO support /dev/stdout
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")
	cmd.Flags().String("ttl", "2m", "Time to live for auto-created DataExport")
	cmd.Flags().Bool("cleanup", false, "Delete auto-created DataExport without prompting (--cleanup=true to delete, --cleanup=false to keep)")

	return cmd
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	namespace, _ := cmd.Flags().GetString("namespace")
	dstPath, _ := cmd.Flags().GetString("output")
	ttl, _ := cmd.Flags().GetString("ttl")
	cleanup, _ := cmd.Flags().GetBool("cleanup")
	cleanupExplicit := cmd.Flags().Changed("cleanup")

	dataName, srcPath, err := dataio.ParseArgs(args)
	if err != nil {
		return fmt.Errorf("arguments parsing error: %s", err.Error())
	}

	flags := cmd.PersistentFlags()
	safeClient.SupportNoAuth = false

	sClient, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}

	rtClient, err := sClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return err
	}

	publishFlag, err := dataio.ParsePublishFlag(cmd.Flags())
	if err != nil {
		return err
	}

	publish, err := dataio.ResolvePublish(ctx, publishFlag, rtClient, sClient, log)
	if err != nil {
		return err
	}

	deName, err := util.CreateDataExporterIfNeededFunc(ctx, log, dataName, namespace, publish, ttl, rtClient)
	if err != nil {
		return err
	}

	log.Info("DataExport created", slog.String("name", deName), slog.String("namespace", namespace))

	url, volumeMode, subClient, err := util.PrepareDownloadFunc(ctx, log, deName, namespace, publish, sClient)
	if err != nil {
		return err
	}

	switch volumeMode {
	case "Filesystem":
		if srcPath == "" {
			return fmt.Errorf("invalid source path: '%s'", srcPath)
		}

		if dstPath == "" {
			pathList := strings.Split(srcPath, "/")
			dstPath = pathList[len(pathList)-1]
		}
	case "Block":
		srcPath = ""

		if dstPath == "" {
			dstPath = deName
		}
	default:
		return fmt.Errorf("%w: %s", dataio.ErrUnsupportedVolumeMode, volumeMode)
	}

	log.Info("Start downloading", slog.String("url", url+srcPath), slog.String("dstPath", dstPath))

	sem := make(chan struct{}, fswalk.DefaultConcurrency)

	err = fswalk.RecursiveDownload(ctx, subClient, log, sem, url, srcPath, dstPath)
	if err != nil {
		log.Error("Not all files have been downloaded", slog.String("error", err.Error()))
	} else {
		log.Info("All files have been downloaded", slog.String("dst_path", dstPath))
	}

	// Clean up auto-created DataExport
	if deName != dataName && dataio.ShouldCleanup(cleanup, cleanupExplicit) {
		if err := util.DeleteDataExport(ctx, deName, namespace, rtClient); err != nil {
			log.Warn("Failed to delete DataExport", slog.String("name", deName), slog.String("error", err.Error()))
		}
	}

	return nil
}
