/*
Copyright 2026 Flant JSC

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
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	snapshotlog "github.com/deckhouse/deckhouse-cli/internal/snapshot/log"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdLong = `Download manifests and volume data from a Deckhouse namespace Snapshot
into a structured local directory:

  archive.json   - archive identity and selection metadata
  index.json     - capabilities, catalog paths, summary counts (incl. volumeModel)
  COMPLETE       - sentinel written last; absent means incomplete
  indexes/       - nodes.jsonl, objects.jsonl, progress.jsonl, volumes.jsonl
  manifests/     - content-addressed gzip manifest blobs
  data/          - downloaded volume data

Volume data on-disk format (default --volume-compression=gzip):
  Block volumes:      data/<nodeID>/<vsc>.img.gz
                      Multi-member gzip; each member is ~64 MiB.
                      Resume: byte-level (Range header + truncate to last checkpoint).
  Filesystem volumes: data/<nodeID>/<pvcName>/<file>.gz (each file gzip'd individually)
                      Resume: per-file (existing .gz files are skipped).

Use --volume-compression=none to store volume data uncompressed.
  Block volumes:      data/<nodeID>/<vsc>.img  (HTTP Range resume)
  Filesystem volumes: data/<nodeID>/<pvcName>/<file>  (volume-level resume only)

The Snapshot must already exist and be in Ready state. If it is not Ready,
the command exits with an error and prints the kubectl command to inspect it.

If the output directory already contains a download for the same snapshot, the
command resumes where it left off (per-file for filesystem, per-byte for block gzip).

Volume data is downloaded by creating a temporary shadow VolumeSnapshotContent
+ VolumeSnapshot pair that points at the original snapshot handle, creating a
DataExport (kind=VolumeSnapshot) against it, streaming the data, and cleaning
up all temporary objects afterwards.`

	cmdExample = `  # Download manifests and volumes from a snapshot
  d8 snapshot download my-ns demo-snapshot

  # Download to a specific directory
  d8 snapshot download my-ns demo-snapshot -o /tmp/snap-archive

  # Resume a partial download
  d8 snapshot download my-ns demo-snapshot -o /tmp/snap-archive

  # Force a clean re-download, ignoring any existing archive
  d8 snapshot download my-ns demo-snapshot -o /tmp/snap-archive --fresh

  # Download only manifests (skip volume data)
  d8 snapshot download my-ns demo-snapshot --volumes=false

  # Download only volume data (skip manifests)
  d8 snapshot download my-ns demo-snapshot --manifests=false

  # Download only a subtree rooted at a specific node
  d8 snapshot download my-ns demo-snapshot --node VirtualDiskSnapshot--root-disk

  # Download a single object (client-side filter)
  d8 snapshot download my-ns demo-snapshot --object apps/v1/Deployment/my-deploy`
)

// NewCommand returns the cobra command for `d8 snapshot download`.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "download <namespace> <snapshot>",
		Short:   "Download snapshot manifests and volume data to a local directory",
		Long:    cmdLong,
		Example: cmdExample,
		Args:    cobra.ExactArgs(2),
		RunE:    run,
	}

	cmd.Flags().StringP("output", "o", "", "destination directory (default: ./<namespace>-<snapshot>)")
	cmd.Flags().String("node", "", "download only the subtree rooted at this node ID (e.g. VirtualDiskSnapshot--root-disk)")
	cmd.Flags().String("object", "", "download a single object, format: <apiVersion>/<Kind>/<name> (e.g. apps/v1/Deployment/my-deploy)")
	cmd.Flags().Bool("fresh", false, "overwrite an existing archive without prompting")
	cmd.Flags().Int("retries", 3, "number of download attempts per node before giving up")
	cmd.Flags().Duration("retry-delay", 2*time.Second, "base delay between retries (doubles on each attempt)")
	cmd.Flags().Bool("manifests", true, "download Kubernetes resource manifests (default true; use --manifests=false to skip)")
	cmd.Flags().Bool("volumes", true, "download volume data via DataExport (default true; use --volumes=false to skip)")
	cmd.Flags().String("ttl", "", "TTL for auto-created DataExport objects during volume download (e.g. 1h; default 30m)")
	cmd.Flags().String("volume-compression", "gzip", `compression for downloaded volume data: "gzip" (default) or "none"`)

	return cmd
}

func run(cmd *cobra.Command, args []string) error {
	log := snapshotlog.New()
	opts := newOptions(cmd, args)

	safeClient.SupportNoAuth = false

	sClient, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("build kube client: %w", err)
	}

	rtClient, err := sClient.NewRTClient()
	if err != nil {
		return fmt.Errorf("build runtime client: %w", err)
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	return pipeline.Run(ctx, sClient, rtClient, opts, log)
}

func newOptions(cmd *cobra.Command, args []string) pipeline.Options {
	namespace, snapshotName := args[0], args[1]

	outputDir, _ := cmd.Flags().GetString("output")
	nodeID, _ := cmd.Flags().GetString("node")
	objectFilter, _ := cmd.Flags().GetString("object")
	fresh, _ := cmd.Flags().GetBool("fresh")
	retries, _ := cmd.Flags().GetInt("retries")
	retryDelay, _ := cmd.Flags().GetDuration("retry-delay")
	includeManifests, _ := cmd.Flags().GetBool("manifests")
	includeVolumes, _ := cmd.Flags().GetBool("volumes")
	dataExportTTL, _ := cmd.Flags().GetString("ttl")
	volumeCompression, _ := cmd.Flags().GetString("volume-compression")

	if outputDir == "" {
		outputDir = fmt.Sprintf("%s-%s", namespace, snapshotName)
	}

	return pipeline.Options{
		Namespace:         namespace,
		SnapshotName:      snapshotName,
		OutputDir:         outputDir,
		NodeFilter:        nodeID,
		ObjectFilter:      objectFilter,
		Fresh:             fresh,
		Retries:           retries,
		RetryDelay:        retryDelay,
		OverwritePromptFn: overwritePrompt(),
		IncludeManifests:  includeManifests,
		IncludeVolumes:    includeVolumes,
		DataExportTTL:     dataExportTTL,
		VolumeCompression: volumeCompression,
	}
}

// overwritePrompt returns a prompt function that asks the user interactively
// whether to overwrite the existing directory. When stdin is not a TTY, it
// returns nil so the pipeline emits an actionable error instructing the caller
// to use --fresh.
func overwritePrompt() func(string) bool {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return nil
	}

	return func(d string) bool {
		fmt.Fprintf(os.Stderr, "\nDirectory %q already contains a different snapshot archive.\nOverwrite? [y/N] ", d)

		sc := bufio.NewScanner(os.Stdin)
		if !sc.Scan() {
			return false
		}

		return strings.ToLower(strings.TrimSpace(sc.Text())) == "y"
	}
}
