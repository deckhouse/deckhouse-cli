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

// Package download implements the `d8 snapshot download` sub-command.
package download

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "download"
)

const (
	flagOutput     = "output"
	flagNode       = "node"
	flagObject     = "object"
	flagFresh      = "fresh"
	flagRetries    = "retries"
	flagRetryDelay = "retry-delay"
	flagManifests  = "manifests"
	flagVolumes    = "volumes"
	flagTTL        = "ttl"
)

const (
	cmdShort = "Download snapshot manifests and volume data to a local directory"

	cmdLong = `Download manifests and volume data from a Deckhouse namespace Snapshot
into a structured local directory:

  archive.json   - archive identity and selection metadata
  index.json     - capabilities, catalog paths, summary counts
  COMPLETE       - sentinel written last; absent means incomplete
  indexes/       - nodes.jsonl, objects.jsonl, progress.jsonl, volumes.jsonl
  manifests/     - content-addressed manifest blobs
  data/          - downloaded volume data (block: .img files; filesystem: dirs)

The Snapshot must already exist and be in Ready state. If it is not Ready,
the command exits with an error and prints the kubectl command to inspect it.

If the output directory already contains a download for the same snapshot, the
command resumes where it left off.

By default both manifests and volume data are downloaded. Use --manifests=false
or --volumes=false to skip either. Volume data is downloaded by creating a
temporary DataExport (kind=VolumeSnapshotContent) per volume, waiting for it
to become Ready, streaming the data, and deleting the DataExport.`

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
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " <namespace> <snapshot>",
		Short:   cmdShort,
		Long:    cmdLong,
		Example: cmdExample,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args, log)
		},
	}

	cmd.Flags().StringP(flagOutput, "o", "", "destination directory (default: ./<namespace>-<snapshot>)")
	cmd.Flags().String(flagNode, "", "download only the subtree rooted at this node ID (e.g. VirtualDiskSnapshot--root-disk)")
	cmd.Flags().String(flagObject, "", "download a single object, format: <apiVersion>/<Kind>/<name> (e.g. apps/v1/Deployment/my-deploy)")
	cmd.Flags().Bool(flagFresh, false, "overwrite an existing archive without prompting")
	cmd.Flags().Int(flagRetries, 3, "number of download attempts per node before giving up")
	cmd.Flags().Duration(flagRetryDelay, 2*time.Second, "base delay between retries (doubles on each attempt)")
	cmd.Flags().Bool(flagManifests, true, "download Kubernetes resource manifests (default true; use --manifests=false to skip)")
	cmd.Flags().Bool(flagVolumes, true, "download volume data via DataExport (default true; use --volumes=false to skip)")
	cmd.Flags().String(flagTTL, "", "TTL for auto-created DataExport objects during volume download (e.g. 1h; default 30m)")

	return cmd
}

func run(cmd *cobra.Command, args []string, log *slog.Logger) error {
	namespace, snapshotName := args[0], args[1]

	outputDir, _ := cmd.Flags().GetString(flagOutput)
	nodeID, _ := cmd.Flags().GetString(flagNode)
	objectFilter, _ := cmd.Flags().GetString(flagObject)
	fresh, _ := cmd.Flags().GetBool(flagFresh)
	retries, _ := cmd.Flags().GetInt(flagRetries)
	retryDelay, _ := cmd.Flags().GetDuration(flagRetryDelay)
	includeManifests, _ := cmd.Flags().GetBool(flagManifests)
	includeVolumes, _ := cmd.Flags().GetBool(flagVolumes)
	dataExportTTL, _ := cmd.Flags().GetString(flagTTL)

	if outputDir == "" {
		outputDir = fmt.Sprintf("%s-%s", namespace, snapshotName)
	}

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

	opts := pipeline.Options{
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
	}

	return pipeline.Run(ctx, sClient, rtClient, opts, log)
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
