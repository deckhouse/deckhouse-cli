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
)

const (
	cmdShort = "Download snapshot manifests to a local directory"

	cmdLong = `Download manifests from a Deckhouse namespace Snapshot into a structured
local directory (archive.json, index.json, COMPLETE, indexes/, manifests/).

The Snapshot must already exist and be in Ready state. If it is not Ready,
the command exits with an error and prints the kubectl command to inspect it.

If the output directory already contains a download for the same snapshot, the
command resumes where it left off. If the snapshot content has changed since
the last download, only the changed nodes are re-fetched.

Re-running the same command after a partial failure automatically resumes the
remaining nodes.

Only manifest download is supported in this release. Volume data (data/)
is reserved for a future phase.`

	cmdExample = `  # Download all manifests from a snapshot into ./my-ns-demo-snapshot/
  d8 snapshot download my-ns demo-snapshot

  # Download to a specific directory
  d8 snapshot download my-ns demo-snapshot -o /tmp/snap-archive

  # Re-run after a partial failure to resume missing nodes
  d8 snapshot download my-ns demo-snapshot -o /tmp/snap-archive

  # Force a clean re-download, ignoring any existing archive
  d8 snapshot download my-ns demo-snapshot -o /tmp/snap-archive --fresh

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
