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

package local

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/localscan"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/treeview"
)

// NewDescribeCommand builds the `d8 snapshot local describe` cobra command.
func NewDescribeCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "describe <DIR>",
		Short:         "Show the structure of a locally downloaded snapshot archive as a tree",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		Example: `  # Show the structure of a downloaded snapshot archive
  d8 snapshot local describe ./my-snap

  # Show the structure of an archive at an absolute path
  d8 snapshot local describe /backups/my-snap`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDescribe(log, cmd, args)
		},
	}

	return cmd
}

// runDescribe resolves the archive directory, scans it with localscan, and
// renders the resulting node tree to stdout via the shared treeview renderer.
func runDescribe(log *slog.Logger, cmd *cobra.Command, args []string) error {
	absDir, err := filepath.Abs(args[0])
	if err != nil {
		return fmt.Errorf("resolving archive directory %q: %w", args[0], err)
	}

	log.Debug("scanning archive", slog.String("dir", absDir))

	root, err := localscan.Scan(absDir)
	if err != nil {
		return fmt.Errorf("scanning archive directory %q: %w", absDir, err)
	}

	tvRoot := toTreeViewNode(absDir, root)

	if err := treeview.Render(cmd.OutOrStdout(), tvRoot); err != nil {
		return fmt.Errorf("rendering snapshot tree: %w", err)
	}

	return nil
}

// toTreeViewNode maps a localscan.Node and its descendants into a treeview.Node.
// The root node label is the resolved absolute directory path; child node labels
// are the directory basename from the archive path (e.g. "volumesnapshot_mypvc").
// Volume leaf labels are the captured PVC names from each VolumeInfo.
func toTreeViewNode(rootDir string, n *localscan.Node) treeview.Node {
	label := nodeLabel(rootDir, n)

	children := make([]treeview.Node, 0, len(n.Children))

	for _, child := range n.Children {
		children = append(children, toTreeViewNode(rootDir, child))
	}

	volumes := make([]string, 0, len(n.Volumes))

	for _, v := range n.Volumes {
		volumes = append(volumes, v.Target.Name)
	}

	return treeview.Node{
		Label:    label,
		Children: children,
		Volumes:  volumes,
	}
}

// nodeLabel returns the display label for a localscan node.
// The archive root uses the resolved directory path; all other nodes use the
// directory basename (e.g. "volumesnapshot_mypvc") from the node's relative path.
func nodeLabel(rootDir string, n *localscan.Node) string {
	if n.Path == "." {
		return rootDir
	}

	return filepath.Base(n.Path)
}
