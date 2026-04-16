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

package commands

import (
	"sort"
	"strings"

	vaultcommand "github.com/hashicorp/vault/command"
	"github.com/spf13/cobra"
)

type commandNode struct {
	name     string
	synopsis string
	children map[string]*commandNode
}

func sortedSynopsisKeys(synopses map[string]string) []string {
	keys := make([]string, 0, len(synopses))
	for k := range synopses {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func sortedCommandKeys(nodes map[string]*commandNode) []string {
	keys := make([]string, 0, len(nodes))
	for k := range nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func extendPath(prefix []string, name string) []string {
	path := make([]string, len(prefix)+1)
	copy(path, prefix)
	path[len(prefix)] = name

	return path
}

// buildCommandTree converts a flat map of space-delimited command paths into
// a tree structure suitable for creating nested cobra commands.
func buildCommandTree(synopses map[string]string) map[string]*commandNode {
	roots := make(map[string]*commandNode)

	for _, key := range sortedSynopsisKeys(synopses) {
		parts := strings.Fields(key)
		if len(parts) == 0 {
			continue
		}
		synopsis := synopses[key]

		nodes := roots
		for i, part := range parts {
			node, ok := nodes[part]
			if !ok {
				node = &commandNode{
					name:     part,
					children: make(map[string]*commandNode),
				}
				nodes[part] = node
			}
			if i == len(parts)-1 {
				node.synopsis = synopsis
			}
			nodes = node.children
		}
	}

	return roots
}

// buildCobraCommands recursively converts a commandNode tree into cobra commands.
// Each command delegates execution to vaultcommand.Run with the appropriate
// command path prefix, preserving the original vault CLI behavior.
func buildCobraCommands(nodes map[string]*commandNode, pathPrefix []string) []*cobra.Command {
	keys := sortedCommandKeys(nodes)
	cmds := make([]*cobra.Command, 0, len(keys))

	for _, key := range keys {
		node := nodes[key]
		vaultPath := extendPath(pathPrefix, node.name)

		cmd := &cobra.Command{
			Use:                node.name,
			Short:              node.synopsis,
			SilenceErrors:      true,
			SilenceUsage:       true,
			DisableFlagParsing: true,
			Run: func(path []string) func(*cobra.Command, []string) {
				return func(_ *cobra.Command, args []string) {
					fullArgs := make([]string, 0, len(path)+len(args))
					fullArgs = append(fullArgs, path...)
					fullArgs = append(fullArgs, args...)
					vaultcommand.Run(fullArgs)
				}
			}(vaultPath),
		}

		if len(node.children) > 0 {
			for _, child := range buildCobraCommands(node.children, vaultPath) {
				cmd.AddCommand(child)
			}
		}

		cmds = append(cmds, cmd)
	}

	return cmds
}

// StrongholdSubcommands builds a cobra command tree from the vault CLI
// command registry (via CommandSynopses). This keeps synopses in sync with
// initCommands and respects build tags (e.g. clionly omits server/agent).
// Cobra exposes the structure to help and help-json; execution still goes
// through vaultcommand.Run.
func StrongholdSubcommands() []*cobra.Command {
	synopses := vaultcommand.CommandSynopses()
	tree := buildCommandTree(synopses)
	return buildCobraCommands(tree, nil)
}
