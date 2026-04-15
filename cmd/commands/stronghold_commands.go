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

// vaultCommandSynopses returns a flat map of vault CLI command paths to their
// synopsis strings. The keys use spaces to represent hierarchy, matching the
// mitchellh/cli command registration format (e.g. "operator raft snapshot save").
//
// Only commands available under the "clionly" build tag are included.
func vaultCommandSynopses() map[string]string {
	return map[string]string{
		"audit":                              (&vaultcommand.AuditCommand{}).Synopsis(),
		"audit disable":                      (&vaultcommand.AuditDisableCommand{}).Synopsis(),
		"audit enable":                       (&vaultcommand.AuditEnableCommand{}).Synopsis(),
		"audit list":                         (&vaultcommand.AuditListCommand{}).Synopsis(),
		"auth":                               (&vaultcommand.AuthCommand{}).Synopsis(),
		"auth disable":                       (&vaultcommand.AuthDisableCommand{}).Synopsis(),
		"auth enable":                        (&vaultcommand.AuthEnableCommand{}).Synopsis(),
		"auth help":                          (&vaultcommand.AuthHelpCommand{}).Synopsis(),
		"auth list":                          (&vaultcommand.AuthListCommand{}).Synopsis(),
		"auth move":                          (&vaultcommand.AuthMoveCommand{}).Synopsis(),
		"auth tune":                          (&vaultcommand.AuthTuneCommand{}).Synopsis(),
		"debug":                              (&vaultcommand.DebugCommand{}).Synopsis(),
		"delete":                             (&vaultcommand.DeleteCommand{}).Synopsis(),
		"events subscribe":                   (&vaultcommand.EventsSubscribeCommands{}).Synopsis(),
		"kv":                                 (&vaultcommand.KVCommand{}).Synopsis(),
		"kv delete":                          (&vaultcommand.KVDeleteCommand{}).Synopsis(),
		"kv destroy":                         (&vaultcommand.KVDestroyCommand{}).Synopsis(),
		"kv enable-versioning":               (&vaultcommand.KVEnableVersioningCommand{}).Synopsis(),
		"kv get":                             (&vaultcommand.KVGetCommand{}).Synopsis(),
		"kv list":                            (&vaultcommand.KVListCommand{}).Synopsis(),
		"kv metadata":                        (&vaultcommand.KVMetadataCommand{}).Synopsis(),
		"kv metadata delete":                 (&vaultcommand.KVMetadataDeleteCommand{}).Synopsis(),
		"kv metadata get":                    (&vaultcommand.KVMetadataGetCommand{}).Synopsis(),
		"kv metadata patch":                  (&vaultcommand.KVMetadataPatchCommand{}).Synopsis(),
		"kv metadata put":                    (&vaultcommand.KVMetadataPutCommand{}).Synopsis(),
		"kv patch":                           (&vaultcommand.KVPatchCommand{}).Synopsis(),
		"kv put":                             (&vaultcommand.KVPutCommand{}).Synopsis(),
		"kv rollback":                        (&vaultcommand.KVRollbackCommand{}).Synopsis(),
		"kv undelete":                        (&vaultcommand.KVUndeleteCommand{}).Synopsis(),
		"lease":                              (&vaultcommand.LeaseCommand{}).Synopsis(),
		"lease lookup":                       (&vaultcommand.LeaseLookupCommand{}).Synopsis(),
		"lease renew":                        (&vaultcommand.LeaseRenewCommand{}).Synopsis(),
		"lease revoke":                       (&vaultcommand.LeaseRevokeCommand{}).Synopsis(),
		"list":                               (&vaultcommand.ListCommand{}).Synopsis(),
		"login":                              (&vaultcommand.LoginCommand{}).Synopsis(),
		"monitor":                            (&vaultcommand.MonitorCommand{}).Synopsis(),
		"namespace":                          (&vaultcommand.NamespaceCommand{}).Synopsis(),
		"namespace create":                   (&vaultcommand.NamespaceCreateCommand{}).Synopsis(),
		"namespace delete":                   (&vaultcommand.NamespaceDeleteCommand{}).Synopsis(),
		"namespace list":                     (&vaultcommand.NamespaceListCommand{}).Synopsis(),
		"namespace lock":                     (&vaultcommand.NamespaceAPILockCommand{}).Synopsis(),
		"namespace lookup":                   (&vaultcommand.NamespaceLookupCommand{}).Synopsis(),
		"namespace patch":                    (&vaultcommand.NamespacePatchCommand{}).Synopsis(),
		"namespace unlock":                   (&vaultcommand.NamespaceAPIUnlockCommand{}).Synopsis(),
		"operator":                           (&vaultcommand.OperatorCommand{}).Synopsis(),
		"operator generate-root":             (&vaultcommand.OperatorGenerateRootCommand{}).Synopsis(),
		"operator init":                      (&vaultcommand.OperatorInitCommand{}).Synopsis(),
		"operator key-status":                (&vaultcommand.OperatorKeyStatusCommand{}).Synopsis(),
		"operator members":                   (&vaultcommand.OperatorMembersCommand{}).Synopsis(),
		"operator raft":                      (&vaultcommand.OperatorRaftCommand{}).Synopsis(),
		"operator raft autopilot get-config": (&vaultcommand.OperatorRaftAutopilotGetConfigCommand{}).Synopsis(),
		"operator raft autopilot set-config": (&vaultcommand.OperatorRaftAutopilotSetConfigCommand{}).Synopsis(),
		"operator raft autopilot state":      (&vaultcommand.OperatorRaftAutopilotStateCommand{}).Synopsis(),
		"operator raft demote":               (&vaultcommand.OperatorRaftDemoteCommand{}).Synopsis(),
		"operator raft join":                 (&vaultcommand.OperatorRaftJoinCommand{}).Synopsis(),
		"operator raft list-peers":           (&vaultcommand.OperatorRaftListPeersCommand{}).Synopsis(),
		"operator raft promote":              (&vaultcommand.OperatorRaftPromoteCommand{}).Synopsis(),
		"operator raft remove-peer":          (&vaultcommand.OperatorRaftRemovePeerCommand{}).Synopsis(),
		"operator raft snapshot":             (&vaultcommand.OperatorRaftSnapshotCommand{}).Synopsis(),
		"operator raft snapshot restore":     (&vaultcommand.OperatorRaftSnapshotRestoreCommand{}).Synopsis(),
		"operator raft snapshot save":        (&vaultcommand.OperatorRaftSnapshotSaveCommand{}).Synopsis(),
		"operator rekey":                     (&vaultcommand.OperatorRekeyCommand{}).Synopsis(),
		"operator rotate":                    (&vaultcommand.OperatorRotateCommand{}).Synopsis(),
		"operator seal":                      (&vaultcommand.OperatorSealCommand{}).Synopsis(),
		"operator step-down":                 (&vaultcommand.OperatorStepDownCommand{}).Synopsis(),
		"operator unseal":                    (&vaultcommand.OperatorUnsealCommand{}).Synopsis(),
		"operator usage":                     (&vaultcommand.OperatorUsageCommand{}).Synopsis(),
		"patch":                              (&vaultcommand.PatchCommand{}).Synopsis(),
		"path-help":                          (&vaultcommand.PathHelpCommand{}).Synopsis(),
		"pki":                                (&vaultcommand.PKICommand{}).Synopsis(),
		"pki health-check":                   (&vaultcommand.PKIHealthCheckCommand{}).Synopsis(),
		"pki issue":                          (&vaultcommand.PKIIssueCACommand{}).Synopsis(),
		"pki list-intermediates":             (&vaultcommand.PKIListIntermediateCommand{}).Synopsis(),
		"pki reissue":                        (&vaultcommand.PKIReIssueCACommand{}).Synopsis(),
		"pki verify-sign":                    (&vaultcommand.PKIVerifySignCommand{}).Synopsis(),
		"plugin":                             (&vaultcommand.PluginCommand{}).Synopsis(),
		"plugin deregister":                  (&vaultcommand.PluginDeregisterCommand{}).Synopsis(),
		"plugin info":                        (&vaultcommand.PluginInfoCommand{}).Synopsis(),
		"plugin list":                        (&vaultcommand.PluginListCommand{}).Synopsis(),
		"plugin register":                    (&vaultcommand.PluginRegisterCommand{}).Synopsis(),
		"plugin reload":                      (&vaultcommand.PluginReloadCommand{}).Synopsis(),
		"plugin reload-status":               (&vaultcommand.PluginReloadStatusCommand{}).Synopsis(),
		"policy":                             (&vaultcommand.PolicyCommand{}).Synopsis(),
		"policy delete":                      (&vaultcommand.PolicyDeleteCommand{}).Synopsis(),
		"policy list":                        (&vaultcommand.PolicyListCommand{}).Synopsis(),
		"policy read":                        (&vaultcommand.PolicyReadCommand{}).Synopsis(),
		"policy write":                       (&vaultcommand.PolicyWriteCommand{}).Synopsis(),
		"print":                              (&vaultcommand.PrintCommand{}).Synopsis(),
		"print token":                        (&vaultcommand.PrintTokenCommand{}).Synopsis(),
		"read":                               (&vaultcommand.ReadCommand{}).Synopsis(),
		"secrets":                            (&vaultcommand.SecretsCommand{}).Synopsis(),
		"secrets disable":                    (&vaultcommand.SecretsDisableCommand{}).Synopsis(),
		"secrets enable":                     (&vaultcommand.SecretsEnableCommand{}).Synopsis(),
		"secrets list":                       (&vaultcommand.SecretsListCommand{}).Synopsis(),
		"secrets move":                       (&vaultcommand.SecretsMoveCommand{}).Synopsis(),
		"secrets tune":                       (&vaultcommand.SecretsTuneCommand{}).Synopsis(),
		"ssh":                                (&vaultcommand.SSHCommand{}).Synopsis(),
		"status":                             (&vaultcommand.StatusCommand{}).Synopsis(),
		"token":                              (&vaultcommand.TokenCommand{}).Synopsis(),
		"token capabilities":                 (&vaultcommand.TokenCapabilitiesCommand{}).Synopsis(),
		"token create":                       (&vaultcommand.TokenCreateCommand{}).Synopsis(),
		"token lookup":                       (&vaultcommand.TokenLookupCommand{}).Synopsis(),
		"token renew":                        (&vaultcommand.TokenRenewCommand{}).Synopsis(),
		"token revoke":                       (&vaultcommand.TokenRevokeCommand{}).Synopsis(),
		"transit":                            (&vaultcommand.TransitCommand{}).Synopsis(),
		"transit import":                     (&vaultcommand.TransitImportCommand{}).Synopsis(),
		"transit import-version":             (&vaultcommand.TransitImportVersionCommand{}).Synopsis(),
		"unwrap":                             (&vaultcommand.UnwrapCommand{}).Synopsis(),
		"version":                            (&vaultcommand.VersionCommand{}).Synopsis(),
		"version-history":                    (&vaultcommand.VersionHistoryCommand{}).Synopsis(),
		"write":                              (&vaultcommand.WriteCommand{}).Synopsis(),
	}
}

type commandNode struct {
	name     string
	synopsis string
	children map[string]*commandNode
}

// buildCommandTree converts a flat map of space-delimited command paths into
// a tree structure suitable for creating nested cobra commands.
func buildCommandTree(synopses map[string]string) map[string]*commandNode {
	roots := make(map[string]*commandNode)

	keys := make([]string, 0, len(synopses))
	for k := range synopses {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		parts := strings.Split(key, " ")
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
	keys := make([]string, 0, len(nodes))
	for k := range nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cmds := make([]*cobra.Command, 0, len(keys))
	for _, key := range keys {
		node := nodes[key]
		vaultPath := append(append([]string{}, pathPrefix...), node.name)

		cmd := &cobra.Command{
			Use:                node.name,
			Short:              node.synopsis,
			SilenceErrors:      true,
			SilenceUsage:       true,
			DisableFlagParsing: true,
			Run: func(vp []string) func(*cobra.Command, []string) {
				return func(_ *cobra.Command, args []string) {
					fullArgs := make([]string, 0, len(vp)+len(args))
					fullArgs = append(fullArgs, vp...)
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
// command registry. This makes vault's command structure visible to cobra's
// help system and the help-json documentation generator, while preserving
// pass-through execution to vaultcommand.Run.
func StrongholdSubcommands() []*cobra.Command {
	synopses := vaultCommandSynopses()
	tree := buildCommandTree(synopses)
	return buildCobraCommands(tree, nil)
}
