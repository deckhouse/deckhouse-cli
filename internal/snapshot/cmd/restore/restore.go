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

// Package restorecmd implements the `d8 snapshot restore` command.
package restorecmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdUse = "restore"

	flagNamespace      = "namespace"
	flagNode           = "node"
	flagNodeAPIVersion = "node-api-version"
	flagScope          = "scope"
	flagObject         = "object"
	flagDryRun         = "dry-run"
	flagEdit           = "edit"
	flagWait           = "wait"
	flagTimeout        = "timeout"
)

// NewCommand builds the `d8 snapshot restore` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags] <snapshot>",
		Short:         "Restore a snapshot in-place into its namespace",
		SilenceUsage:  true,
		SilenceErrors: true,
		Long: `Restore a snapshot tree in-namespace.

The snapshot is restored into the same namespace it lives in: -n/--namespace is both
the source Snapshot namespace and the restore target. The required VolumeSnapshot /
VirtualDiskSnapshot leaves must already be present in that namespace (they exist after
the original snapshot, or after the snapshot is re-created in this namespace).

Cross-namespace restore is not a single command: it is a separate procedure that downloads
the snapshot from the source namespace, re-creates the Snapshot and its volume-snapshot
leaves in the target namespace, and then runs restore there.

The server compiles the whole subtree in one call; every returned object is applied as-is.
PersistentVolumeClaims already carry spec.dataSourceRef pointing at the VolumeSnapshot (or
VirtualDiskSnapshot for domain disks) present in the namespace, so CSI provisions the data.

--node restricts the restore to a single node subtree. The positional Snapshot always
anchors the hierarchy and must exist; the selection may use either the generated snapshot-CR
Kind/name or the original captured source Kind/name. A Ready child selected by its generated
identity remains restorable when the root is DEGRADED because an unrelated sibling CR was
deleted. Selecting that deleted generated child reports that it belongs to the tree but is
missing. Original-source selection fails closed while any child ref is unresolved, because a
missing child's original identity cannot be inspected; retry with the live match's generated
Kind/name and --node-api-version shown in the error.

--node-api-version disambiguates nodes whose selected Kind/name is shared across API groups
or versions. Use "v1" for the Kubernetes core group, or "<group>/<version>" for a named group.
It requires --node and must match the generated or original identity used by that selection.

--scope narrows how much of the addressed node (the root, or --node's selection) the server
compiles: "subtree" (default) compiles the node and its whole subtree, recursively; "node"
compiles only the addressed node itself, with no descendants. --scope node with no --node
selects the root Snapshot node alone.

--object restricts a --scope node restore to a single captured object within that node, by
"<Kind>/<name>"; it requires --scope node and fails fast, before any network call, otherwise.

--dry-run sends every apply with DryRunAll so the API server validates and admits objects
(schema validation, webhooks, immutable-field checks) without persisting them. Use it to
preflight a restore before committing. The --wait loop is skipped in dry-run mode because
no objects are actually created.

--wait only tracks PersistentVolumeClaims that appear in the restored manifest set. Disk-backed
PVCs for domain objects are recreated asynchronously by the domain controller (not part of this
output), so they are not awaited; the command may return before such volumes finish provisioning.
A PVC on a WaitForFirstConsumer StorageClass is checked once and never polled: it is expected to
stay Pending until a Pod schedules against it, so --wait does not block or spend its timeout
budget on it; PVCs on an Immediate (or unspecified, since Immediate is Kubernetes' own default)
StorageClass are still awaited until Bound or --timeout as before.`,
		Example: `  # Restore snapshot "my-snap" in namespace "default"
  d8 snapshot restore my-snap -n default

  # Restore only a single disk-snapshot node and its subtree -- the generated
  # snapshot CR name form (e.g. DemoVirtualDiskSnapshot/nss-child-abc123) still works too
  d8 snapshot restore my-snap -n default --node DemoVirtualDisk/bk-disk-a

  # Disambiguate identical Kind/name identities from different API groups or versions
  d8 snapshot restore my-snap -n default --node DemoVirtualDisk/bk-disk-a --node-api-version virtualization.deckhouse.io/v1alpha2

  # Restore only the selected node itself, no descendants
  d8 snapshot restore my-snap -n default --node DemoVirtualDisk/bk-disk-a --scope node

  # Restore a single captured object within a node
  d8 snapshot restore my-snap -n default --node DemoVirtualDisk/bk-disk-a --scope node --object PersistentVolumeClaim/bk-disk-a

  # Preflight: validate all objects without applying them
  d8 snapshot restore my-snap -n default --dry-run

  # Restore and wait for the restored PVCs to become Bound
  d8 snapshot restore my-snap -n default --wait --timeout 15m

  # Review and edit manifests in $EDITOR before applying
  d8 snapshot restore my-snap -n default --edit`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "snapshot namespace; also the restore target namespace (required)")
	cmd.Flags().String(flagNode, "", "restrict restore to a single node subtree; format '<Kind>/<name>' (e.g. --node DemoVirtualDisk/bk-disk-a); the generated snapshot CR name form (e.g. DemoVirtualDiskSnapshot/nss-child-abc123) is still accepted")
	cmd.Flags().String(flagNodeAPIVersion, "", "disambiguate --node by exact API version: 'v1' for core resources or '<group>/<version>' for named groups (requires --node)")
	cmd.Flags().String(flagScope, string(aggapi.RestoreScopeSubtree), "restore scope: 'subtree' (default) compiles the addressed node and its whole subtree; 'node' compiles only the addressed node itself")
	cmd.Flags().String(flagObject, "", "restrict a --scope node restore to a single captured object; format '<Kind>/<name>' (requires --scope node)")
	cmd.Flags().Bool(flagDryRun, false, "validate objects via DryRunAll without persisting; skips --wait (use to preflight a restore)")
	cmd.Flags().Bool(flagEdit, false, "open resolved manifests in $KUBE_EDITOR/$EDITOR before applying; aborts on non-zero exit, unchanged, or empty content")
	cmd.Flags().Bool(flagWait, false, "wait for restored PersistentVolumeClaims to become Bound (only PVCs in the manifest set; domain disk-backed PVCs created asynchronously are not awaited; a WaitForFirstConsumer-class PVC left Pending with no consumer is not waited on)")
	cmd.Flags().Duration(flagTimeout, 10*time.Minute, "timeout for the --wait Bound check")

	return cmd
}

// Run validates flags, builds the kube clients, and executes the restore.
// It derives a signal-cancellable context from cmd.Context() so that Ctrl-C
// (SIGINT) and SIGTERM cleanly stop the restore.
func Run(log *slog.Logger, cmd *cobra.Command, args []string) error {
	parentCtx := cmd.Context()
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	ctx, cancel := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNamespace, err)
	}

	if namespace == "" {
		return fmt.Errorf("--%s is required", flagNamespace)
	}

	snapshotName := args[0]

	nodeFlag, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNode, err)
	}

	selectedKind, selectedName, err := parseNodeFlag(nodeFlag)
	if err != nil {
		return fmt.Errorf("invalid --%s %q: %w", flagNode, nodeFlag, err)
	}

	selectedAPIVersion, err := cmd.Flags().GetString(flagNodeAPIVersion)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNodeAPIVersion, err)
	}

	if selectedAPIVersion != "" && selectedKind == "" {
		return fmt.Errorf("--%s requires --%s", flagNodeAPIVersion, flagNode)
	}

	if err := restore.ValidateNodeAPIVersion(selectedAPIVersion); err != nil {
		return fmt.Errorf("invalid --%s %q: %w", flagNodeAPIVersion, selectedAPIVersion, err)
	}

	scopeFlag, err := cmd.Flags().GetString(flagScope)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagScope, err)
	}

	scope := aggapi.RestoreScope(scopeFlag)
	if scope != aggapi.RestoreScopeSubtree && scope != aggapi.RestoreScopeNode {
		return fmt.Errorf("invalid --%s %q: must be %q or %q", flagScope, scopeFlag, aggapi.RestoreScopeSubtree, aggapi.RestoreScopeNode)
	}

	objectFlag, err := cmd.Flags().GetString(flagObject)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagObject, err)
	}

	filterKind, filterName, err := parseNodeFlag(objectFlag)
	if err != nil {
		return fmt.Errorf("invalid --%s %q: %w", flagObject, objectFlag, err)
	}

	if filterKind != "" && scope != aggapi.RestoreScopeNode {
		return fmt.Errorf("--%s requires --%s=%s (got --%s=%s)", flagObject, flagScope, aggapi.RestoreScopeNode, flagScope, scope)
	}

	dryRun, err := cmd.Flags().GetBool(flagDryRun)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagDryRun, err)
	}

	edit, err := cmd.Flags().GetBool(flagEdit)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagEdit, err)
	}

	wait, err := cmd.Flags().GetBool(flagWait)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagWait, err)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTimeout, err)
	}

	safeClient.SupportNoAuth = false

	sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("building kube client: %w", err)
	}

	restConfig := boundedControlPlaneConfig(sc.RESTConfig())

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("building discovery client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	aggClient, err := aggapi.NewClientForConfig(restConfig, mapper)
	if err != nil {
		return fmt.Errorf("building aggregated API client: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	cfg := restore.Config{
		Namespace:              namespace,
		Snapshot:               snapshotName,
		SelectedNodeKind:       selectedKind,
		SelectedNodeName:       selectedName,
		SelectedNodeAPIVersion: selectedAPIVersion,
		Scope:                  scope,
		FilterKind:             filterKind,
		FilterName:             filterName,
		Edit:                   edit,
		DryRun:                 dryRun,
		Wait:                   wait,
		Timeout:                timeout,
		Source:                 aggClient,
		Dynamic:                dynClient,
		Mapper:                 mapper,
		ControlPlaneTimeout:    restore.DefaultControlPlaneTimeout,
		Log:                    log,
	}

	log.Info("starting snapshot restore",
		slog.String("namespace", namespace),
		slog.String("snapshot", snapshotName),
	)

	if err := restore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("snapshot restore failed: %w", err)
	}

	log.Info("snapshot restore complete",
		slog.String("namespace", namespace),
		slog.String("snapshot", snapshotName),
	)

	return nil
}

func boundedControlPlaneConfig(config *rest.Config) *rest.Config {
	bounded := rest.CopyConfig(config)
	bounded.Timeout = restore.DefaultControlPlaneTimeout
	previousWrap := bounded.WrapTransport
	bounded.WrapTransport = func(roundTripper http.RoundTripper) http.RoundTripper {
		if transport, ok := roundTripper.(*http.Transport); ok {
			cloned := transport.Clone()
			cloned.TLSHandshakeTimeout = restore.DefaultControlPlaneTimeout
			cloned.ResponseHeaderTimeout = restore.DefaultControlPlaneTimeout

			baseDialContext := cloned.DialContext
			if baseDialContext == nil {
				baseDialContext = (&net.Dialer{}).DialContext
			}

			cloned.DialContext = func(
				ctx context.Context,
				network string,
				address string,
			) (net.Conn, error) {
				dialCtx, cancel := context.WithTimeout(ctx, restore.DefaultControlPlaneTimeout)
				defer cancel()

				return baseDialContext(dialCtx, network, address)
			}

			roundTripper = cloned
		}

		if previousWrap != nil {
			return previousWrap(roundTripper)
		}

		return roundTripper
	}

	return bounded
}

// parseNodeFlag parses a "<Kind>/<name>" flag value into its components. Shared by --node
// and --object, which use the identical format. An empty string returns empty strings and
// no error (full-tree restore for --node; no object filter for --object). The value must
// contain exactly one "/" with a non-empty kind and name on each side.
func parseNodeFlag(s string) (string, string, error) {
	if s == "" {
		return "", "", nil
	}

	idx := strings.IndexByte(s, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("expected format '<Kind>/<name>', got %q: missing '/'", s)
	}

	kind := s[:idx]
	name := s[idx+1:]

	if kind == "" {
		return "", "", fmt.Errorf("kind must not be empty in %q", s)
	}

	if name == "" {
		return "", "", fmt.Errorf("name must not be empty in %q", s)
	}

	if strings.Contains(name, "/") {
		return "", "", fmt.Errorf("name must not contain '/' in %q; expected exactly one '/'", s)
	}

	return kind, name, nil
}
