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

// Package delete implements the `d8 snapshot delete` command: it deletes one or
// more Snapshot objects by name, by label selector, or all in a namespace, and
// can optionally wait until they are fully removed.
package delete

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	cmdUse = "delete"

	flagNamespace      = "namespace"
	flagSelector       = "selector"
	flagAll            = "all"
	flagWait           = "wait"
	flagTimeout        = "timeout"
	flagIgnoreNotFound = "ignore-not-found"
	flagKubeconfig     = "kubeconfig"
	flagContext        = "context"

	defaultWaitTimeout = 5 * time.Minute
	pollInterval       = 2 * time.Second
)

// snapshotGVR is the dynamic resource for storage.deckhouse.io Snapshots.
var snapshotGVR = schema.GroupVersionResource{
	Group:    snapshotapi.StorageGroup,
	Version:  snapshotapi.Version,
	Resource: "snapshots",
}

// deleteOptions bundles the resolved inputs for one delete run so the cluster
// logic stays decoupled from cobra flag plumbing (and unit-testable).
type deleteOptions struct {
	namespace      string
	names          []string
	selector       string
	all            bool
	wait           bool
	timeout        time.Duration
	poll           time.Duration
	ignoreNotFound bool
}

// NewCommand builds the `d8 snapshot delete` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [name...]",
		Short:         "Delete one or more Snapshots",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ArbitraryArgs,
		Long: `Delete Snapshot objects in a namespace.

Targets are selected in exactly one of three ways:
  - by name:      d8 snapshot delete snap-a snap-b -n demo
  - by selector:  d8 snapshot delete -l app=demo -n demo
  - all:          d8 snapshot delete --all -n demo

Deleting a Snapshot lets the state-snapshotter controller garbage-collect the
associated SnapshotContent and data artifacts. Use --wait to block until the
Snapshot objects are fully removed from the API server.`,
		Example: `  # Delete two named Snapshots and wait until they are gone
  d8 snapshot delete snap-a snap-b -n demo --wait

  # Delete every Snapshot labeled app=demo
  d8 snapshot delete -l app=demo -n demo

  # Delete all Snapshots in the namespace
  d8 snapshot delete --all -n demo

  # Do not error if a named Snapshot is already gone
  d8 snapshot delete snap-a -n demo --ignore-not-found`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
		ValidArgsFunction: completeSnapshotNames,
	}

	// Reuse the standard kubeconfig/context flags (same as the other snapshot
	// subcommands), so NewDynamicClient and KubeconfigNamespace can read them.
	flags.AddPersistentFlags(cmd)

	cmd.Flags().StringP(flagNamespace, "n", "", "namespace of the Snapshots (defaults to the kubeconfig context namespace)")
	cmd.Flags().StringP(flagSelector, "l", "", "delete Snapshots matching this label selector (e.g. app=demo,tier=db)")
	cmd.Flags().Bool(flagAll, false, "delete all Snapshots in the namespace")
	cmd.Flags().Bool(flagWait, false, "wait until the Snapshots are fully deleted")
	cmd.Flags().Duration(flagTimeout, defaultWaitTimeout, "timeout for --wait")
	cmd.Flags().Bool(flagIgnoreNotFound, false, "do not error if a named Snapshot does not exist")

	_ = cmd.RegisterFlagCompletionFunc(flagNamespace, func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return utilk8s.CompleteNamespaces(cmd, toComplete)
	})

	return cmd
}

// completeSnapshotNames offers Snapshot names in the target namespace for shell
// completion of positional args.
func completeSnapshotNames(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	namespace, _ := cmd.Flags().GetString(flagNamespace)
	if namespace == "" {
		kubeconfigPath, _ := cmd.Flags().GetString(flagKubeconfig)
		contextName, _ := cmd.Flags().GetString(flagContext)
		namespace, _ = utilk8s.KubeconfigNamespace(kubeconfigPath, contextName)
	}

	return utilk8s.CompleteResourceNames(cmd, snapshotGVR, namespace, toComplete)
}

// Run resolves flags, builds the dynamic client, and deletes the Snapshots.
func Run(log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	opts, err := resolveOptions(cmd, args)
	if err != nil {
		return err
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	return runDelete(ctx, dyn, cmd.OutOrStdout(), opts, log)
}

// resolveOptions reads and validates flags/args into a deleteOptions. Exactly
// one selection mode (names, --selector, or --all) must be provided.
func resolveOptions(cmd *cobra.Command, args []string) (deleteOptions, error) {
	selector, err := cmd.Flags().GetString(flagSelector)
	if err != nil {
		return deleteOptions{}, fmt.Errorf("reading --%s flag: %w", flagSelector, err)
	}

	all, err := cmd.Flags().GetBool(flagAll)
	if err != nil {
		return deleteOptions{}, fmt.Errorf("reading --%s flag: %w", flagAll, err)
	}

	// Validate the selection mode before touching the cluster/kubeconfig so a
	// misuse fails fast and deterministically.
	if err := validateScope(len(args) > 0, selector != "", all); err != nil {
		return deleteOptions{}, err
	}

	wait, err := cmd.Flags().GetBool(flagWait)
	if err != nil {
		return deleteOptions{}, fmt.Errorf("reading --%s flag: %w", flagWait, err)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		return deleteOptions{}, fmt.Errorf("reading --%s flag: %w", flagTimeout, err)
	}

	ignoreNotFound, err := cmd.Flags().GetBool(flagIgnoreNotFound)
	if err != nil {
		return deleteOptions{}, fmt.Errorf("reading --%s flag: %w", flagIgnoreNotFound, err)
	}

	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		return deleteOptions{}, fmt.Errorf("reading --%s flag: %w", flagNamespace, err)
	}

	if namespace == "" {
		kubeconfigPath, _ := cmd.Flags().GetString(flagKubeconfig)
		contextName, _ := cmd.Flags().GetString(flagContext)

		namespace, err = utilk8s.KubeconfigNamespace(kubeconfigPath, contextName)
		if err != nil {
			return deleteOptions{}, err
		}
	}

	return deleteOptions{
		namespace:      namespace,
		names:          args,
		selector:       selector,
		all:            all,
		wait:           wait,
		timeout:        timeout,
		poll:           pollInterval,
		ignoreNotFound: ignoreNotFound,
	}, nil
}

// validateScope enforces that exactly one of name args, --selector, or --all is set.
func validateScope(haveNames, haveSelector, all bool) error {
	modes := 0
	if haveNames {
		modes++
	}

	if haveSelector {
		modes++
	}

	if all {
		modes++
	}

	switch {
	case modes == 0:
		return fmt.Errorf("specify one or more Snapshot names, --%s, or --%s", flagSelector, flagAll)
	case modes > 1:
		return fmt.Errorf("Snapshot names, --%s, and --%s are mutually exclusive", flagSelector, flagAll)
	default:
		return nil
	}
}

// runDelete resolves the target set and deletes each Snapshot, optionally waiting
// for full removal. Errors are aggregated so one bad target does not hide others.
func runDelete(ctx context.Context, dyn dynamic.Interface, w io.Writer, opts deleteOptions, log *slog.Logger) error {
	fromSelection := opts.all || opts.selector != ""

	targets := opts.names
	if fromSelection {
		names, err := listTargetNames(ctx, dyn, opts.namespace, opts.selector)
		if err != nil {
			return err
		}

		if len(names) == 0 {
			fmt.Fprintln(w, "No snapshots found.")

			return nil
		}

		targets = names
	}

	var (
		deleted []string
		errs    []error
	)

	for _, name := range targets {
		err := dyn.Resource(snapshotGVR).Namespace(opts.namespace).Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil {
			if kubeerrors.IsNotFound(err) {
				// When selecting by label/all the object may race away between
				// List and Delete; with --ignore-not-found the user opted out of
				// the error. Either way: skip silently.
				if fromSelection || opts.ignoreNotFound {
					continue
				}

				errs = append(errs, fmt.Errorf("Snapshot %q not found in namespace %q", name, opts.namespace))

				continue
			}

			errs = append(errs, fmt.Errorf("deleting Snapshot %s/%s: %w", opts.namespace, name, err))

			continue
		}

		deleted = append(deleted, name)

		fmt.Fprintf(w, "snapshot.%s/%s deleted\n", snapshotapi.StorageGroup, name)
		log.Info("Snapshot deleted", slog.String("namespace", opts.namespace), slog.String("name", name))
	}

	if opts.wait {
		for _, name := range deleted {
			if err := waitGone(ctx, dyn, opts.namespace, name, opts.timeout, opts.poll, log); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

// listTargetNames returns the names of Snapshots in the namespace, optionally
// filtered by a label selector.
func listTargetNames(ctx context.Context, dyn dynamic.Interface, namespace, selector string) ([]string, error) {
	listOpts := metav1.ListOptions{}
	if selector != "" {
		listOpts.LabelSelector = selector
	}

	list, err := dyn.Resource(snapshotGVR).Namespace(namespace).List(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("listing Snapshots in namespace %q: %w", namespace, err)
	}

	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}

	return names, nil
}

// waitGone polls the Snapshot until it is no longer found or the timeout elapses.
func waitGone(ctx context.Context, dyn dynamic.Interface, namespace, name string, timeout, poll time.Duration, log *slog.Logger) error {
	deadline := time.Now().Add(timeout)

	for {
		_, err := dyn.Resource(snapshotGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if kubeerrors.IsNotFound(err) {
			log.Debug("Snapshot deleted", slog.String("namespace", namespace), slog.String("name", name))

			return nil
		}

		if err != nil {
			return fmt.Errorf("get Snapshot %s/%s: %w", namespace, name, err)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for Snapshot %s/%s to be deleted", namespace, name)
		}

		if !sleepCtx(ctx, poll) {
			return ctx.Err()
		}
	}
}

// sleepCtx sleeps for d or until ctx is cancelled, returning false on cancel.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
