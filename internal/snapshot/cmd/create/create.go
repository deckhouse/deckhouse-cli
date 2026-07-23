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

// Package create implements the `d8 snapshot create` command: it creates a
// Snapshot object in a namespace, optionally narrowing the captured set with a
// label selector and waiting until the Snapshot becomes Ready.
package create

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	cmdUse = "create"

	flagNamespace  = "namespace"
	flagSelector   = "selector"
	flagWait       = "wait"
	flagTimeout    = "timeout"
	flagOutput     = "output"
	flagKubeconfig = "kubeconfig"
	flagContext    = "context"

	// readyConditionType is the status condition reporting overall Snapshot
	// readiness; it matches the type used by list/restore.
	readyConditionType = "Ready"

	defaultWaitTimeout = 5 * time.Minute
	pollInterval       = 2 * time.Second
)

// snapshotGVR is the dynamic resource for state-snapshotter.deckhouse.io Snapshots.
var snapshotGVR = schema.GroupVersionResource{
	Group:    snapshotapi.StorageGroup,
	Version:  snapshotapi.Version,
	Resource: "snapshots",
}

// createOptions bundles the resolved inputs for one create run so the cluster
// logic stays decoupled from cobra flag plumbing (and unit-testable).
type createOptions struct {
	namespace   string
	name        string
	matchLabels map[string]interface{}
	wait        bool
	timeout     time.Duration
	poll        time.Duration
	outputFmt   string
}

// NewCommand builds the `d8 snapshot create` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " <name>",
		Short:         "Create a Snapshot of a namespace",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		Long: `Create a Snapshot object that captures a namespace's state/configuration.

The Snapshot is created in the target namespace (defaults to the kubeconfig context
namespace). An empty selector captures the whole namespace; pass --selector to capture
only the objects matching the given labels (sets spec.resourceSelector.matchLabels).

The Snapshot is reconciled asynchronously by the state-snapshotter controller. Use --wait
to block until it reports Ready, after which it can be listed, downloaded, and imported.`,
		Example: `  # Snapshot the whole "default" namespace and wait until it is Ready
  d8 snapshot create my-snap -n default --wait

  # Snapshot only the objects labeled app=demo
  d8 snapshot create my-snap -n demo -l app=demo

  # Print the created object as YAML
  d8 snapshot create my-snap -n demo -o yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	// Reuse the standard kubeconfig/context flags (same as the other snapshot
	// subcommands), so NewDynamicClient and KubeconfigNamespace can read them.
	flags.AddPersistentFlags(cmd)

	cmd.Flags().StringP(flagNamespace, "n", "", "namespace to create the Snapshot in (defaults to the kubeconfig context namespace)")
	cmd.Flags().StringP(flagSelector, "l", "", "capture only objects matching this label selector (e.g. app=demo,tier=db); sets spec.resourceSelector.matchLabels")
	cmd.Flags().Bool(flagWait, false, "wait until the Snapshot reports Ready")
	cmd.Flags().Duration(flagTimeout, defaultWaitTimeout, "timeout for --wait")
	utilk8s.AddOutputFlag(cmd, "name", "name", "json", "yaml")

	_ = cmd.RegisterFlagCompletionFunc(flagNamespace, func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return utilk8s.CompleteNamespaces(cmd, toComplete)
	})

	return cmd
}

// Run resolves flags, builds the dynamic client, and creates the Snapshot.
func Run(log *slog.Logger, cmd *cobra.Command, args []string) error {
	return run(log, cmd, args, utilk8s.NewDynamicClient)
}

func run(
	log *slog.Logger,
	cmd *cobra.Command,
	args []string,
	newDynamicClient func(*cobra.Command) (dynamic.Interface, error),
) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	opts, err := resolveOptions(cmd, args[0])
	if err != nil {
		return err
	}

	dyn, err := newDynamicClient(cmd)
	if err != nil {
		return err
	}

	return runCreate(ctx, dyn, cmd.OutOrStdout(), opts, log)
}

// resolveOptions reads and validates the flag values into a createOptions.
func resolveOptions(cmd *cobra.Command, name string) (createOptions, error) {
	outputFmt, err := cmd.Flags().GetString(flagOutput)
	if err != nil {
		return createOptions{}, fmt.Errorf("reading --%s flag: %w", flagOutput, err)
	}

	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		return createOptions{}, fmt.Errorf("reading --%s flag: %w", flagNamespace, err)
	}

	// kubectl-style default: when -n is omitted, fall back to the namespace
	// pinned by the current kubeconfig context (or "default").
	if namespace == "" {
		kubeconfigPath, _ := cmd.Flags().GetString(flagKubeconfig)
		contextName, _ := cmd.Flags().GetString(flagContext)

		namespace, err = utilk8s.KubeconfigNamespace(kubeconfigPath, contextName)
		if err != nil {
			return createOptions{}, err
		}
	}

	selector, err := cmd.Flags().GetString(flagSelector)
	if err != nil {
		return createOptions{}, fmt.Errorf("reading --%s flag: %w", flagSelector, err)
	}

	matchLabels, err := parseMatchLabels(selector)
	if err != nil {
		return createOptions{}, err
	}

	wait, err := cmd.Flags().GetBool(flagWait)
	if err != nil {
		return createOptions{}, fmt.Errorf("reading --%s flag: %w", flagWait, err)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		return createOptions{}, fmt.Errorf("reading --%s flag: %w", flagTimeout, err)
	}

	return createOptions{
		namespace:   namespace,
		name:        name,
		matchLabels: matchLabels,
		wait:        wait,
		timeout:     timeout,
		poll:        pollInterval,
		outputFmt:   outputFmt,
	}, nil
}

// runCreate creates the Snapshot via the dynamic client and, when requested,
// waits for it to become Ready before rendering the result.
func runCreate(ctx context.Context, dyn dynamic.Interface, w io.Writer, opts createOptions, log *slog.Logger) error {
	obj := buildSnapshot(opts.namespace, opts.name, opts.matchLabels)

	created, err := dyn.Resource(snapshotGVR).Namespace(opts.namespace).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if kubeerrors.IsAlreadyExists(err) {
			return fmt.Errorf("Snapshot %q already exists in namespace %q", opts.name, opts.namespace)
		}

		return fmt.Errorf("creating Snapshot %s/%s: %w", opts.namespace, opts.name, err)
	}

	log.Info("Snapshot created",
		slog.String("namespace", opts.namespace),
		slog.String("name", opts.name),
	)

	if opts.wait {
		created, err = waitReady(ctx, dyn, opts.namespace, opts.name, opts.timeout, opts.poll, log)
		if err != nil {
			return err
		}
	}

	return renderCreated(w, created, opts.outputFmt)
}

// buildSnapshot assembles the unstructured Snapshot to create. spec.mode is always set to
// Capture (the unified contract's create intent); matchLabels narrows the captured set when
// a selector is given, and an omitted selector captures the whole namespace.
func buildSnapshot(namespace, name string, matchLabels map[string]interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"mode": string(snapshotapi.SnapshotModeCapture),
	}

	if len(matchLabels) > 0 {
		spec["resourceSelector"] = map[string]interface{}{"matchLabels": matchLabels}
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		"kind":       "Snapshot",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"spec": spec,
	}}
}

// parseMatchLabels parses a kubectl-style "key=value,key2=value2" selector into
// a matchLabels map. An empty selector yields a nil map (whole-namespace capture).
func parseMatchLabels(selector string) (map[string]interface{}, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, nil
	}

	parts := strings.Split(selector, ",")
	labels := make(map[string]interface{}, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid --%s %q: empty selector component", flagSelector, selector)
		}

		if strings.Count(part, "=") != 1 {
			return nil, fmt.Errorf(
				"invalid --%s %q: component %q must contain exactly one '='",
				flagSelector,
				selector,
				part,
			)
		}

		key, value, _ := strings.Cut(part, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		if problems := validation.IsQualifiedName(key); len(problems) > 0 {
			return nil, fmt.Errorf(
				"invalid --%s %q: invalid label key %q: %s",
				flagSelector,
				selector,
				key,
				strings.Join(problems, "; "),
			)
		}

		if problems := validation.IsValidLabelValue(value); len(problems) > 0 {
			return nil, fmt.Errorf(
				"invalid --%s %q: invalid label value %q: %s",
				flagSelector,
				selector,
				value,
				strings.Join(problems, "; "),
			)
		}

		if _, exists := labels[key]; exists {
			return nil, fmt.Errorf("invalid --%s %q: duplicate label key %q", flagSelector, selector, key)
		}

		labels[key] = value
	}

	return labels, nil
}

// waitReady polls the Snapshot until its Ready condition is True or the timeout
// elapses. Ready=False is treated as still-in-progress (capture is async), so it
// keeps polling; on timeout it surfaces the last observed reason/message.
func waitReady(ctx context.Context, dyn dynamic.Interface, namespace, name string, timeout, poll time.Duration, log *slog.Logger) (*unstructured.Unstructured, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastReason, lastMessage string

	for {
		if err := waitCtx.Err(); err != nil {
			return nil, waitReadyContextError(err, namespace, name, lastReason, lastMessage)
		}

		obj, err := dyn.Resource(snapshotGVR).Namespace(namespace).Get(waitCtx, name, metav1.GetOptions{})
		if err != nil {
			if waitErr := waitCtx.Err(); waitErr != nil {
				return nil, waitReadyContextError(waitErr, namespace, name, lastReason, lastMessage)
			}

			return nil, fmt.Errorf("get Snapshot %s/%s: %w", namespace, name, err)
		}

		if err := waitCtx.Err(); err != nil {
			return nil, waitReadyContextError(err, namespace, name, lastReason, lastMessage)
		}

		status, reason, message := readyCondition(obj)
		if status == string(metav1.ConditionTrue) {
			log.Info("Snapshot is Ready", slog.String("namespace", namespace), slog.String("name", name))

			return obj, nil
		}

		lastReason, lastMessage = reason, message

		log.Debug("waiting for Snapshot to become Ready",
			slog.String("namespace", namespace),
			slog.String("name", name),
			slog.String("status", status),
			slog.String("reason", reason),
		)

		if !sleepCtx(waitCtx, poll) {
			return nil, waitReadyContextError(waitCtx.Err(), namespace, name, lastReason, lastMessage)
		}
	}
}

func waitReadyContextError(err error, namespace, name, lastReason, lastMessage string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf(
			"timeout waiting for Snapshot %s/%s to become Ready (last reason=%q message=%q): %w",
			namespace,
			name,
			lastReason,
			lastMessage,
			err,
		)
	}

	return err
}

// readyCondition returns the status/reason/message of the "Ready" condition, or
// empty strings when the Snapshot carries no such condition yet.
func readyCondition(obj *unstructured.Unstructured) (string, string, string) {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return "", "", ""
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		condType, _, _ := unstructured.NestedString(m, "type")
		if condType != readyConditionType {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")
		reason, _, _ := unstructured.NestedString(m, "reason")
		message, _, _ := unstructured.NestedString(m, "message")

		return status, reason, message
	}

	return "", "", ""
}

// renderCreated reports the created Snapshot. The default "name" format prints a
// kubectl-style confirmation line; json/yaml print the object verbatim.
func renderCreated(w io.Writer, obj *unstructured.Unstructured, outputFmt string) error {
	switch outputFmt {
	case "json", "yaml":
		return utilk8s.PrintObject(w, obj, outputFmt)
	case "name", "":
		_, err := fmt.Fprintf(w, "snapshot.%s/%s created\n", snapshotapi.StorageGroup, obj.GetName())
		return err
	default:
		return fmt.Errorf("unsupported output format %q; use name|json|yaml", outputFmt)
	}
}

// sleepCtx sleeps for d or until ctx is cancelled, returning false on cancel.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
