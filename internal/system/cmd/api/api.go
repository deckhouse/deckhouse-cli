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

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/api/apiclient"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const outputText = "text"

var apiLong = templates.LongDesc(`
Query the runtime API of the Deckhouse controller (/api/v1).

The controller serves the API only when it runs Module v2
(DECKHOUSE_ENABLE_MODULE_V2=true). Every route is fetched over HTTP through the
pods/proxy subresource of the leader pod, which reaches the controller's TCP
listener. The controller of deckhouse main keeps the packages subtree, which
carries registry credentials and rendered Secrets, on a Unix socket inside its
container, so the packages commands answer 404 until it publishes them over TCP.

© Flant JSC 2026`)

// NewCommand returns the hidden "api" command tree, one leaf per API route.
func NewCommand() *cobra.Command {
	apiCmd := &cobra.Command{
		Use:    "api",
		Short:  "Query the runtime API of the Deckhouse controller.",
		Long:   apiLong,
		Hidden: true,
	}

	apiCmd.PersistentFlags().String("pod", "", "Controller pod to query instead of the leader.")

	apiCmd.AddCommand(
		newHealthzCommand(),
		newReadyzCommand(),
		newEndpointsCommand(),
		newMetricsCommand(),
		newPprofCommand(),
		newGetCommand(),
		newQueuesCommand(),
		newSchedulerCommand(),
		newRequirementsCommand(),
		newPackagesCommand(),
	)

	return apiCmd
}

func newHealthzCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "healthz",
		Short: "Check that the controller process is up (GET /healthz).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			status, err := client.Healthz(cmd.Context())
			if err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), status)

			return nil
		},
	}
}

func newReadyzCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "readyz",
		Short: "Check the readiness of the controller replica (GET /readyz); fails while it is not ready.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			readiness, err := client.Readyz(cmd.Context())
			if err != nil {
				return err
			}

			if !readiness.Ready {
				return fmt.Errorf("%w: %s", apiclient.ErrNotReady, readiness.Message)
			}

			fmt.Fprintln(cmd.OutOrStdout(), readiness.Message)

			return nil
		},
	}
}

func newEndpointsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "endpoints",
		Short: "List the routes the API serves (GET /endpoints).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			endpoints, err := client.Endpoints(cmd.Context())
			if err != nil {
				return err
			}

			for _, endpoint := range endpoints {
				fmt.Fprintln(cmd.OutOrStdout(), endpoint.Method, endpoint.Path)
			}

			return nil
		},
	}
}

func newMetricsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "metrics",
		Short: "Print the Prometheus metrics of the controller (GET /metrics).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			metrics, err := client.Metrics(cmd.Context())
			if err != nil {
				return err
			}

			return writeText(cmd.OutOrStdout(), metrics)
		},
	}
}

func newPprofCommand() *cobra.Command {
	pprofCmd := &cobra.Command{
		Use:   "pprof NAME",
		Short: "Fetch /debug/pprof/NAME: heap, goroutine, allocs, block, mutex, threadcreate, profile, trace, cmdline or symbol.",
		Example: `  d8 system api pprof heap > heap.pprof
  d8 system api pprof profile --seconds 30 > cpu.pprof
  d8 system api pprof goroutine --debug 2`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			seconds, err := cmd.Flags().GetInt("seconds")
			if err != nil {
				return err
			}

			debug, err := cmd.Flags().GetInt("debug")
			if err != nil {
				return err
			}

			// Profiles are binary unless debug asks named ones for text; cmdline and symbol are text anyway.
			binary := debug == 0 && args[0] != "cmdline" && args[0] != "symbol"
			if binary && cmd.OutOrStdout() == os.Stdout && term.IsTerminal(int(os.Stdout.Fd())) {
				return errors.New("refusing to write a binary profile to the terminal: redirect stdout to a file or pass --debug 1")
			}

			query := url.Values{}
			if seconds > 0 {
				query.Set("seconds", strconv.Itoa(seconds))
			}

			if debug > 0 {
				query.Set("debug", strconv.Itoa(debug))
			}

			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			profile, err := client.Pprof(cmd.Context(), args[0], query)
			if err != nil {
				return err
			}

			_, err = cmd.OutOrStdout().Write(profile)

			return err
		},
	}

	pprofCmd.Flags().Int("seconds", 0, "How long profile and trace record.")
	pprofCmd.Flags().Int("debug", 0, "Text format of named profiles: 1 or 2 (0 writes the binary format).")

	return pprofCmd
}

func newGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get PATH",
		Short: "GET any route and print the answer as it comes.",
		Example: `  d8 system api get '/api/v1/queues/dump?output=yaml'
  d8 system api get /debug/vars`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := url.Parse(args[0])
			if err != nil || !strings.HasPrefix(target.Path, "/") {
				return fmt.Errorf("PATH must be an absolute path with an optional query, got %q", args[0])
			}

			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			body, err := client.Get(cmd.Context(), target.Path, target.Query())
			if err != nil {
				return err
			}

			_, err = cmd.OutOrStdout().Write(body)

			return err
		},
	}
}

func newQueuesCommand() *cobra.Command {
	queuesCmd := &cobra.Command{Use: "queues", Short: "Task queues of the packages."}

	dumpCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump the task queues with their tasks (GET " + apiclient.APIPrefix + "/queues/dump).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			name, err := cmd.Flags().GetString("name")
			if err != nil {
				return err
			}

			return runDump(cmd, apiclient.APIPrefix+"/queues/dump", name, func(ctx context.Context, client *apiclient.Client, w io.Writer) error {
				dump, err := client.Queues(ctx, name)
				if err != nil {
					return err
				}

				printQueues(w, dump)

				return nil
			})
		},
	}

	dumpCmd.Flags().String("name", "", "Only the queues of this package.")
	addOutputFlag(dumpCmd, true)
	queuesCmd.AddCommand(dumpCmd)

	return queuesCmd
}

func newSchedulerCommand() *cobra.Command {
	schedulerCmd := &cobra.Command{Use: "scheduler", Short: "Scheduling state of the packages."}

	dumpCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump the scheduler nodes (GET " + apiclient.APIPrefix + "/scheduler/dump).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			name, err := cmd.Flags().GetString("name")
			if err != nil {
				return err
			}

			return runDump(cmd, apiclient.APIPrefix+"/scheduler/dump", name, func(ctx context.Context, client *apiclient.Client, w io.Writer) error {
				if name == "" {
					dump, err := client.Scheduler(ctx)
					if err != nil {
						return err
					}

					return printSchedulerNodes(w, dump.Nodes)
				}

				node, err := client.SchedulerNode(ctx, name)
				if err != nil {
					return err
				}

				return printSchedulerNodes(w, map[string]apiclient.SchedulerNode{name: *node})
			})
		},
	}

	dumpCmd.Flags().String("name", "", "Only the node of this package.")
	addOutputFlag(dumpCmd, true)
	schedulerCmd.AddCommand(dumpCmd)

	return schedulerCmd
}

func newRequirementsCommand() *cobra.Command {
	requirementsCmd := &cobra.Command{Use: "requirements", Short: "Values stored for the release requirement checks."}

	dumpCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump the requirement values (GET " + apiclient.APIPrefix + "/requirements/dump).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDump(cmd, apiclient.APIPrefix+"/requirements/dump", "", nil)
		},
	}

	addOutputFlag(dumpCmd, false)
	requirementsCmd.AddCommand(dumpCmd)

	return requirementsCmd
}

func newPackagesCommand() *cobra.Command {
	packagesCmd := &cobra.Command{
		Use:   "packages",
		Short: "Applications and modules (GET " + apiclient.APIPrefix + "/packages/...); 404 while the controller keeps them on its socket.",
	}

	dumpCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump the packages with their status, definition, values and hooks (GET " + apiclient.APIPrefix + "/packages/dump).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			name, err := cmd.Flags().GetString("name")
			if err != nil {
				return err
			}

			return runDump(cmd, apiclient.APIPrefix+"/packages/dump", name, func(ctx context.Context, client *apiclient.Client, w io.Writer) error {
				if name == "" {
					dump, err := client.Packages(ctx)
					if err != nil {
						return err
					}

					return printPackages(w, dump)
				}

				pkg, err := client.Package(ctx, name)
				if err != nil {
					return err
				}

				dump := &apiclient.PackagesDump{}
				if pkg.Application != nil {
					dump.Apps = map[string]apiclient.Application{name: *pkg.Application}
				} else {
					dump.Modules = map[string]apiclient.Module{name: *pkg.Module}
				}

				return printPackages(w, dump)
			})
		},
	}

	dumpCmd.Flags().String("name", "", "Only this package.")
	addOutputFlag(dumpCmd, true)

	globalCmd := &cobra.Command{Use: "global", Short: "The global module."}
	globalDumpCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump the global module (GET " + apiclient.APIPrefix + "/packages/global/dump).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDump(cmd, apiclient.APIPrefix+"/packages/global/dump", "", nil)
		},
	}

	addOutputFlag(globalDumpCmd, false)
	globalCmd.AddCommand(globalDumpCmd)

	renderCmd := &cobra.Command{
		Use:   "render NAME",
		Short: "Render the Helm manifests of a package (GET " + apiclient.APIPrefix + "/packages/render/NAME).",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newClient(cmd)
			if err != nil {
				return err
			}

			manifests, err := client.Render(cmd.Context(), args[0])
			if err != nil {
				return err
			}

			return writeText(cmd.OutOrStdout(), []byte(manifests))
		},
	}

	snapshotsCmd := &cobra.Command{
		Use:   "snapshots NAME",
		Short: "Dump the hook snapshots of a package (GET " + apiclient.APIPrefix + "/packages/snapshots/NAME).",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDump(cmd, apiclient.APIPrefix+"/packages/snapshots/"+url.PathEscape(args[0]), "", nil)
		},
	}

	addOutputFlag(snapshotsCmd, false)

	packagesCmd.AddCommand(dumpCmd, globalCmd, renderCmd, snapshotsCmd)

	return packagesCmd
}

// newClient connects through pods/proxy to the pod --pod names, or to the leader.
func newClient(cmd *cobra.Command) (*apiclient.Client, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	podName, err := cmd.Flags().GetString("pod")
	if err != nil {
		return nil, err
	}

	var pod *corev1.Pod
	if podName != "" {
		pod, err = apiclient.Pod(cmd.Context(), kubeCl, podName)
	} else {
		pod, err = apiclient.LeaderPod(cmd.Context(), kubeCl)
	}

	if err != nil {
		return nil, err
	}

	proxy, err := apiclient.NewProxyTransport(config, kubeCl, pod)
	if err != nil {
		return nil, err
	}

	return apiclient.New(proxy), nil
}

// addOutputFlag registers -o; text is offered where a summary view exists.
func addOutputFlag(cmd *cobra.Command, withText bool) {
	usage := "Output format: yaml|json, as the API encodes it."
	if withText {
		usage = "Output format: yaml|json, as the API encodes it, or text for a summary."
	}

	cmd.Flags().StringP("output", "o", apiclient.OutputYAML, usage)
}

// runDump prints the dump at path as the API encodes it in yaml or json, or hands
// the typed client to text for the summary view. A non-empty name narrows the dump.
func runDump(cmd *cobra.Command, path, name string, text func(context.Context, *apiclient.Client, io.Writer) error) error {
	format, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}

	switch {
	case format == apiclient.OutputYAML, format == apiclient.OutputJSON:
	case format == outputText && text != nil:
	case text != nil:
		return fmt.Errorf("unknown output %q, want yaml, json or text", format)
	default:
		return fmt.Errorf("unknown output %q, want yaml or json", format)
	}

	client, err := newClient(cmd)
	if err != nil {
		return err
	}

	if format == outputText {
		return text(cmd.Context(), client, cmd.OutOrStdout())
	}

	query := url.Values{"output": {format}}
	if name != "" {
		query.Set("name", name)
	}

	body, err := client.Get(cmd.Context(), path, query)
	if err != nil {
		return err
	}

	return writeText(cmd.OutOrStdout(), body)
}

// writeText writes body and makes sure it ends with a newline.
func writeText(w io.Writer, body []byte) error {
	if _, err := w.Write(body); err != nil {
		return err
	}

	if len(body) > 0 && body[len(body)-1] != '\n' {
		_, err := io.WriteString(w, "\n")

		return err
	}

	return nil
}
