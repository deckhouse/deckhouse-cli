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

package istio

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var istioLong = templates.LongDesc(`
Start an interactive debug container with istioctl and the RBAC needed to
inspect pods in a target namespace (get/list pods and create pods/portforward).

The ServiceAccount is created in --namespace. Roles and RoleBindings are
created in --target-namespace (workload pods) and --istio-namespace (default
d8-istio): pods get/list, pods/portforward create, and in the Istio namespace
also serviceaccounts get plus serviceaccounts/token create for istiod RPC
auth (istioctl proxy-status). No Istio config mutations. The debug image is
taken from ConfigMap d8-system/debug-container unless --image is set.

The pod is deleted when the session ends. RBAC objects are left in place so
the next run can reuse them.

© Flant JSC 2026`)

var istioExample = templates.Examples(`
	# Debug the current kubeconfig namespace
	d8 network istio

	# Run the debug pod in one namespace with access to another
	d8 network istio -n debug-tools --target-namespace production

	# Override the debug image
	d8 network istio --image registry.example/debug:latest`)

func NewCommand() *cobra.Command {
	opts := Options{
		Command: []string{"bash"},
	}

	cmd := &cobra.Command{
		Use:           "istio",
		Short:         "Run an interactive istioctl debug container",
		Long:          istioLong,
		Example:       istioExample,
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Interactive bash only. One-shot `istio -- command` is not supported:
			// poll-then-attach loses early stdout, treats a fast Succeeded pod as an
			// error, and cannot propagate the container exit code (attach has none).
			kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
			if err != nil {
				return fmt.Errorf("read --kubeconfig: %w", err)
			}

			contextName, err := cmd.Flags().GetString("context")
			if err != nil {
				return fmt.Errorf("read --context: %w", err)
			}

			restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
			if err != nil {
				return fmt.Errorf("setup Kubernetes client: %w", err)
			}

			if opts.Namespace == "" {
				opts.Namespace, err = transport.KubeconfigNamespace(kubeconfigPath, contextName)
				if err != nil {
					return err
				}
			}

			if opts.TargetNamespace == "" {
				opts.TargetNamespace = opts.Namespace
			}

			if opts.IstioNamespace == "" {
				opts.IstioNamespace = defaultIstioNS
			}

			err = Run(cmd.Context(), kubeCl, restConfig, opts)
			if errors.Is(err, context.Canceled) {
				// SIGINT/SIGTERM cancels cmd.Context() via the root graceful
				// handler. Run already deletes the debug pod in a defer that
				// uses context.Background(), so treat cancel as a clean stop.
				return nil
			}

			return err
		},
	}

	cmd.Flags().StringVarP(&opts.Namespace, "namespace", "n", "", "Namespace for the ServiceAccount and debug pod (default: current kubeconfig namespace)")
	cmd.Flags().StringVar(&opts.TargetNamespace, "target-namespace", "", "Namespace whose workload pods istioctl should inspect (default: --namespace)")
	cmd.Flags().StringVar(&opts.IstioNamespace, "istio-namespace", defaultIstioNS, "Istio control-plane namespace for read-only access (istioctl proxy-status)")
	cmd.Flags().StringVar(&opts.Image, "image", "", "Debug container image (default: ConfigMap d8-system/debug-container)")
	cmd.Flags().StringP("kubeconfig", "k", utilk8s.DefaultKubeconfigPath(), "Path to kubeconfig file")
	cmd.Flags().String("context", "", "The name of the kubeconfig context to use")

	return cmd
}
