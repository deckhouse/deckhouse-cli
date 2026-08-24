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
	"fmt"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var istioLong = templates.LongDesc(`
Start an interactive debug container with istioctl and the RBAC needed to
inspect pods in a target namespace (get/list pods and create pods/portforward).

The ServiceAccount is created in --namespace. The Role and RoleBinding are
created in --target-namespace (defaults to --namespace). The debug image is
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
		Use:           "istio [-- command ...]",
		Short:         "Run an interactive istioctl debug container",
		Long:          istioLong,
		Example:       istioExample,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Match kubectl: first SIGINT/SIGTERM must stop the attach session
			// instead of being swallowed by the d8 root graceful handler.
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)

			if len(args) > 0 {
				opts.Command = args
			}

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
				opts.Namespace, err = kubeconfigNamespace(kubeconfigPath, contextName)
				if err != nil {
					return err
				}
			}

			if opts.TargetNamespace == "" {
				opts.TargetNamespace = opts.Namespace
			}

			return Run(cmd.Context(), kubeCl, restConfig, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Namespace, "namespace", "n", "", "Namespace for the ServiceAccount and debug pod (default: current kubeconfig namespace)")
	cmd.Flags().StringVar(&opts.TargetNamespace, "target-namespace", "", "Namespace whose pods istioctl should be able to inspect (default: --namespace)")
	cmd.Flags().StringVar(&opts.Image, "image", "", "Debug container image (default: ConfigMap d8-system/debug-container)")
	cmd.Flags().StringP("kubeconfig", "k", utilk8s.DefaultKubeconfigPath(), "Path to kubeconfig file")
	cmd.Flags().String("context", "", "The name of the kubeconfig context to use")

	return cmd
}

func kubeconfigNamespace(kubeconfigPath, contextName string) (string, error) {
	overrides := &clientcmd.ConfigOverrides{}
	if contextName != utilk8s.DefaultKubeContext {
		overrides.CurrentContext = contextName
	}

	ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		overrides,
	).Namespace()
	if err != nil {
		return "", fmt.Errorf("resolve namespace from kubeconfig: %w", err)
	}

	if ns == "" {
		return "default", nil
	}

	return ns, nil
}
