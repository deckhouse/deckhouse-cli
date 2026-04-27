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

package utilk8s

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"
)

// NewDynamicClient creates a dynamic Kubernetes client from cobra command flags.
// It reads "kubeconfig" and "context" persistent flags registered on a parent command.
func NewDynamicClient(cmd *cobra.Command) (dynamic.Interface, error) {
	kubeconfigPath, _ := cmd.Flags().GetString("kubeconfig")
	contextName, _ := cmd.Flags().GetString("context")

	restConfig, _, err := SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	return dyn, nil
}
