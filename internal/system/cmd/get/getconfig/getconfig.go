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

package get

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// Unified function to get configuration from Kubernetes cluster.
// - The output will be syntax highlighted if stdout is a terminal for readability.
// - Plain text will be printed otherwise (for pipes/redirects).
func BaseGetConfigCMD(cmd *cobra.Command, _ string, secret, dataKey string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	_, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	secretConfig, err := kubeCl.CoreV1().
		Secrets("kube-system").
		Get(context.Background(), secret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Error fetching secret: %w", err)
	}

	data, ok := secretConfig.Data[dataKey]
	if !ok {
		return fmt.Errorf("Data key %q not found in secret %q", dataKey, secret)
	}

	fmt.Print(string(data))
	return nil
}
