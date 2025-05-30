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

package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
)

const (
	cmNamespace = "d8-system"
	cmName      = "debug-container"
	cmImageKey  = "image"
)

func getKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()

	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = kubeconfigPath
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	config, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load kubernetes config: %w", err)
	}

	return config, nil
}

func getDebugImage(cmd *cobra.Command) (string, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return "", fmt.Errorf("failed to get kubeconfig flag: %w", err)
	}

	config, err := getKubeConfig(kubeconfigPath)
	if err != nil {
		return "", fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	configMap, err := kubeCl.CoreV1().ConfigMaps(cmNamespace).Get(ctx, cmName, v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get ConfigMap %s/%s: %w", cmNamespace, cmName, err)
	}

	imageName, ok := configMap.Data[cmImageKey]
	if !ok {
		return "", fmt.Errorf("key '%s' not found in ConfigMap %s/%s", cmImageKey, cmNamespace, cmName)
	}

	if imageName == "" {
		return "", fmt.Errorf("image name is empty in ConfigMap %s/%s", cmNamespace, cmName)
	}

	return imageName, nil
}

func init() {
	kubectlCmd := kubecmd.NewDefaultKubectlCommand()
	kubectlCmd.Use = "k"
	kubectlCmd.Aliases = []string{"kubectl"}
	kubectlCmd = ReplaceCommandName("kubectl", "d8 k", kubectlCmd)

	var debugCmd *cobra.Command
	for _, cmd := range kubectlCmd.Commands() {
		if cmd.Name() == "debug" {
			debugCmd = cmd
			break
		}
	}

	if debugCmd != nil {
		if imageFlag := debugCmd.Flags().Lookup("image"); imageFlag != nil {
			imageFlag.Usage = "Container image to use for debug container. If not specified, the platform's recommended image will be used."
		}
	}

	rootCmd.PersistentFlags().String("kubeconfig", "", "Path to the kubeconfig file")

	originalPersistentPreRunE := kubectlCmd.PersistentPreRunE
	kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if cmd.Name() == "debug" || (cmd.Parent() != nil && cmd.Parent().Name() == "debug") {
			imageFlag := cmd.Flags().Lookup("image")
			if imageFlag != nil && imageFlag.Value.String() == "" {
				debugImage, err := getDebugImage(cmd)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: cannot get debug container image from cluster: %v\n", err)
					fmt.Fprintf(os.Stderr, "Continuing with default kubectl behavior...\n")
				} else {
					fmt.Fprintf(os.Stderr, "Using debug container image: %s\n", debugImage)
					cmd.Flags().Set("image", debugImage)
				}
			}
		}

		if originalPersistentPreRunE != nil {
			return originalPersistentPreRunE(cmd, args)
		}
		return nil
	}

	// Based on https://github.com/kubernetes/kubernetes/blob/v1.29.3/staging/src/k8s.io/component-base/cli/run.go#L88

	kubectlCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)
	kubectlCmd.SilenceErrors = true
	logs.AddFlags(kubectlCmd.PersistentFlags())

	switch {
	case kubectlCmd.PersistentPreRun != nil:
		pre := kubectlCmd.PersistentPreRun
		kubectlCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			pre(cmd, args)
		}
	case kubectlCmd.PersistentPreRunE != nil:
		pre := kubectlCmd.PersistentPreRunE
		kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			logs.InitLogs()
			return pre(cmd, args)
		}
	default:
		kubectlCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
		}
	}

	rootCmd.AddCommand(kubectlCmd)
}
