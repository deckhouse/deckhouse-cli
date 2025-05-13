package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
)

func getDebugImage(cmd *cobra.Command) (string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}

	if cmd.Flags().Lookup("kubeconfig") != nil {
		path, err := cmd.Flags().GetString("kubeconfig")
		if err == nil && path != "" {
			loadingRules.ExplicitPath = path
		}
	}

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	config, err := kubeConfig.ClientConfig()
	if err != nil {
		return "", fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	configMap, err := kubeCl.CoreV1().ConfigMaps("default").Get(ctx, "debug-container", v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get configmap: %w", err)
	}

	containerName, ok := configMap.Data["container_name"]
	if !ok {
		return "", fmt.Errorf("container_name not found in ConfigMap")
	}

	containerName = strings.TrimSpace(containerName)
	if containerName == "" {
		return "", fmt.Errorf("debug container image not found in ConfigMap")
	}
	return containerName, nil
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
					fmt.Fprintf(os.Stderr, "Error: cannot get debug container image from cluster: %v\n", err)
					fmt.Fprintf(os.Stderr, "Tip: Check if KUBECONFIG is set or use --kubeconfig flag\n")
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
