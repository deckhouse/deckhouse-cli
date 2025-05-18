package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/remotecommand"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
)

type GlobalValues struct {
	ModulesImages struct {
		Digests struct {
			Common struct {
				DebugContainer string `json:"debugContainer"`
			} `json:"common"`
		} `json:"digests"`
		Registry struct {
			Address string `json:"address"`
			Path    string `json:"path"`
		} `json:"registry"`
	} `json:"modulesImages"`
}

func getDebugImage(cmd *cobra.Command) (string, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return "", fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return "", fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	command := []string{"deckhouse-controller", "global", "values", "-o", "json"}
	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	executor, err := utilk8s.ExecInPod(config, kubeCl, command, podName, "d8-system", "deckhouse")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err = executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
		return "", err
	}

	fmt.Printf("%s\n", stdout.String()) // TODO: remove it

	var gv GlobalValues
	err = json.Unmarshal([]byte(stdout.String()), &gv)
	if err != nil {
		fmt.Printf("failed JSON parsing: %v\n", err)
		return "", err
	}

	imageAddress := gv.ModulesImages.Registry.Address
	imagePath := gv.ModulesImages.Registry.Path
	imageHash := gv.ModulesImages.Digests.Common.DebugContainer
	containerName := fmt.Sprintf("%s%s@%s\n", imageAddress, imagePath, imageHash)

	// loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	// configOverrides := &clientcmd.ConfigOverrides{}

	// if cmd.Flags().Lookup("kubeconfig") != nil {
	// 	path, err := cmd.Flags().GetString("kubeconfig")
	// 	if err == nil && path != "" {
	// 		loadingRules.ExplicitPath = path
	// 	}
	// }

	// kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	// config, err := kubeConfig.ClientConfig()
	// if err != nil {
	// 	return "", fmt.Errorf("failed to setup Kubernetes client: %w", err)
	// }

	// kubeCl, err := kubernetes.NewForConfig(config)
	// if err != nil {
	// 	return "", fmt.Errorf("failed to create Kubernetes client: %w", err)
	// }

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	// configMap, err := kubeCl.CoreV1().ConfigMaps("default").Get(ctx, "debug-container", v1.GetOptions{})
	// if err != nil {
	// 	return "", fmt.Errorf("failed to get configmap: %w", err)
	// }

	// containerName, ok := configMap.Data["container_name"]
	// if !ok {
	// 	return "", fmt.Errorf("container_name not found in ConfigMap")
	// }

	// containerName = strings.TrimSpace(containerName)
	// if containerName == "" {
	// 	return "", fmt.Errorf("debug container image not found in ConfigMap")
	// }
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
