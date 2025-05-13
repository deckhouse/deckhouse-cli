package cmd

import (
	"context"
	"encoding/json"
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
	kubeconfigPath := ""

	if cmd.Flags().Lookup("kubeconfig") != nil {
		flag, err := cmd.Flags().GetString("kubeconfig")
		if err == nil && flag != "" {
			kubeconfigPath = flag
		}
	}

	if kubeconfigPath == "" && cmd.Parent() != nil && cmd.Parent().Flags().Lookup("kubeconfig") != nil {
		flag, err := cmd.Parent().Flags().GetString("kubeconfig")
		if err == nil && flag != "" {
			kubeconfigPath = flag
		}
	}

	if kubeconfigPath == "" {
		rootCommand := cmd
		for rootCommand.Parent() != nil {
			rootCommand = rootCommand.Parent()
		}

		if rootCommand.PersistentFlags().Lookup("kubeconfig") != nil {
			flag, err := rootCommand.PersistentFlags().GetString("kubeconfig")
			if err == nil && flag != "" {
				kubeconfigPath = flag
			}
		}
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return "", fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	configMap, err := kubeCl.CoreV1().ConfigMaps("d8-cloud-instance-manager").Get(ctx, "bashible-apiserver-files", v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get configmap: %w", err)
	}

	imagesDigestsJSON, ok := configMap.Data["images_digests.json"]
	if !ok {
		return "", fmt.Errorf("images_digests.json not found in ConfigMap")
	}

	var imageData struct {
		Common struct {
			DebugContainer string `json:"debugContainer"`
		} `json:"common"`
	}
	if err := json.Unmarshal([]byte(imagesDigestsJSON), &imageData); err != nil {
		return "", fmt.Errorf("failed to parse images_digests.json: %v", err)
	}

	image := strings.TrimSpace(imageData.Common.DebugContainer)
	if image == "" {
		return "", fmt.Errorf("debug container image not found in ConfigMap")
	}
	return image, nil
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

	kubectlCmd.PersistentFlags().String("kubeconfig", "", "Path to the kubeconfig file")
	rootCmd.PersistentFlags().String("kubeconfig", "", "Path to the kubeconfig file")

	originalPersistentPreRunE := kubectlCmd.PersistentPreRunE
	kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if cmd.Name() == "debug" || (cmd.Parent() != nil && cmd.Parent().Name() == "debug") {
			imageFlag := cmd.Flags().Lookup("image")
			if imageFlag != nil && imageFlag.Value.String() == "" {
				debugImage, err := getDebugImage(cmd)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: cannot get debug container image from cluster: %v\n", err)
					return fmt.Errorf("failed to get debug container image: %w", err)
				}
				fmt.Fprintf(os.Stderr, "Using debug container image: %s\n", debugImage)
				cmd.Flags().Set("image", debugImage)
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
