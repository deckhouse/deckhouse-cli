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

const DefaultDebugImage = "ubuntu:20.04"

func getDebugImage(cmd *cobra.Command) (string, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return "", fmt.Errorf("failed to get kubeconfig flag: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return "", fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	command := []string{"deckhouse-controller", "global", "values", "-o", "json"}
	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return "", fmt.Errorf("failed to get Deckhouse pod: %w", err)
	}

	executor, err := utilk8s.ExecInPod(config, kubeCl, command, podName, "d8-system", "deckhouse")
	if err != nil {
		return "", fmt.Errorf("failed to create executor for pod %s: %w", podName, err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err = executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
		return "", fmt.Errorf("failed to execute command in pod: %w", err)
	}

	var gv GlobalValues
	err = json.Unmarshal([]byte(stdout.String()), &gv)
	if err != nil {
		return "", fmt.Errorf("failed to parse JSON response: %w", err)
	}

	imageAddress := gv.ModulesImages.Registry.Address
	imagePath := gv.ModulesImages.Registry.Path
	imageHash := gv.ModulesImages.Digests.Common.DebugContainer

	containerName := fmt.Sprintf("%s%s@%s", imageAddress, imagePath, imageHash)
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

		originalPreRunE := debugCmd.PreRunE
		debugCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
			imageFlag := cmd.Flags().Lookup("image")
			if imageFlag != nil && imageFlag.Value.String() == "" {
				debugImage, err := getDebugImage(cmd)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: cannot get debug container image from cluster: %v\n", err)
					fmt.Fprintf(os.Stderr, "Tip: Check if KUBECONFIG is set or use --kubeconfig flag\n")

					fmt.Fprintf(os.Stderr, "Using default debug container image: %s\n", DefaultDebugImage)
					err = cmd.Flags().Set("image", DefaultDebugImage)
					if err != nil {
						return fmt.Errorf("Failed to set default image: %w", err)
					}
				} else {
					fmt.Fprintf(os.Stderr, "Using debug container image: %s\n", debugImage)
					err = cmd.Flags().Set("image", debugImage)
					if err != nil {
						return fmt.Errorf("Failed to set debug image: %w", err)
					}
				}
			}

			if originalPreRunE != nil {
				return originalPreRunE(cmd, args)
			}
			return nil
		}
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
