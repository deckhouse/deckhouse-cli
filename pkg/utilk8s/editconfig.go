package utilk8s

import (
	"context"
	"fmt"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "os"
        "os/exec"
	"github.com/deckhouse/deckhouse-cli/internal/backup/utilk8s"
	"github.com/spf13/cobra"
)

func BaseEditConfigCMD(cmd *cobra.Command, name, secret, dataKey string) error {
	editor, err := cmd.Flags().GetString("editor")
	if err != nil {
		return fmt.Errorf("Failed to open editor: %w", err)
	}

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	_, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	secretConfig, err := kubeCl.CoreV1().Secrets("kube-system").Get(context.Background(), secret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Error fetching secret: %w", err)
	}

	tempFile, err := os.CreateTemp(os.TempDir(), "secret.*.yaml")
	if err != nil {
		return fmt.Errorf("Can't save cluster configuration: %w\n", err)
		return err
	}
	err = os.WriteFile(tempFile.Name(), secretConfig.Data[dataKey], 0644)
	if err != nil {
		return fmt.Errorf("Error writing decoded data to file: %w", err)
	}

	cmdExec := exec.Command(editor, tempFile.Name())
	cmdExec.Stdin = os.Stdin
	cmdExec.Stdout = os.Stdout
	cmdExec.Stderr = os.Stderr
	err = cmdExec.Run()
	if err != nil {
		return fmt.Errorf("Error opening in editor: %w", err)
	}

	updatedContent, err := os.ReadFile(tempFile.Name())
	if err != nil {
		return fmt.Errorf("Error reading updated file: %w", err)
	}

	secretData := secretConfig.Data[dataKey]
	if string(secretData) == string(updatedContent) {
		fmt.Println("Configurations are equal. Nothing to update.")
		return nil
	}
	secretConfig.Data[dataKey] = updatedContent

	_, err = kubeCl.CoreV1().Secrets("kube-system").Update(context.Background(), secretConfig, metav1.UpdateOptions{})
	if err != nil {
	        return fmt.Errorf("Error updating secret: %w", err)
	}

	fmt.Println("Secret updated successfully")

	return err
}
