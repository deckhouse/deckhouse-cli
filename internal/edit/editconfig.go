package edit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/backup/utilk8s"
	"github.com/spf13/cobra"
	"io"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"os"
	"os/exec"
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

	tempFile, err := writeSecretTmp(secretConfig, dataKey)
	if err != nil {
		return err
	}

	cmdExec := exec.Command(editor, tempFile.Name())
	cmdExec.Stdin = os.Stdin
	cmdExec.Stdout = os.Stdout
	cmdExec.Stderr = os.Stderr
	err = cmdExec.Run()
	if err != nil {
		return fmt.Errorf("Error opening in editor: %w", err)
	}

	updatedContent, err := openSecretTmp(tempFile, secretConfig, dataKey)
	patchBytes, err := json.Marshal(updatedContent)
	if err != nil {
		fmt.Errorf("Error convert to json updated data: %w", err)
	}

	_, err = kubeCl.CoreV1().Secrets("kube-system").Patch(context.TODO(), secret, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("Error updating secret: %w", err)
	}

	fmt.Println("Secret updated successfully")

	return err
}

func writeSecretTmp(secretConfig *v1.Secret, dataKey string) (*os.File, error) {
	tempFile, err := os.CreateTemp(os.TempDir(), "secret.*.yaml")
	if err != nil {
		return nil, fmt.Errorf("Can't save cluster configuration: %w\n", err)
	}

	_, err = tempFile.Write(secretConfig.Data[dataKey])
	if err != nil {
		return nil, fmt.Errorf("Error writing decoded data to file: %w", err)
	}
	return tempFile, nil
}

func openSecretTmp(tempFile *os.File, secretConfig *v1.Secret, dataKey string) ([]byte, error) {
	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("Error reading updated file: %w", err)
	}

	updatedContent, err := io.ReadAll(tempFile)
	if err != nil {
		return nil, fmt.Errorf("Error reading updated file: %w", err)
	}

	if bytes.Compare(secretConfig.Data[dataKey], bytes.TrimSpace(updatedContent)) == 0 {
		fmt.Println("Configurations are equal. Nothing to update.")
		return nil, err
	}
	return updatedContent, nil
}
