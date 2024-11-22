package utilk8s

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/backup/utilk8s"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"os/exec"
	"sigs.k8s.io/yaml"
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

	content, err := yaml.Marshal(secretConfig)
	if err != nil {
		return fmt.Errorf("Error marshaling secret to YAML: %w", err)
	}

	type Secret struct {
		Data map[string]string `yaml:"data"`
	}

	var secretStruct Secret
	err = yaml.Unmarshal(content, &secretStruct)
	if err != nil {
		return fmt.Errorf("Error parsing YAML file: %w", err)
	}
	decodedValue, err := base64.StdEncoding.DecodeString(secretStruct.Data[dataKey])
	if err != nil {
		return fmt.Errorf("Error decoding base64 value for field '%w': %w", dataKey, err)
	}

	tempFileName, err := os.CreateTemp(os.TempDir(), "secret.*.yaml")
	if err != nil {
		return fmt.Errorf("Can't save cluster configuration: %w\n", err)
		return err
	}
	err = os.WriteFile(tempFileName.Name(), decodedValue, 0644)
	if err != nil {
		return fmt.Errorf("Error writing decoded data to file: %w", err)
	}

	cmdExec := exec.Command(editor, tempFileName.Name())
	cmdExec.Stdin = os.Stdin
	cmdExec.Stdout = os.Stdout
	cmdExec.Stderr = os.Stderr
	err = cmdExec.Run()
	if err != nil {
		return fmt.Errorf("Error opening in editor: %w", err)
	}

	updatedContent, err := os.ReadFile(tempFileName.Name())
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
