package utilk8s

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/backup/utilk8s"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
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
		log.Fatalf("Error fetching secret: %s", err.Error())
	}

	content, err := yaml.Marshal(secretConfig)
	if err != nil {
		log.Fatalf("Error marshaling secret to YAML: %s", err.Error())
	}

	type Secret struct {
		Data map[string]string `yaml:"data"`
	}

	var secretStruct Secret
	err = yaml.Unmarshal(content, &secretStruct)
	if err != nil {
		log.Fatalf("Error parsing YAML file: %v", err)
	}
	decodedValue, err := base64.StdEncoding.DecodeString(secretStruct.Data[dataKey])
	if err != nil {
		log.Fatalf("Error decoding base64 value for field '%s': %s", dataKey, err.Error())
	}

	tempFileName, err := os.CreateTemp(os.TempDir(), "secret.*.yaml")
	if err != nil {
		log.Fatalf("can't save cluster configuration: %s\n", err)
		return err
	}
	err = os.WriteFile(tempFileName.Name(), decodedValue, 0644)
	if err != nil {
		log.Fatalf("Error writing decoded data to file: %s", err.Error())
	}

	cmdExec := exec.Command(editor, tempFileName.Name())
	cmdExec.Stdin = os.Stdin
	cmdExec.Stdout = os.Stdout
	cmdExec.Stderr = os.Stderr
	err = cmdExec.Run()
	if err != nil {
		log.Fatalf("Error opening in editor: %s", err.Error())
	}

	updatedContent, err := os.ReadFile(tempFileName.Name())
	if err != nil {
		log.Fatalf("Error reading updated file: %s", err.Error())
	}

	secretData := secretConfig.Data[dataKey]
	if string(secretData) == string(updatedContent) {
		fmt.Println("Configurations are equal. Nothing to update.")
		return nil
	}

	secretConfig.Data[dataKey] = updatedContent

	_, err = kubeCl.CoreV1().Secrets("kube-system").Update(context.Background(), secretConfig, metav1.UpdateOptions{})
	if err != nil {
		log.Fatalf("Error updating secret: %s", err.Error())
	}

	fmt.Println("Secret updated successfully")

	return err
}
