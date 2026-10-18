package edit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const secretNamespace = "kube-system"

// CreateOptions allows the edited Secret to be created when it is absent.
// A nil *CreateOptions means a missing Secret is an error.
type CreateOptions struct {
	Labels map[string]string
}

// editFunc receives the current content of the data key and returns the edited one.
type editFunc func(content []byte) ([]byte, error)

func BaseEditConfigCMD(cmd *cobra.Command, _ string, secret, dataKey string, createOpts *CreateOptions) error {
	editor, err := cmd.Flags().GetString("editor")
	if err != nil {
		return fmt.Errorf("Failed to get editor from --editor flag: %w", err)
	}

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

	return editSecret(context.Background(), kubeCl, secret, dataKey, createOpts, func(content []byte) ([]byte, error) {
		return editInEditor(editor, content)
	})
}

func editSecret(
	ctx context.Context,
	kubeCl kubernetes.Interface,
	secret, dataKey string,
	createOpts *CreateOptions,
	edit editFunc,
) error {
	secretConfig, err := kubeCl.CoreV1().
		Secrets(secretNamespace).
		Get(ctx, secret, metav1.GetOptions{})

	absent := apierrors.IsNotFound(err) && createOpts != nil
	if err != nil && !absent {
		return fmt.Errorf("Error fetching secret: %w", err)
	}

	if absent {
		secretConfig = newSecret(secret, createOpts.Labels)
	}

	currentContent := secretConfig.Data[dataKey]

	updatedContent, err := edit(currentContent)
	if err != nil {
		return err
	}

	if sha256.Sum256(currentContent) == sha256.Sum256(updatedContent) {
		fmt.Println("Configurations are equal. Nothing to update.")
		return nil
	}

	if absent {
		return createSecret(ctx, kubeCl, secretConfig, dataKey, updatedContent)
	}

	encodedValue, err := encodeSecretTmp(updatedContent, dataKey)
	if err != nil {
		return fmt.Errorf("Error encoding secret: %w", err)
	}

	_, err = kubeCl.CoreV1().
		Secrets(secretNamespace).Patch(ctx, secret, types.MergePatchType, encodedValue, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("Error updating secret: %w", err)
	}

	fmt.Println("Secret updated successfully")

	return nil
}

func newSecret(name string, labels map[string]string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: secretNamespace,
			Labels:    labels,
		},
		Type: v1.SecretTypeOpaque,
	}
}

func createSecret(ctx context.Context, kubeCl kubernetes.Interface, secretConfig *v1.Secret, dataKey string, content []byte) error {
	if len(bytes.TrimSpace(content)) == 0 {
		fmt.Println("Configuration is empty. Nothing to create.")
		return nil
	}

	secretConfig.Data = map[string][]byte{dataKey: content}

	_, err := kubeCl.CoreV1().
		Secrets(secretNamespace).
		Create(ctx, secretConfig, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("Secret %s/%s was created while editing, run the command again to edit it: %w",
			secretNamespace, secretConfig.Name, err)
	}

	if err != nil {
		return fmt.Errorf("Error creating secret: %w", err)
	}

	fmt.Println("Secret created successfully")

	return nil
}

func editInEditor(editor string, content []byte) ([]byte, error) {
	tempFile, err := writeSecretTmp(content)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}()

	cmdExec := exec.Command(editor, tempFile.Name())
	cmdExec.Stdin = os.Stdin
	cmdExec.Stdout = os.Stdout
	cmdExec.Stderr = os.Stderr

	err = cmdExec.Run()
	if err != nil {
		return nil, fmt.Errorf("Error opening in editor: %w", err)
	}

	updatedContent, err := os.ReadFile(tempFile.Name())
	if err != nil {
		return nil, fmt.Errorf("Error reading updated file: %w", err)
	}

	return updatedContent, nil
}

func writeSecretTmp(content []byte) (*os.File, error) {
	tempFile, err := os.CreateTemp(os.TempDir(), "secret.*.yaml")
	if err != nil {
		return nil, fmt.Errorf("can't save cluster configuration: %w", err)
	}

	_, err = tempFile.Write(content)
	if err != nil {
		return nil, fmt.Errorf("Error writing decoded data to file: %w", err)
	}

	if err = tempFile.Sync(); err != nil {
		return nil, fmt.Errorf("Sync temp file buffers to disk: %w", err)
	}

	return tempFile, nil
}

func encodeSecretTmp(updatedContent []byte, dataKey string) ([]byte, error) {
	encodedValue := base64.StdEncoding.EncodeToString(updatedContent)
	patchData := map[string]interface{}{
		"data": map[string]interface{}{
			dataKey: encodedValue,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return nil, fmt.Errorf("Error convert to json updated data: %w", err)
	}

	return patchBytes, nil
}
