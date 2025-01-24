package operatemodule

import (
	"bytes"
	"context"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

func OptionsModule(cmd *cobra.Command, pathFromOption string) error {

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	const (
		apiProtocol   = "http"
		apiEndpoint   = "127.0.0.1"
		apiPort       = "9652"
		modulePath    = "module"
		labelSelector = "leader=true"
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	fullEndpointUrl := fmt.Sprintf("%s://%s:%s/%s/%s", apiProtocol, apiEndpoint, apiPort, modulePath, pathFromOption)
	getApi := []string{"curl", fullEndpointUrl}
	podName, err := getDeckhousePod(kubeCl, namespace, labelSelector, containerName)
	executor, err := execInPod(config, kubeCl, getApi, podName, namespace, containerName)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err = executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
		return err
	}

	fmt.Printf("%s\n", stdout.String())
	return err
}

func getDeckhousePod(kubeCl *kubernetes.Clientset, namespace string, labelSelector string, containerName string) (string, error) {
	pods, err := kubeCl.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("Error listing pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("No pods found with the label: %s", labelSelector)
	}

	pod := pods.Items[0]
	podName := pod.Name
	var containerFound bool
	for _, c := range pod.Spec.Containers {
		if c.Name == containerName {
			containerFound = true
			break
		}
	}
	if !containerFound {
		return "", fmt.Errorf("Container %q not found in pod %q", containerName, podName)
	}
	return podName, nil
}

func execInPod(config *rest.Config, kubeCl *kubernetes.Clientset, getApi []string, podName string, namespace string, containerName string) (remotecommand.Executor, error) {
	scheme := runtime.NewScheme()
	parameterCodec := runtime.NewParameterCodec(scheme)
	if err := v1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("Failed to create parameter codec: %w", err)
	}

	req := kubeCl.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Command:   getApi,
			Container: containerName,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, parameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("Creating SPDY executor for Pod %s: %v", podName, err)
	}
	return executor, nil
}
