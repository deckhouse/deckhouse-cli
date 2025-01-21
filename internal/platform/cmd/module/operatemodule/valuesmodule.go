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
	"k8s.io/client-go/tools/remotecommand"
	"log"
	"os"
)

func ValuesModule(cmd *cobra.Command, name string) error {

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
		valuesPath    = "values.yaml"
		labelSelector = "leader=true"
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	// Get list of pods based on label selector
	pods, err := kubeCl.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		fmt.Println("Error listing pods:", err)
		os.Exit(1)
	}

	// Check if any pods are found
	if len(pods.Items) == 0 {
		fmt.Println("No pods found with the label:", labelSelector)
		os.Exit(1)
	}

	// Use the first pod found
	pod := pods.Items[0]
	podName := pod.Name

	// Check if the container exists in the pod
	containerFound := false
	for _, c := range pod.Spec.Containers {
		if c.Name == containerName {
			containerFound = true
			break
		}
	}
	if !containerFound {
		fmt.Println("Container %q not found in pod %q", containerName, podName)
	}

	endpointUrl := fmt.Sprintf("%s://%s/%s/%s/%s/%s", apiProtocol, apiEndpoint, apiPort, modulePath, name, valuesPath)

	getApi := []string{"curl", endpointUrl}

	scheme := runtime.NewScheme()
	parameterCodec := runtime.NewParameterCodec(scheme)
	if err := v1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("Failed to create parameter codec: %w", err)
	}

	// Execute the command in the pod
	req := kubeCl.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
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

	// Set up a buffer to capture the output
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		log.Printf("Creating SPDY executor for Pod %s: %v", podName, err)
	}

	// Run the command
	if err = executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
		return err
	}

	// Print the results
	fmt.Printf("Command stdout: %s\n", stdout.String())

	return err
}
