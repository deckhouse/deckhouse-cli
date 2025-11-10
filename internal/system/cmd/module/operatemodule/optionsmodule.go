package operatemodule

import (
	"bytes"
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func OptionsModule(config *rest.Config, kubeCl kubernetes.Interface, pathFromOption string) error {
	const (
		apiProtocol = "http"
		apiEndpoint = "127.0.0.1"
		apiPort     = "9652"
		modulePath  = "module"

		labelSelector = "leader=true"
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	fullEndpointURL := fmt.Sprintf("%s://%s:%s/%s/%s", apiProtocol, apiEndpoint, apiPort, modulePath, pathFromOption)
	getAPI := []string{"curl", fullEndpointURL}
	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return err
	}
	executor, err := utilk8s.ExecInPod(config, kubeCl, getAPI, podName, namespace, containerName)
	if err != nil {
		return err
	}

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
