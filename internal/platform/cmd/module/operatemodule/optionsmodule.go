package operatemodule

import (
	"bytes"
	"context"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/operatepod"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
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

	fullEndpointUrl := fmt.Sprintf("%s://%s:%s/%s/%s", apiProtocol, apiEndpoint, apiPort, modulePath, pathFromOption)
	getApi := []string{"curl", fullEndpointUrl}
	podName, err := operatepod.GetDeckhousePod(kubeCl, namespace, labelSelector)
	executor, err := operatepod.ExecInPod(config, kubeCl, getApi, podName, namespace, containerName)

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
