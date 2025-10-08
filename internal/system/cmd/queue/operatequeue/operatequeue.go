package operatequeue

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/muesli/termenv"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func OperateQueue(config *rest.Config, kubeCl *kubernetes.Clientset, pathFromOption string, watch bool) error {
	if !watch {
		return executeQueueCommand(config, kubeCl, pathFromOption)
	}

	return watchQueueCommand(config, kubeCl, pathFromOption)
}

func executeQueueCommand(config *rest.Config, kubeCl *kubernetes.Clientset, pathFromOption string) error {
	const (
		apiProtocol = "http"
		apiEndpoint = "127.0.0.1"
		apiPort     = "9652"
		queuePath   = "queue"

		labelSelector = "leader=true"
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	fullEndpointUrl := fmt.Sprintf("%s://%s:%s/%s/%s", apiProtocol, apiEndpoint, apiPort, queuePath, pathFromOption)
	getApi := []string{"curl", fullEndpointUrl}
	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return err
	}
	executor, err := utilk8s.ExecInPod(config, kubeCl, getApi, podName, namespace, containerName)
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
	return nil
}

func watchQueueCommand(config *rest.Config, kubeCl *kubernetes.Clientset, pathFromOption string) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	output := termenv.DefaultOutput()

	fmt.Println("Watching queue (press Ctrl+C to stop)...")

	for {
		select {
		case <-signals:
			fmt.Println("\nWatch stopped.")
			return nil
		case <-ticker.C:
			output.ClearScreen()
			output.MoveCursor(1, 1)
			fmt.Printf("Watching queue - %s\n\n", time.Now().Format("15:04:05"))

			err := executeQueueCommand(config, kubeCl, pathFromOption)
			if err != nil {
				fmt.Printf("Error fetching queue: %v\n", err)
			}
		}
	}
}
