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
	out, err := fetchQueue(config, kubeCl, pathFromOption)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", out)

	return nil
}

func fetchQueue(config *rest.Config, kubeCl *kubernetes.Clientset, pathFromOption string) (string, error) {
	const (
		apiProtocol = "http"
		apiEndpoint = "127.0.0.1"
		apiPort     = "9652"
		queuePath   = "queue"

		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	fullEndpointURL := fmt.Sprintf("%s://%s:%s/%s/%s", apiProtocol, apiEndpoint, apiPort, queuePath, pathFromOption)
	getAPI := []string{"curl", fullEndpointURL}

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return "", err
	}

	executor, err := utilk8s.ExecInPod(config, kubeCl, getAPI, podName, namespace, containerName)
	if err != nil {
		return "", err
	}

	var (
		stdout bytes.Buffer
		stderr bytes.Buffer
	)

	if err := executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
		return "", err
	}

	return stdout.String(), nil
}

func watchQueueCommand(config *rest.Config, kubeCl *kubernetes.Clientset, pathFromOption string) error {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	output := termenv.DefaultOutput()
	output.AltScreen()
	output.HideCursor()

	defer func() {
		output.ShowCursor()
		output.ExitAltScreen()
	}()

	// Render frames into a single buffer and write them in one syscall so the
	// terminal never has a chance to display a half-cleared screen. The
	// "cursor home, write, clear to end of screen" pattern overwrites the
	// previous frame in place instead of wiping it first, which is what was
	// causing the visible blinking.
	render := func() {
		body, fetchErr := fetchQueue(config, kubeCl, pathFromOption)

		var content bytes.Buffer
		fmt.Fprintf(&content, "Watching queue - %s (press Ctrl+C to stop)\n\n", time.Now().Format("15:04:05"))

		if fetchErr != nil {
			fmt.Fprintf(&content, "Error fetching queue: %v\n", fetchErr)
		} else {
			content.WriteString(body)

			if content.Len() == 0 || content.Bytes()[content.Len()-1] != '\n' {
				content.WriteByte('\n')
			}
		}

		var frame bytes.Buffer
		// Move cursor to the top-left corner.
		frame.WriteString("\x1b[H")
		// Overwrite the previous frame in place. Because a new line may be
		// shorter than the line it replaces, erase to the end of each line
		// (\x1b[K) before the newline; otherwise the tail of the previous,
		// longer line stays on screen and produces garbled, overlapping text.
		lines := bytes.Split(content.Bytes(), []byte("\n"))
		for i, line := range lines {
			frame.Write(line)
			frame.WriteString("\x1b[K")

			if i < len(lines)-1 {
				frame.WriteByte('\n')
			}
		}
		// Erase everything from the cursor to the end of the screen so that
		// leftover lines from a longer previous frame are cleaned up.
		frame.WriteString("\x1b[J")

		_, _ = os.Stdout.Write(frame.Bytes())
	}

	render()

	for {
		select {
		case <-signals:
			return nil
		case <-ticker.C:
			render()
		}
	}
}
