package debugtar

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// writeArchive streams a gzipped tar of the given commands' output to stdout.
// It is the shared body of the debug archives: only the command set and the
// progress banners differ between them.
func writeArchive(
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	commands []command,
	commandTimeout, requestInterval time.Duration,
	startBanner, doneBanner string,
) (err error) {
	gzipWriter := gzip.NewWriter(os.Stdout)
	tarWriter := tar.NewWriter(gzipWriter)

	defer func() {
		if closeErr := tarWriter.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to finalize tar archive: %w", closeErr)
		}

		if closeErr := gzipWriter.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to finalize gzip stream: %w", closeErr)
		}
	}()

	fmt.Fprintf(os.Stderr, "%s\n", startBanner)

	if err = runCommands(tarWriter, config, kubeCl, podName, namespace, containerName, commands, commandTimeout, requestInterval); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", doneBanner)

	return nil
}

// runCommands executes each command inside the Deckhouse pod and streams its
// output into the tar archive, honoring the optional rate limit between command
// executions. The command list is already filtered by the caller.
func runCommands(
	tarWriter *tar.Writer,
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	commands []command,
	commandTimeout, requestInterval time.Duration,
) error {
	var tickCh <-chan time.Time

	if requestInterval > 0 {
		ticker := time.NewTicker(requestInterval)
		defer ticker.Stop()

		tickCh = ticker.C
	}

	for _, cmd := range commands {
		if tickCh != nil {
			<-tickCh
		}

		fullCommand := append([]string{cmd.Cmd}, cmd.Args...)

		cmdCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		output, stderrOutput, streamErr := utilk8s.ExecCommandInPod(cmdCtx, config, kubeCl, fullCommand, podName, namespace, containerName)

		cancel()

		if streamErr != nil {
			// Report the error itself, the command that produced it and how much
			// output survived: the entry is written either way, so without the
			// byte count an operator cannot tell an empty file from a truncated
			// one, and without the error a non-zero exit code looks the same as a
			// broken stream.
			if errors.Is(streamErr, context.DeadlineExceeded) {
				fmt.Fprintf(os.Stderr, "  WARNING: timed out collecting %s after %s, keeping %d bytes collected so far\n",
					cmd.File, commandTimeout, len(output))
			} else {
				fmt.Fprintf(os.Stderr, "  ERROR: collecting %s: %v, keeping %d bytes\n    command: %s\n",
					cmd.File, streamErr, len(output), strings.Join(fullCommand, " "))
			}

			if trimmed := strings.TrimSpace(stderrOutput); trimmed != "" {
				fmt.Fprintf(os.Stderr, "    stderr: %s\n", trimmed)
			}
		}

		if notice := defaultedContainerNotice(stderrOutput); notice != "" {
			output = append([]byte(notice), output...)
		}

		if err := cmd.writeToTar(tarWriter, output); err != nil {
			return fmt.Errorf("failed to write tar file %s: %w", cmd.File, err)
		}
	}

	return nil
}

// defaultedContainerNotice extracts kubectl's client-side "Defaulted container
// ... out of: ..." notice(s) from a command's stderr, so they can be prepended
// to the collected log output.
func defaultedContainerNotice(stderrOutput string) string {
	var notice strings.Builder

	for _, line := range strings.Split(stderrOutput, "\n") {
		if strings.Contains(line, "Defaulted container") {
			notice.WriteString(line)
			notice.WriteString("\n")
		}
	}

	return notice.String()
}
