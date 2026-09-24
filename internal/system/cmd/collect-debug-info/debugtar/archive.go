package debugtar

import (
	"archive/tar"
	"bufio"
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

const (
	// stdoutBufferSize keeps the gzip stream from reaching os.Stdout one small
	// block at a time: without it every flush of the compressor is a syscall.
	stdoutBufferSize = 256 << 10

	// collectionErrorsFile is the archive entry that lists the commands which
	// failed or timed out. The warnings printed during collection go to stderr,
	// which is not part of the archive and is gone by the time anyone opens it,
	// so without this entry a truncated file is indistinguishable from a
	// complete one.
	collectionErrorsFile = "collection-errors.txt"

	// reportStderrLimit caps how much of a command's stderr is quoted in
	// collectionErrorsFile: a failing log collection can produce a lot of it,
	// and the report is meant to be read, not to hold the output again.
	reportStderrLimit = 2 << 10
)

// commandFailure records one command that did not complete cleanly, so the
// archive can describe its own gaps.
type commandFailure struct {
	file     string
	command  string
	err      error
	timedOut bool
	timeout  time.Duration
	kept     int
	stderr   string
}

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
	if err = validateCommands(commands); err != nil {
		return err
	}

	bufferedStdout := bufio.NewWriterSize(os.Stdout, stdoutBufferSize)
	gzipWriter := gzip.NewWriter(bufferedStdout)
	tarWriter := tar.NewWriter(gzipWriter)

	defer func() {
		if closeErr := tarWriter.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to finalize tar archive: %w", closeErr)
		}

		if closeErr := gzipWriter.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to finalize gzip stream: %w", closeErr)
		}

		if flushErr := bufferedStdout.Flush(); flushErr != nil && err == nil {
			err = fmt.Errorf("failed to flush the archive to stdout: %w", flushErr)
		}
	}()

	fmt.Fprintf(os.Stderr, "%s\n", startBanner)

	failures, err := runCommands(tarWriter, config, kubeCl, podName, namespace, containerName, commands, commandTimeout, requestInterval)
	if err != nil {
		return err
	}

	if err = writeCollectionErrors(tarWriter, failures); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", doneBanner)

	if len(failures) > 0 {
		fmt.Fprintf(os.Stderr, "%d command(s) failed or timed out, see %s inside the archive\n",
			len(failures), collectionErrorsFile)
	}

	return nil
}

// validateCommands rejects a command list that cannot produce a well-formed
// archive, before a single command runs: a duplicate entry name means only the
// last copy survives extraction, and an unresolved {module-name} placeholder
// means the entry is stored under a template name nobody can use. Both are
// mistakes in the command tables rather than cluster conditions, and both are
// far cheaper to report up front than after several minutes of collection.
//
// It sits here because runCommands is the only writer of archive entries, so
// this covers the cluster-wide table and the virtualization one alike -- the
// latter never passes through filterAndExpandCommands.
func validateCommands(commands []command) error {
	seen := make(map[string]bool, len(commands))

	for _, cmd := range commands {
		if strings.Contains(cmd.File, "{module-name}") {
			return fmt.Errorf("unresolved {module-name} placeholder in archive entry %q", cmd.File)
		}

		if cmd.File == collectionErrorsFile {
			return fmt.Errorf("archive entry %q is reserved for the collection error report", cmd.File)
		}

		if seen[cmd.File] {
			return fmt.Errorf("duplicate archive entry %q: only the last copy would survive extraction", cmd.File)
		}

		seen[cmd.File] = true
	}

	return nil
}

// runCommands executes each command inside the Deckhouse pod and streams its
// output into the tar archive, honoring the optional rate limit between command
// executions. The command list is already filtered by the caller. It returns
// the commands that did not complete cleanly; a returned error means the
// archive itself could not be written and collection cannot continue.
func runCommands(
	tarWriter *tar.Writer,
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	commands []command,
	commandTimeout, requestInterval time.Duration,
) ([]commandFailure, error) {
	var (
		tickCh   <-chan time.Time
		failures []commandFailure
	)

	if requestInterval > 0 {
		ticker := time.NewTicker(requestInterval)
		defer ticker.Stop()

		tickCh = ticker.C
	}

	for i, cmd := range commands {
		if tickCh != nil && i > 0 {
			<-tickCh
		}

		fullCommand := append([]string{cmd.Cmd}, cmd.Args...)

		cmdCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		output, stderrOutput, streamErr := utilk8s.ExecCommandInPod(cmdCtx, config, kubeCl, fullCommand, podName, namespace, containerName)

		cancel()

		if streamErr != nil {
			timedOut := errors.Is(streamErr, context.DeadlineExceeded)

			if timedOut {
				fmt.Fprintf(os.Stderr, "  WARNING: timed out collecting %s after %s, keeping %d bytes collected so far\n",
					cmd.File, commandTimeout, len(output))
			} else {
				fmt.Fprintf(os.Stderr, "  ERROR: collecting %s: %v, keeping %d bytes\n    command: %s\n",
					cmd.File, streamErr, len(output), strings.Join(fullCommand, " "))
			}

			if trimmed := strings.TrimSpace(stderrOutput); trimmed != "" {
				fmt.Fprintf(os.Stderr, "    stderr: %s\n", trimmed)
			}

			failures = append(failures, commandFailure{
				file:     cmd.File,
				command:  strings.Join(fullCommand, " "),
				err:      streamErr,
				timedOut: timedOut,
				timeout:  commandTimeout,
				kept:     len(output),
				stderr:   stderrOutput,
			})
		}

		// The notice is passed as a separate chunk rather than prepended to
		// output: prepending would copy the whole collected output to add a
		// couple of lines in front of it.
		if err := cmd.writeToTar(tarWriter, []byte(defaultedContainerNotice(stderrOutput)), output); err != nil {
			return nil, fmt.Errorf("failed to write tar file %s: %w", cmd.File, err)
		}
	}

	return failures, nil
}

// writeCollectionErrors stores the list of failed commands as the last archive
// entry, so the archive states its own gaps to whoever opens it.
func writeCollectionErrors(tarWriter *tar.Writer, failures []commandFailure) error {
	if len(failures) == 0 {
		return nil
	}

	entry := command{File: collectionErrorsFile}

	if err := entry.writeToTar(tarWriter, formatCollectionErrors(failures)); err != nil {
		return fmt.Errorf("failed to write tar file %s: %w", collectionErrorsFile, err)
	}

	return nil
}

// formatCollectionErrors renders the failure list as the body of
// collectionErrorsFile.
func formatCollectionErrors(failures []commandFailure) []byte {
	var report strings.Builder

	report.WriteString("Commands that did not complete during this collection.\n")
	report.WriteString("The archive entries they produced are empty or truncated.\n\n")

	for _, failure := range failures {
		if failure.timedOut {
			fmt.Fprintf(&report, "%s: TIMED OUT after %s, %d bytes kept\n", failure.file, failure.timeout, failure.kept)
		} else {
			fmt.Fprintf(&report, "%s: %v, %d bytes kept\n", failure.file, failure.err, failure.kept)
		}

		fmt.Fprintf(&report, "  command: %s\n", failure.command)

		if trimmed := strings.TrimSpace(failure.stderr); trimmed != "" {
			fmt.Fprintf(&report, "  stderr: %s\n", truncateForReport(trimmed))
		}

		report.WriteString("\n")
	}

	return []byte(report.String())
}

// truncateForReport keeps one quoted stderr from taking over the report.
func truncateForReport(s string) string {
	if len(s) <= reportStderrLimit {
		return s
	}

	return s[:reportStderrLimit] + "... (truncated)"
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
