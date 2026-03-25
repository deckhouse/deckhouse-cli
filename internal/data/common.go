package dataio

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/term"
)

const (
	DefaultTTL                = "2m"
	PersistentVolumeClaimKind = "PersistentVolumeClaim"
	VolumeSnapshotKind        = "VolumeSnapshot"
	VirtualDiskKind           = "VirtualDisk"
	VirtualDiskSnapshotKind   = "VirtualDiskSnapshot"
	Namespace                 = "d8-storage-volume-data-manager"
)

var (
	ErrUnsupportedVolumeMode = errors.New("invalid volume mode")
)

const (
	defaultOnNonTTY   = false
	defaultInputOnErr = "no"
)

// ShouldCleanup decides whether to delete an auto-created DataExport.
// When the --cleanup flag was explicitly set by the user, its value is used directly.
// Otherwise the decision is delegated to an interactive prompt with a timeout.
func ShouldCleanup(cleanup, cleanupExplicit bool) bool {
	if cleanupExplicit {
		return cleanup
	}
	return AskYesNoWithTimeout(
		"DataExport will auto-delete in 30 sec [press y+Enter to delete now, n+Enter to cancel]",
		time.Second*30,
	)
}

func AskYesNoWithTimeout(prompt string, timeout time.Duration) bool {
	// In non-interactive sessions (pipe/no TTY), do not prompt and use safe default.
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return defaultOnNonTTY
	}

	// Buffered channel avoids blocking send if timeout branch wins first.
	inputChan := make(chan string, 1)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("%s: ", prompt)
			input, err := reader.ReadString('\n')
			if err != nil {
				// Read errors (EOF/closed stdin/etc.) can repeat forever; fall back once and exit.
				fmt.Println("Error reading input, chosen default value: no.")
				inputChan <- defaultInputOnErr
				return
			}

			input = strings.ToLower(strings.TrimSpace(input))
			if slices.Contains([]string{"y", "n"}, input) {
				inputChan <- strings.TrimSpace(input)
				return
			}
			// Retry only for invalid user input.
			fmt.Println("Invalid input. Please press 'y' or 'n'.")
		}
	}()

	select {
	case input := <-inputChan:
		if input == "n" || input == "no" {
			return false
		}
		return true
	case <-time.After(timeout):
		fmt.Printf("\n")
		return true
	}
}

func ParseArgs(args []string) ( /*deName*/ string /*srcPath*/, string, error) {
	var deName, srcPath string

	switch len(args) {
	case 1:
		deName = args[0]
	case 2:
		deName = args[0]
		srcPath = args[1]
	default:
		return "", "", fmt.Errorf("invalid arguments")
	}

	if !strings.HasPrefix(srcPath, "/") {
		srcPath = "/" + srcPath
	}

	return deName, srcPath, nil
}
