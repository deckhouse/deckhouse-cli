package dataio

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

const (
	DefaultTTL                = "2m"
	PersistentVolumeClaimKind = "PersistentVolumeClaim"
	VolumeSnapshotKind        = "VolumeSnapshot"
	VirtualDiskKind           = "VirtualDisk"
	VirtualDiskSnapshotKind   = "VirtualDiskSnapshot"
)

var (
	ErrUnsupportedVolumeMode = errors.New("invalid volume mode")
)

func AskYesNoWithTimeout(prompt string, timeout time.Duration) bool {
	inputChan := make(chan string)
	defer close(inputChan)

	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("%s: ", prompt)
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading input, please try again.")
				continue
			}

			input = strings.ToLower(strings.TrimSpace(input))
			if slices.Contains([]string{"y", "n"}, input) {
				inputChan <- strings.TrimSpace(input)
				return
			}
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

func ParseArgs(args []string) (deName, srcPath string, err error) {
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

	return
}
