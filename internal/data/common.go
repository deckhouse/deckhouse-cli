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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// KindToGroup resolves the API group for a supported DataExport target kind. The kind is
// sent verbatim as targetRef.kind; only the group needs deriving here (the controller
// resolves the served version via its RESTMapper). These groups match the producer's
// DataExportTargetRefSpec contract in storage-volume-data-manager/api/v1alpha1/data_export.go.
// Returns an error for unrecognised kinds.
func KindToGroup(kind string) (string, error) {
	switch kind {
	case PersistentVolumeClaimKind:
		return "", nil
	case VolumeSnapshotKind:
		return "snapshot.storage.k8s.io", nil
	case VirtualDiskKind:
		return "virtualization.deckhouse.io", nil
	case VirtualDiskSnapshotKind:
		return "virtualization.deckhouse.io", nil
	default:
		return "", fmt.Errorf("unsupported DataExport target kind %q", kind)
	}
}

// Condition types and reasons shared by DataExport and DataImport.
const (
	// ConditionTypeReady is the readiness condition both producers set.
	ConditionTypeReady = "Ready"

	// ConditionTypeExpired is the standalone expiry condition storage-volume-data-manager sets.
	// storage-foundation dropped it in favour of ReasonExpired on Ready.
	ConditionTypeExpired = "Expired"

	// ReasonExpired is the Ready-condition reason both producers use for idle expiry.
	ReasonExpired = "Expired"
)

// IsExpired reports whether the conditions say the DataExport or DataImport has terminally
// idle-expired, so the caller must recreate it rather than keep polling. After expiry the
// producer's garbage collector only removes the object once its retention TTL runs out, so
// waiting it out would stall for as long as that retention lasts.
//
// Both spellings of expiry are accepted, because the producers do not agree on one and a client
// that reads only its own producer's spelling silently waits forever against the other:
//
//   - storage-volume-data-manager raises a standalone Expired condition (and also reports it as a
//     Ready reason);
//   - storage-foundation has no Expired condition type at all and reports it only as
//     Ready=False with reason Expired.
//
// The two cannot be confused for one another: neither producer uses either spelling to mean
// anything but expiry.
func IsExpired(conditions []metav1.Condition) bool {
	if expired := meta.FindStatusCondition(conditions, ConditionTypeExpired); expired != nil &&
		expired.Status == metav1.ConditionTrue {
		return true
	}

	ready := meta.FindStatusCondition(conditions, ConditionTypeReady)

	return ready != nil && ready.Status == metav1.ConditionFalse && ready.Reason == ReasonExpired
}

// NotReady returns the Ready condition when it is present and not True, and nil otherwise —
// including when the object carries no Ready condition at all, which callers treat as "nothing
// said yet" rather than as a failure.
func NotReady(conditions []metav1.Condition) *metav1.Condition {
	ready := meta.FindStatusCondition(conditions, ConditionTypeReady)
	if ready == nil || ready.Status == metav1.ConditionTrue {
		return nil
	}

	return ready
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
