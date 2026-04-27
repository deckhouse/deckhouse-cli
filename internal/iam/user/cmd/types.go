/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package user

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

const (
	defaultWait    = true
	defaultTimeout = 5 * time.Minute
)

type waitFlags struct {
	wait    bool
	timeout time.Duration
}

func addWaitFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("wait", defaultWait, "Wait for UserOperation completion and print result.")
	cmd.Flags().Duration("timeout", defaultTimeout, "How long to wait for completion when --wait is enabled.")
}

func getWaitFlags(cmd *cobra.Command) (waitFlags, error) {
	waitVal, err := cmd.Flags().GetBool("wait")
	if err != nil {
		return waitFlags{}, err
	}
	timeoutVal, err := cmd.Flags().GetDuration("timeout")
	if err != nil {
		return waitFlags{}, err
	}
	return waitFlags{wait: waitVal, timeout: timeoutVal}, nil
}

func createUserOperation(ctx context.Context, dyn dynamic.Interface, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return dyn.Resource(iamtypes.UserOperationGVR).Create(ctx, obj, metav1.CreateOptions{})
}

// userOpRequest describes a single UserOperation submitted via the CLI.
// It exists to deduplicate the create+wait+report flow shared by lock,
// unlock, reset-password and reset2fa.
type userOpRequest struct {
	// NamePrefix is the prefix for the generated UserOperation name, e.g. "op-lock-".
	NamePrefix string
	// OpType matches spec.type on the UserOperation CR (e.g. "Lock", "Unlock").
	OpType string
	// User is the target username (spec.user).
	User string
	// ExtraSpec holds operation-specific spec fields, e.g. spec.lock or spec.resetPassword.
	ExtraSpec map[string]any
}

// runUserOperation creates a UserOperation, optionally waits for completion,
// and writes status to cmd. It centralises the "wait/no-wait + Failed/Succeeded"
// branching that used to be copy-pasted across the user subcommands.
func runUserOperation(cmd *cobra.Command, dyn dynamic.Interface, req userOpRequest) error {
	wf, err := getWaitFlags(cmd)
	if err != nil {
		return err
	}

	// Nano-second precision avoids collisions when several UserOperation
	// commands are issued within the same wall-clock second (CI, retries, etc.).
	name := fmt.Sprintf("%s%d", req.NamePrefix, time.Now().UnixNano())
	spec := map[string]any{
		"user":          req.User,
		"type":          req.OpType,
		"initiatorType": "admin",
	}
	for k, v := range req.ExtraSpec {
		spec[k] = v
	}

	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": iamtypes.APIVersionDeckhouseV1,
			"kind":       iamtypes.KindUserOperation,
			"metadata":   map[string]any{"name": name},
			"spec":       spec,
		},
	}

	if _, err := createUserOperation(cmd.Context(), dyn, obj); err != nil {
		return fmt.Errorf("create UserOperation: %w", err)
	}

	if !wf.wait {
		cmd.Printf("%s\n", name)
		return nil
	}

	result, err := waitUserOperation(cmd.Context(), dyn, name, wf.timeout)
	if err != nil {
		return fmt.Errorf("wait UserOperation: %w", err)
	}

	phase, _, _ := unstructured.NestedString(result.Object, "status", "phase")
	message, _, _ := unstructured.NestedString(result.Object, "status", "message")
	if phase == "Failed" {
		return fmt.Errorf("%s failed: %s", req.OpType, message)
	}
	cmd.Printf("Succeeded: %s\n", name)
	return nil
}

func waitUserOperation(ctx context.Context, dyn dynamic.Interface, name string, timeout time.Duration) (*unstructured.Unstructured, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		obj, err := dyn.Resource(iamtypes.UserOperationGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			// Hard error: if we can't read the resource, we can't reliably wait for completion.
			return false, err
		}

		phase, found, _ := unstructured.NestedString(obj.Object, "status", "phase")
		if !found || phase == "" {
			// Not completed yet: keep polling until the controller fills status.phase.
			return false, nil
		}

		// Completed (Succeeded/Failed): stop polling.
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Fetch the final object to return the latest status/message.
	obj, err := dyn.Resource(iamtypes.UserOperationGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}
