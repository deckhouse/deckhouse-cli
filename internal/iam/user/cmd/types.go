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
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
)

var userOperationGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1",
	Resource: "useroperations",
}

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
	return dyn.Resource(userOperationGVR).Create(ctx, obj, metav1.CreateOptions{})
}

func waitUserOperation(ctx context.Context, dyn dynamic.Interface, name string, timeout time.Duration) (*unstructured.Unstructured, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		obj, err := dyn.Resource(userOperationGVR).Get(ctx, name, metav1.GetOptions{})
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
	obj, err := dyn.Resource(userOperationGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}
