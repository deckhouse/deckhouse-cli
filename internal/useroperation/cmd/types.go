package useroperation

import (
	"context"
	"fmt"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
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

type waitFlags struct {
	wait    bool
	timeout time.Duration
}

func addWaitFlags(cmd *cobra.Command, defaults waitFlags) {
	cmd.Flags().Bool("wait", defaults.wait, "Wait for UserOperation completion and print result.")
	cmd.Flags().Duration("timeout", defaults.timeout, "How long to wait for completion when --wait is enabled.")
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

func getStringFlag(cmd *cobra.Command, name string) (string, error) {
	if cmd.Flags().Lookup(name) != nil {
		return cmd.Flags().GetString(name)
	}
	if cmd.InheritedFlags().Lookup(name) != nil {
		return cmd.InheritedFlags().GetString(name)
	}
	return "", fmt.Errorf("flag %q not found", name)
}

func newDynamicClient(cmd *cobra.Command) (dynamic.Interface, error) {
	kubeconfigPath, err := getStringFlag(cmd, "kubeconfig")
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}
	contextName, err := getStringFlag(cmd, "context")
	if err != nil {
		return nil, fmt.Errorf("failed to get context: %w", err)
	}
	restConfig, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	return dyn, nil
}

func createUserOperation(ctx context.Context, dyn dynamic.Interface, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return dyn.Resource(userOperationGVR).Create(ctx, obj, metav1.CreateOptions{})
}

func waitUserOperation(ctx context.Context, dyn dynamic.Interface, name string, timeout time.Duration) (*unstructured.Unstructured, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var last *unstructured.Unstructured
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		obj, err := dyn.Resource(userOperationGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		last = obj
		phase, found, _ := unstructured.NestedString(obj.Object, "status", "phase")
		if !found || phase == "" {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return last, err
	}
	return last, nil
}
