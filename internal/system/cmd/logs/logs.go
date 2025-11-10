/*
Copyright 2024 Flant JSC

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

package logs

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/utils/ptr"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var listLong = templates.LongDesc(`
Inspect deckhouse-controller logs from Deckhouse Kubernetes Platform.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	logCmd := &cobra.Command{
		Use:           "logs",
		Short:         "Inspect deckhouse-controller logs.",
		Long:          listLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       ValidateParameters,
		RunE:          getLogDeckhouse,
	}
	AddFlags(logCmd.Flags())
	return logCmd
}

func getLogDeckhouse(cmd *cobra.Command, _ []string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	_, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return fmt.Errorf("Failed to get Deckhouse pod: %w", err)
	}
	logOptions := &v1.PodLogOptions{
		Container: "deckhouse",
		Follow:    Follow,
	}
	if Tail > 0 {
		logOptions.TailLines = &Tail
	}
	if Since != "" {
		duration, _ := time.ParseDuration(Since)
		logOptions.SinceSeconds = ptr.To(int64(duration.Seconds()))
	}
	if SinceTime != "" {
		t, _ := time.Parse(time.DateTime, SinceTime)
		logOptions.SinceTime = &metav1.Time{Time: t}
	}

	req := kubeCl.CoreV1().Pods("d8-system").GetLogs(podName, logOptions)
	stream, err := req.Stream(context.Background())
	if err != nil {
		return fmt.Errorf("error opening log stream: %v", err)
	}
	defer stream.Close()

	_, err = io.Copy(os.Stdout, stream)
	if err != nil {
		return fmt.Errorf(
			"error reading logs: %v", err)
	}
	return nil
}
