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

package adapters

import (
	"context"
	"fmt"
	"io"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/system/domain"
	"github.com/deckhouse/deckhouse-cli/internal/system/usecase"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// Compile-time checks
var (
	_ usecase.ModuleService = (*ModuleServiceAdapter)(nil)
	_ usecase.LogService    = (*LogServiceAdapter)(nil)
)

// ModuleServiceAdapter adapts K8s clients to usecase.ModuleService
type ModuleServiceAdapter struct {
	restConfig    *rest.Config
	kubeCl        kubernetes.Interface
	dynamicClient dynamic.Interface
}

// NewModuleServiceAdapter creates a new ModuleServiceAdapter
func NewModuleServiceAdapter(restConfig *rest.Config, kubeCl kubernetes.Interface) (*ModuleServiceAdapter, error) {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}
	return &ModuleServiceAdapter{
		restConfig:    restConfig,
		kubeCl:        kubeCl,
		dynamicClient: dynamicClient,
	}, nil
}

func (a *ModuleServiceAdapter) List(ctx context.Context) ([]domain.Module, error) {
	// This delegates to the existing operatemodule logic
	// The actual list is printed to stdout by operatemodule.OptionsModule
	err := operatemodule.OptionsModule(a.restConfig, a.kubeCl, "list.yaml")
	if err != nil {
		return nil, fmt.Errorf("list modules: %w", err)
	}
	// Note: The existing implementation prints directly, not returning modules
	return nil, nil
}

func (a *ModuleServiceAdapter) Enable(ctx context.Context, name string) error {
	return operatemodule.OperateModule(a.dynamicClient, name, operatemodule.ModuleEnabled)
}

func (a *ModuleServiceAdapter) Disable(ctx context.Context, name string) error {
	return operatemodule.OperateModule(a.dynamicClient, name, operatemodule.ModuleDisabled)
}

func (a *ModuleServiceAdapter) GetValues(ctx context.Context, name string) (*domain.ModuleValues, error) {
	// Delegates to operatemodule which prints to stdout
	err := operatemodule.OptionsModule(a.restConfig, a.kubeCl, "values.yaml")
	if err != nil {
		return nil, fmt.Errorf("get values: %w", err)
	}
	return nil, nil
}

func (a *ModuleServiceAdapter) GetSnapshots(ctx context.Context, name string) (*domain.ModuleSnapshot, error) {
	// Delegates to operatemodule which prints to stdout
	err := operatemodule.OptionsModule(a.restConfig, a.kubeCl, "snapshots.yaml")
	if err != nil {
		return nil, fmt.Errorf("get snapshots: %w", err)
	}
	return nil, nil
}

// LogServiceAdapter adapts K8s clients to usecase.LogService
type LogServiceAdapter struct {
	restConfig *rest.Config
	kubeCl     kubernetes.Interface
}

// NewLogServiceAdapter creates a new LogServiceAdapter
func NewLogServiceAdapter(restConfig *rest.Config, kubeCl kubernetes.Interface) *LogServiceAdapter {
	return &LogServiceAdapter{
		restConfig: restConfig,
		kubeCl:     kubeCl,
	}
}

func (a *LogServiceAdapter) StreamLogs(ctx context.Context, follow bool, output io.Writer) error {
	podName, err := utilk8s.GetDeckhousePod(a.kubeCl.(*kubernetes.Clientset))
	if err != nil {
		return fmt.Errorf("get deckhouse pod: %w", err)
	}

	command := []string{"cat", "/var/log/deckhouse/current.log"}
	if follow {
		command = []string{"tail", "-f", "/var/log/deckhouse/current.log"}
	}

	executor, err := utilk8s.ExecInPod(a.restConfig, a.kubeCl, command, podName, "d8-system", "deckhouse")
	if err != nil {
		return fmt.Errorf("exec in pod: %w", err)
	}

	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: output,
		Stderr: output,
	})
}

