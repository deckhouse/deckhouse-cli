/*
Copyright 2025 Flant JSC

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

package cnimodules

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	constant "github.com/deckhouse/deckhouse-cli/internal/status/tools/constants"
	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, dynamicClient dynamic.Interface) statusresult.StatusResult {
	modules, err := getModules(ctx, dynamicClient)
	output := color.RedString("Error getting modules: %v\n", err)
	if err == nil {
		output = formatModules(modules)
	}
	return statusresult.StatusResult{
		Title:  "Modules",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type CNIModule struct {
	Name    string
	Weight  string
	Source  string
	Phase   string
	Enabled string
	Ready   string
}

func getModules(ctx context.Context, dynamicCl dynamic.Interface) ([]CNIModule, error) {
	gvr := schema.GroupVersionResource{
		Group:    "deckhouse.io",
		Version:  "v1alpha1",
		Resource: "modules",
	}
	moduleList, err := dynamicCl.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list modules: %w", err)
	}
	modules := make([]CNIModule, 0, len(moduleList.Items))
	for _, item := range moduleList.Items {
		if !strings.Contains(item.GetName(), "cni") {
			continue
		}
		module, ok := CNIModuleProcessing(item.Object)
		if !ok {
			continue
		}
		modules = append(modules, module)
	}
	return modules, nil
}

// CNIModuleProcessing converts raw resource data into a CNIModule struct for output and analysis.
func CNIModuleProcessing(item map[string]interface{}) (CNIModule, bool) {
	metadataRaw, ok := item["metadata"].(map[string]interface{})
	if !ok {
		return CNIModule{}, false
	}

	name, ok := metadataRaw["name"].(string)
	if !ok {
		return CNIModule{}, false
	}

	properties, ok := item["properties"].(map[string]interface{})
	if !ok {
		return CNIModule{}, false
	}

	moduleStatus, ok := item["status"].(map[string]interface{})
	if !ok {
		return CNIModule{}, false
	}

	conditionsRaw, ok := moduleStatus["conditions"].([]interface{})
	if !ok {
		return CNIModule{}, false
	}

	enabled := "False"
	ready := "False"
	for _, c := range conditionsRaw {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		conditionType, ok := cond["type"].(string)
		if !ok {
			continue
		}
		status, ok := cond["status"].(string)
		if !ok {
			continue
		}

		if conditionType == constant.ModuleConditionEnabledByModuleConfig ||
			conditionType == constant.ModuleConditionEnabledByModuleManager {
			enabled = status
		}
		if conditionType == constant.ModuleConditionIsReady {
			ready = status
		}
	}

	return CNIModule{
		Name:    name,
		Weight:  fmt.Sprintf("%v", properties["weight"]),
		Source:  fmt.Sprintf("%v", properties["source"]),
		Phase:   fmt.Sprintf("%v", moduleStatus["phase"]),
		Enabled: enabled,
		Ready:   ready,
	}, true
}

// Format returns a readable view of resource status for CLI display.
func formatModules(modules []CNIModule) string {
	if len(modules) == 0 {
		return color.YellowString("❗ No CNI modules found\n")
	}
	var sb strings.Builder
	yellow := color.New(color.FgYellow).SprintFunc()
	sb.WriteString(yellow("┌ CNI in cluster:\n"))
	sb.WriteString(yellow(fmt.Sprintf("├ %-18s %-8s %-10s %-12s %-8s %-8s\n", "NAME", "WEIGHT", "SOURCE", "PHASE", "ENABLED", "READY")))
	for i, module := range modules {
		prefix := "├"
		if i == len(modules)-1 {
			prefix = "└"
		}
		name := module.Name
		weight := module.Weight
		source := module.Source
		phase := module.Phase
		enabled := module.Enabled
		ready := module.Ready
		fmt.Fprintf(&sb, "%s %-18s %-8s %-10s %-12s %-8s %-8s\n",
			yellow(prefix),
			name,
			weight,
			source,
			phase,
			enabled,
			ready)
	}
	return sb.String()
}
