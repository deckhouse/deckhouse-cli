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

package deckhousesettings

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, dynamicClient dynamic.Interface) statusresult.StatusResult {
	settings, err := getModuleConfigSettings(ctx, dynamicClient)
	output := color.RedString("Error getting ModuleConfig settings: %v\n", err)
	if err == nil {
		output = formatModuleConfigSettings(settings)
	}
	return statusresult.StatusResult{
		Title:  "Deckhouse ModuleConfig",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type ConfigSetting struct {
	Key      string
	Value    string
	Children []ConfigSetting
	Items    []ConfigSetting
}

func getModuleConfigSettings(ctx context.Context, dynamicClient dynamic.Interface) ([]ConfigSetting, error) {
	gvr := schema.GroupVersionResource{
		Group:    "deckhouse.io",
		Version:  "v1alpha1",
		Resource: "moduleconfigs",
	}

	mc, err := dynamicClient.Resource(gvr).Get(ctx, "deckhouse", metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get ModuleConfig: %w", err)
	}

	rawSettings, found, err := unstructured.NestedMap(mc.Object, "spec", "settings")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to find or parse settings in ModuleConfig: %w", err)
	}

	return configSettingsFromMapProcessing(rawSettings), nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func configSettingsFromMapProcessing(settings map[string]interface{}) []ConfigSetting {
	if len(settings) == 0 {
		return nil
	}

	result := make([]ConfigSetting, 0, len(settings))
	for key, value := range settings {
		result = append(result, configSettingsProcessing(key, value))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result
}

func configSettingsProcessing(key string, data interface{}) ConfigSetting {
	if key == "" {
		return ConfigSetting{}
	}

	// map[string]interface{}
	if m, ok := data.(map[string]interface{}); ok {
		children := make([]ConfigSetting, 0, len(m))
		for k, v := range m {
			children = append(children, configSettingsProcessing(k, v))
		}
		sort.Slice(children, func(i, j int) bool {
			return children[i].Key < children[j].Key
		})
		return ConfigSetting{
			Key:      key,
			Children: children,
		}
	}

	// []interface{}
	if arr, ok := data.([]interface{}); ok {
		items := make([]ConfigSetting, 0, len(arr))
		for i, elem := range arr {
			itemKey := fmt.Sprintf("#%d", i)
			items = append(items, configSettingsProcessing(itemKey, elem))
		}
		return ConfigSetting{
			Key:   key,
			Items: items,
		}
	}

	// default
	return ConfigSetting{
		Key:   key,
		Value: fmt.Sprintf("%v", data),
	}
}

// Format returns a readable view of resource status for CLI display.
func formatModuleConfigSettings(settings []ConfigSetting) string {
	if len(settings) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(color.New(color.FgYellow).Sprint("┌ ModuleConfig Deckhouse Settings:\n"))

	for i, setting := range settings {
		isLast := i == len(settings)-1
		sb.WriteString(formatSettingLine(setting, "", nil, isLast, 0))
	}
	return sb.String()
}

// formatSettingLine produces a single ConfigSetting (and its nested elements)
func formatSettingLine(setting ConfigSetting, indent string, prefixStack []bool, isLast bool, level int) string {
	var sb strings.Builder

	prefix := buildPrefix(prefixStack)

	lineOperator := "├"
	if isLast {
		lineOperator = "└"
	}
	coloredLineOperator := lineOperator
	if level == 0 {
		coloredLineOperator = color.New(color.FgYellow).Sprint(lineOperator)
	}

	// No "children", no arrays: just return the string.
	if len(setting.Children) == 0 && len(setting.Items) == 0 {
		sb.WriteString(fmt.Sprintf("%s%s %s: %s\n", prefix, coloredLineOperator, setting.Key, setting.Value))
		return sb.String()
	}

	// Map/Struct (has "children").
	if len(setting.Children) > 0 {
		sb.WriteString(fmt.Sprintf("%s%s %s:\n", prefix, coloredLineOperator, setting.Key))
		prefixStack = append(prefixStack, !isLast)
		for i, child := range setting.Children {
			sb.WriteString(formatSettingLine(child, indent+"    ", prefixStack, i == len(setting.Children)-1, level+1))
		}
		return sb.String()
	}

	// Array: separate scalar elements and objects.
	if len(setting.Items) > 0 {
		allScalars := areAllItemsScalars(setting.Items)
		sb.WriteString(fmt.Sprintf("%s%s %s:\n", prefix, coloredLineOperator, setting.Key))
		prefixStack = append(prefixStack, !isLast)
		if allScalars {
			for i, item := range setting.Items {
				sb.WriteString(fmt.Sprintf("%s│   %s %s\n", prefix, mapLineOp(i == len(setting.Items)-1), item.Value))
			}
			return sb.String()
		}
		indentForArray := indent + "    "
		for i, item := range setting.Items {
			itemIsLast := i == len(setting.Items)-1
			for j, child := range item.Children {
				sb.WriteString(formatSettingLine(child, indentForArray, prefixStack, j == len(item.Children)-1, level+2))
			}
			if len(item.Children) == 0 && item.Value != "" {
				sb.WriteString(fmt.Sprintf("%s│   %s %s\n", prefix, mapLineOp(itemIsLast), item.Value))
			}
		}
		return sb.String()
	}

	return ""
}

// buildPrefix builds prefix indent.
func buildPrefix(prefixStack []bool) string {
	if len(prefixStack) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, needPipe := range prefixStack {
		if needPipe {
			sb.WriteString("│   ")
		} else {
			sb.WriteString("    ")
		}
	}
	return sb.String()
}

// areAllItemsScalars specifies that all elements of the array are scalars.
func areAllItemsScalars(items []ConfigSetting) bool {
	for _, item := range items {
		if len(item.Children) > 0 || len(item.Items) > 0 {
			return false
		}
	}
	return true
}

// mapLineOp returns the desired operator for a list.
func mapLineOp(isLast bool) string {
	if isLast {
		return "└"
	}
	return "├"
}
