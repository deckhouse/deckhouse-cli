package deckhousesettings

import (
    "context"
    "fmt"
    "strings"
    "sort"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/dynamic"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
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
        return nil, fmt.Errorf("failed to get ModuleConfig: %w\n", err)
    }

    rawSettings, found, err := unstructured.NestedMap(mc.Object, "spec", "settings")
    if err != nil || !found {
        return nil, fmt.Errorf("failed to find or parse settings in ModuleConfig: %w\n", err)
    }

    return configSettingsFromMapProcessing(rawSettings), nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func configSettingsFromMapProcessing(settings map[string]interface{}) []ConfigSetting {
    result := make([]ConfigSetting, 0, len(settings))
    for key, value := range settings {
        result = append(result, configSettingsProcessing(key, value))
    }
    sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
    return result
}

func configSettingsProcessing(key string, data interface{}) ConfigSetting {
    switch v := data.(type) {
    case map[string]interface{}:
        children := make([]ConfigSetting, 0, len(v))
        for k, value := range v {
            child := configSettingsProcessing(k, value)
            children = append(children, child)
        }
        sort.Slice(children, func(i, j int) bool { return children[i].Key < children[j].Key })
        return ConfigSetting{
            Key:      key,
            Children: children,
        }
    case []interface{}:
        items := make([]ConfigSetting, 0, len(v))
        for i, item := range v {
            items = append(items, configSettingsProcessing(fmt.Sprintf("#%d", i), item))
        }
        return ConfigSetting{
            Key:   key,
            Items: items,
        }
    default:
        return ConfigSetting{
            Key:   key,
            Value: fmt.Sprintf("%v", v),
        }
    }
}

// Format returns a readable view of resource status for CLI display.
func formatModuleConfigSettings(settings []ConfigSetting) string {
    var sb strings.Builder
    sb.WriteString(color.New(color.FgYellow).Sprint("┌ ModuleConfig Deckhouse Settings:\n"))
    for i, setting := range settings {
        isLast := (i == len(settings)-1)
        sb.WriteString(formatSettingLine(setting, "", []bool{}, isLast, 0))
    }
    return sb.String()
}

func formatSettingLine(setting ConfigSetting, indent string, prefixStack []bool, isLast bool, level int) string {
    var sb strings.Builder

    var prefix string
    for _, needPipe := range prefixStack {
        if needPipe {
            prefix += "│   "
        } else {
            prefix += "    "
        }
    }

    lineOperator := "├"
    if isLast {
        lineOperator = "└"
    }
    coloredLineOperator := lineOperator
    if level == 0 {
        coloredLineOperator = color.New(color.FgYellow).Sprint(lineOperator)
    }

    if len(setting.Children) == 0 && len(setting.Items) == 0 {
        sb.WriteString(fmt.Sprintf("%s%s %s: %s\n", prefix, coloredLineOperator, setting.Key, setting.Value))
        return sb.String()
    }

    if len(setting.Children) > 0 {
        sb.WriteString(fmt.Sprintf("%s%s %s:\n", prefix, coloredLineOperator, setting.Key))
        newPrefixStack := append(prefixStack, !isLast)
        for i, child := range setting.Children {
            childIsLast := (i == len(setting.Children)-1)
            sb.WriteString(formatSettingLine(child, indent+"    ", newPrefixStack, childIsLast, level+1))
        }
        return sb.String()
    }

    if len(setting.Items) > 0 {
        allScalars := true
        for _, item := range setting.Items {
            if len(item.Children) > 0 || len(item.Items) > 0 {
                allScalars = false
                break
            }
        }
        sb.WriteString(fmt.Sprintf("%s%s %s:\n", prefix, coloredLineOperator, setting.Key))
        newPrefixStack := append(prefixStack, !isLast)
        indentForArray := indent + "    "
        for i, item := range setting.Items {
            itemIsLast := (i == len(setting.Items)-1)
            if allScalars {
                sb.WriteString(fmt.Sprintf("%s%s %s\n", prefix+"│   ", mapLineOp(itemIsLast), item.Value))
            } else {
                for j, child := range item.Children {
                    childIsLast := (j == len(item.Children)-1)
                    sb.WriteString(formatSettingLine(child, indentForArray, newPrefixStack, childIsLast, level+2))
                }
            }
        }
        return sb.String()
    }

    return sb.String()
}

func mapLineOp(isLast bool) string {
    if isLast {
        return "└"
    }
    return "├"
}