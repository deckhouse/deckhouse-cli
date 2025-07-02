package deckhousesettings

import (
    "context"
    "fmt"
    "strings"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/dynamic"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

func Status(dynamicClient dynamic.Interface) statusresult.StatusResult {
    settings, err := getModuleConfigSettings(dynamicClient)
    var output string
    if err != nil {
        output = color.RedString("Error getting ModuleConfig settings: %v", err)
    } else {
        output = formatModuleConfigSettings(settings)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse ModuleConfig",
        Level:  0,
        Output: output,
    }
}



func getModuleConfigSettings(dynamicClient dynamic.Interface) (map[string]interface{}, error) {
    gvr := schema.GroupVersionResource{
        Group:    "deckhouse.io",
        Version:  "v1alpha1",
        Resource: "moduleconfigs",
    }

    mc, err := dynamicClient.Resource(gvr).Namespace("").Get(context.TODO(), "deckhouse", metav1.GetOptions{})
    if err != nil {
        return nil, fmt.Errorf("failed to get ModuleConfig: %w", err)
    }

    rawSettings, found, err := unstructured.NestedMap(mc.Object, "spec", "settings")
    if err != nil || !found {
        return nil, fmt.Errorf("failed to find or parse settings in ModuleConfig: %w", err)
    }

    return rawSettings, nil
}

func formatModuleConfigSettings(settings map[string]interface{}) string {
    yellow := color.New(color.FgYellow).SprintFunc()
    var sb strings.Builder
    sb.WriteString(yellow("┌ ModuleConfig Deckhouse Settings\n"))
    settingsLength := len(settings)
    currentIndex := 0
    for key, value := range settings {
        currentIndex++
        var lineOperator string
        if currentIndex == settingsLength {
            lineOperator = "└"
        } else {
            lineOperator = "├"
        }
        sb.WriteString(fmt.Sprintf("%s %s: %s\n", yellow(lineOperator), key, formatValue(value, "    ")))
    }
    return sb.String()
}

func formatValue(value interface{}, indent string) string {
    switch v := value.(type) {
    case map[string]interface{}:
        var sb strings.Builder
        finalIndex := len(v)
        currentIndex := 0
        for subKey, subValue := range v {
            currentIndex++
            var lineOperator string
            if currentIndex == finalIndex {
                lineOperator = "└"
            } else {
                lineOperator = "├"
            }
            sb.WriteString(fmt.Sprintf("\n%s%s %s: %s", indent, lineOperator, subKey, formatValue(subValue, indent+"    ")))
        }
        return sb.String()
    default:
        return fmt.Sprintf("%v", v)
    }
}