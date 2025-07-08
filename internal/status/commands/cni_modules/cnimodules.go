package cnimodules

import (
    "context"
    "fmt"
    "strings"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/dynamic"
    "k8s.io/apimachinery/pkg/runtime/schema"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
    constant "github.com/deckhouse/deckhouse-cli/internal/status/constants"
)

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
        return nil, fmt.Errorf("failed to list modules: %w\n", err)
    }
    var modules []CNIModule
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

func CNIModuleProcessing(item map[string]interface{}) (CNIModule, bool) {
    name, _ := item["metadata"].(map[string]interface{})["name"].(string)
    properties, ok := item["properties"].(map[string]interface{})
    if !ok {
        return CNIModule{}, false
    }
    moduleStatus, ok := item["status"].(map[string]interface{})
    if !ok {
        return CNIModule{}, false
    }
    conditions, ok := moduleStatus["conditions"].([]interface{})
    if !ok {
        return CNIModule{}, false
    }
    enabled := "False"
    ready := "False"
    for _, cond := range conditions {
        conditionMap, ok := cond.(map[string]interface{})
        if !ok {
            continue
        }
        conditionType, _ := conditionMap["type"].(string)
        status, _ := conditionMap["status"].(string)
        if conditionType == constant.ModuleConditionEnabledByModuleConfig || conditionType == constant.ModuleConditionEnabledByModuleManager {
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
        sb.WriteString(fmt.Sprintf("%s %-18s %-8s %-10s %-12s %-8s %-8s\n",
            yellow(prefix),
            name,
            weight,
            source,
            phase,
            enabled,
            ready,
        ))
    }
    return sb.String()
}