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
)

func Status(dynamicClient dynamic.Interface) statusresult.StatusResult {
    modules, err := getModules(dynamicClient)
    var output string
    if err != nil {
        output = color.RedString("Error getting modules: %v", err)
    } else {
        output = formatModules(modules)
    }
    return statusresult.StatusResult{
        Title:  "Modules",
        Level:  0,
        Output: output,
    }
}



func getModules(dynamicCl dynamic.Interface) ([]map[string]string, error) {
    gvr := schema.GroupVersionResource{
        Group:    "deckhouse.io",
        Version:  "v1alpha1",
        Resource: "modules",
    }

    moduleList, err := dynamicCl.Resource(gvr).Namespace("").List(context.TODO(), metav1.ListOptions{})
    if err != nil {
        return nil, fmt.Errorf("failed to list modules: %w", err)
    }

    var modules []map[string]string
    for _, item := range moduleList.Items {
        if !strings.Contains(item.GetName(), "cni") {
            continue
        }

        properties, ok := item.Object["properties"].(map[string]interface{})
        if !ok {
            continue
        }

        moduleStatus, ok := item.Object["status"].(map[string]interface{})
        if !ok {
            continue
        }

        conditions, ok := moduleStatus["conditions"].([]interface{})
        if !ok {
            continue
        }

        enabled := "False"
        ready := "False"
        for _, cond := range conditions {
            conditionMap, ok := cond.(map[string]interface{})
            if !ok {
                continue
            }
            conditionType := conditionMap["type"].(string)
            status := conditionMap["status"].(string)

            if conditionType == "EnabledByModuleConfig" || conditionType == "EnabledByModuleManager" {
                enabled = status
            }
            if conditionType == "IsReady" {
                ready = status
            }
        }

        module := map[string]string{
            "name":    item.GetName(),
            "weight":  fmt.Sprintf("%v", properties["weight"]),
            "source":  fmt.Sprintf("%v", properties["source"]),
            "phase":   fmt.Sprintf("%v", moduleStatus["phase"]),
            "enabled": enabled,
            "ready":   ready,
        }
        modules = append(modules, module)
    }

    return modules, nil
}

func formatModules(modules []map[string]string) string {
    if len(modules) == 0 {
        return color.YellowString("❗ No CNI modules found")
    }
    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()

    sb.WriteString(yellow("┌ CNI in cluster\n"))

    sb.WriteString(yellow(fmt.Sprintf("├ %-18s %-8s %-10s %-12s %-8s %-8s\n", "NAME", "WEIGHT", "SOURCE", "PHASE", "ENABLED", "READY")))

    for i, module := range modules {
        prefix := "├"
        if i == len(modules)-1 {
            prefix = "└"
        }

        sb.WriteString(fmt.Sprintf("%s %-18s %-8s %-10s %-12s %-8s %-8s\n",
            yellow(prefix),
            module["name"],
            module["weight"],
            module["source"],
            module["phase"],
            module["enabled"],
            module["ready"]))
    }
    return sb.String()
}