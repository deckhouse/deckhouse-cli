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

package deckhouseregistry

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, kubeCl kubernetes.Interface) statusresult.StatusResult {
	info, err := getDeckhouseRegistry(ctx, kubeCl)
	output := color.RedString("Error getting Deckhouse registry: %v\n", err)
	if err == nil {
		output = formatDeckhouseRegistry(info)
	}
	return statusresult.StatusResult{
		Title:  "Deckhouse Registry",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type deckhouseRegistry struct {
	Registry string
	Scheme   string
}

func getDeckhouseRegistry(ctx context.Context, kubeCl kubernetes.Interface) (deckhouseRegistry, error) {
	secret, err := kubeCl.CoreV1().Secrets("d8-system").Get(ctx, "deckhouse-registry", metav1.GetOptions{})
	if err != nil {
		return deckhouseRegistry{}, fmt.Errorf("failed to get secret: %w\n", err)
	}
	return deckhouseRegistryProcessing(secret)
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func deckhouseRegistryProcessing(secret *v1.Secret) (deckhouseRegistry, error) {
	var dr deckhouseRegistry

	if registryData, found := secret.Data["imagesRegistry"]; found {
		dr.Registry = string(registryData)
	}
	if schemeData, found := secret.Data["scheme"]; found {
		dr.Scheme = string(schemeData)
	}
	if dr.Registry == "" {
		return deckhouseRegistry{}, fmt.Errorf("'imagesRegistry' not found\n")
	}
	if dr.Scheme == "" {
		return deckhouseRegistry{}, fmt.Errorf("'scheme' not found\n")
	}

	return dr, nil
}

// Format returns a readable view of resource status for CLI display.
func formatDeckhouseRegistry(info deckhouseRegistry) string {
	yellow := color.New(color.FgYellow).SprintFunc()

	registry := "not found"
	if strings.TrimSpace(info.Registry) != "" {
		registry = info.Registry
	}
	scheme := "not found"
	if strings.TrimSpace(info.Scheme) != "" {
		scheme = info.Scheme
	}

	var sb strings.Builder
	sb.WriteString(yellow("┌ Deckhouse Registry Information:\n"))
	sb.WriteString(fmt.Sprintf("%s %s %s\n", yellow("├"), yellow("Registry:"), registry))
	sb.WriteString(fmt.Sprintf("%s %s %s\n", yellow("└"), yellow("Scheme:"), scheme))

	return sb.String()
}
