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

package deckhousereleases

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
	"github.com/deckhouse/deckhouse-cli/internal/status/tools/timeutil"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, dynamicClient dynamic.Interface) statusresult.StatusResult {
	releases, err := getDeckhouseReleases(ctx, dynamicClient)
	output := color.RedString("Error getting Deckhouse releases: %v\n", err)
	if err == nil {
		output = formatDeckhouseReleases(releases)
	}
	return statusresult.StatusResult{
		Title:  "Deckhouse Releases",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type DeckhouseRelease struct {
	Name           string
	Phase          string
	TransitionTime string
	Message        string
}

func getDeckhouseReleases(ctx context.Context, dynamicCl dynamic.Interface) ([]DeckhouseRelease, error) {
	gvr := schema.GroupVersionResource{
		Group:    "deckhouse.io",
		Version:  "v1alpha1",
		Resource: "deckhousereleases",
	}

	releaseList, err := dynamicCl.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list deckhouse releases: %w", err)
	}

	releases := make([]DeckhouseRelease, 0, len(releaseList.Items))
	for _, item := range releaseList.Items {
		release, ok := deckhouseReleaseProcessing(item.Object, item.GetName())
		if !ok {
			continue
		}
		releases = append(releases, release)
	}

	return releases, nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func deckhouseReleaseProcessing(item map[string]interface{}, name string) (DeckhouseRelease, bool) {
	statusMap, ok := item["status"].(map[string]interface{})
	if !ok {
		return DeckhouseRelease{}, false
	}
	phase := statusMap["phase"].(string)
	transitionTime := statusMap["transitionTime"].(string)
	message := statusMap["message"].(string)

	return DeckhouseRelease{
		Name:           name,
		Phase:          phase,
		TransitionTime: transitionTime,
		Message:        message,
	}, true
}

// formatDeckhouseReleases returns a readable view of resource status for CLI display.
func formatDeckhouseReleases(releases []DeckhouseRelease) string {
	if len(releases) == 0 {
		return color.YellowString("❗ No Deckhouse releases found\n")
	}

	var sb strings.Builder
	yellow := color.New(color.FgYellow).SprintFunc()

	sb.WriteString(yellow("┌ Deckhouse Releases:\n"))
	sb.WriteString(yellow(fmt.Sprintf("%-15s %-15s %-21s %s\n", "├ NAME", "PHASE", "TRANSITIONTIME", "MESSAGE")))

	for i, release := range releases {
		prefix := "├"
		if i == len(releases)-1 {
			prefix = "└"
		}

		timeAgo := timeutil.AgeAgoStr(release.TransitionTime)
		message := release.Message

		sb.WriteString(fmt.Sprintf(
			"%s %-13s %-15s %-21s %s\n",
			yellow(prefix),
			release.Name,
			release.Phase,
			timeAgo,
			message,
		))
	}
	return sb.String()
}
