/*
Copyright 2026 Flant JSC

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

package scan

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestBuildPackageRepositoryOperation(t *testing.T) {
	tests := []struct {
		name           string
		operationName  string
		repositoryName string
		timeout        time.Duration
		wantSpec       map[string]interface{}
	}{
		{
			name:           "basic operation",
			operationName:  "test-scan-manual-1",
			repositoryName: "test",
			timeout:        5 * time.Minute,
			wantSpec: map[string]interface{}{
				"packageRepositoryName": "test",
				"type":                  "Update",
				"update": map[string]interface{}{
					"fullScan": true,
					"timeout":  "5m0s",
				},
			},
		},
		{
			name:           "custom timeout",
			operationName:  "my-repo-scan-manual-123",
			repositoryName: "my-repo",
			timeout:        10 * time.Minute,
			wantSpec: map[string]interface{}{
				"packageRepositoryName": "my-repo",
				"type":                  "Update",
				"update": map[string]interface{}{
					"fullScan": true,
					"timeout":  "10m0s",
				},
			},
		},
		{
			name:           "repository name with dashes",
			operationName:  "my-awesome-repo-scan-manual-456",
			repositoryName: "my-awesome-repo",
			timeout:        5 * time.Minute,
			wantSpec: map[string]interface{}{
				"packageRepositoryName": "my-awesome-repo",
				"type":                  "Update",
				"update": map[string]interface{}{
					"fullScan": true,
					"timeout":  "5m0s",
				},
			},
		},
		{
			name:           "repository name with dots",
			operationName:  "repo.example.com-scan-manual-789",
			repositoryName: "repo.example.com",
			timeout:        1 * time.Hour,
			wantSpec: map[string]interface{}{
				"packageRepositoryName": "repo.example.com",
				"type":                  "Update",
				"update": map[string]interface{}{
					"fullScan": true,
					"timeout":  "1h0m0s",
				},
			},
		},
		{
			name:           "short timeout",
			operationName:  "quick-scan",
			repositoryName: "test",
			timeout:        30 * time.Second,
			wantSpec: map[string]interface{}{
				"packageRepositoryName": "test",
				"type":                  "Update",
				"update": map[string]interface{}{
					"fullScan": true,
					"timeout":  "30s",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPackageRepositoryOperation(tt.operationName, tt.repositoryName, tt.timeout)

			assert.Equal(t, "deckhouse.io/v1alpha1", got.GetAPIVersion())
			assert.Equal(t, "PackageRepositoryOperation", got.GetKind())
			assert.Equal(t, tt.operationName, got.GetName())

			annotations := got.GetAnnotations()
			assert.Equal(t, "deckhouse-cli", annotations["deckhouse.io/created-by"])

			spec, found, err := unstructured.NestedMap(got.Object, "spec")
			require.NoError(t, err)
			require.True(t, found, "spec not found")

			assert.Equal(t, tt.wantSpec["packageRepositoryName"], spec["packageRepositoryName"])
			assert.Equal(t, tt.wantSpec["type"], spec["type"])

			update, found, err := unstructured.NestedMap(got.Object, "spec", "update")
			require.NoError(t, err)
			require.True(t, found, "spec.update not found")

			wantUpdate := tt.wantSpec["update"].(map[string]interface{})
			assert.Equal(t, wantUpdate["fullScan"], update["fullScan"])
			assert.Equal(t, wantUpdate["timeout"], update["timeout"])
		})
	}
}

func TestNewCommand(t *testing.T) {
	cmd := NewCommand()

	assert.Equal(t, "scan <repository-name>", cmd.Use)
	assert.NotNil(t, cmd.ValidArgsFunction, "ValidArgsFunction should be set for shell completion")

	timeoutFlag := cmd.Flags().Lookup("timeout")
	require.NotNil(t, timeoutFlag, "timeout flag not found")
	assert.Equal(t, "5m0s", timeoutFlag.DefValue)

	nameFlag := cmd.Flags().Lookup("name")
	require.NotNil(t, nameFlag, "name flag not found")

	dryRunFlag := cmd.Flags().Lookup("dry-run")
	require.NotNil(t, dryRunFlag, "dry-run flag not found")
	assert.Equal(t, "false", dryRunFlag.DefValue)
}

func TestCompleteRepositoryNames(t *testing.T) {
	scheme := runtime.NewScheme()

	tests := []struct {
		name       string
		repos      []string
		toComplete string
		wantCount  int
		wantNames  map[string]bool
	}{
		{
			name:       "complete all repos",
			repos:      []string{"repo1", "repo2", "my-repo"},
			toComplete: "",
			wantCount:  3,
			wantNames:  map[string]bool{"repo1": true, "repo2": true, "my-repo": true},
		},
		{
			name:       "complete with prefix",
			repos:      []string{"repo1", "repo2", "my-repo"},
			toComplete: "repo",
			wantCount:  2,
			wantNames:  map[string]bool{"repo1": true, "repo2": true},
		},
		{
			name:       "complete with no match",
			repos:      []string{"repo1", "repo2"},
			toComplete: "xyz",
			wantCount:  0,
			wantNames:  map[string]bool{},
		},
		{
			name:       "empty repo list",
			repos:      []string{},
			toComplete: "",
			wantCount:  0,
			wantNames:  map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []runtime.Object
			for _, repoName := range tt.repos {
				objects = append(objects, &unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "deckhouse.io/v1alpha1",
						"kind":       "PackageRepository",
						"metadata": map[string]interface{}{
							"name": repoName,
						},
					},
				})
			}

			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
				map[schema.GroupVersionResource]string{
					packageRepositoryGVR: "PackageRepositoryList",
				},
				objects...)

			ctx := context.Background()
			repoClient := client.Resource(packageRepositoryGVR)
			list, err := repoClient.List(ctx, metav1.ListOptions{})
			require.NoError(t, err)

			var gotNames []string
			for _, item := range list.Items {
				name := item.GetName()
				if tt.toComplete == "" || len(name) >= len(tt.toComplete) && name[:len(tt.toComplete)] == tt.toComplete {
					gotNames = append(gotNames, name)
				}
			}

			assert.Len(t, gotNames, tt.wantCount)
			for _, name := range gotNames {
				assert.True(t, tt.wantNames[name], "unexpected name %q in results", name)
			}
		})
	}
}
