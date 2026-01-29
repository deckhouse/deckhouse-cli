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

			if got.GetAPIVersion() != "deckhouse.io/v1alpha1" {
				t.Errorf("apiVersion = %v, want deckhouse.io/v1alpha1", got.GetAPIVersion())
			}

			if got.GetKind() != "PackageRepositoryOperation" {
				t.Errorf("kind = %v, want PackageRepositoryOperation", got.GetKind())
			}

			if got.GetName() != tt.operationName {
				t.Errorf("name = %v, want %v", got.GetName(), tt.operationName)
			}

			annotations := got.GetAnnotations()
			if annotations["deckhouse.io/created-by"] != "deckhouse-cli" {
				t.Errorf("annotation deckhouse.io/created-by = %v, want deckhouse-cli", annotations["deckhouse.io/created-by"])
			}

			spec, found, err := unstructured.NestedMap(got.Object, "spec")
			if err != nil || !found {
				t.Fatalf("spec not found: %v", err)
			}

			if spec["packageRepositoryName"] != tt.wantSpec["packageRepositoryName"] {
				t.Errorf("spec.packageRepositoryName = %v, want %v", spec["packageRepositoryName"], tt.wantSpec["packageRepositoryName"])
			}

			if spec["type"] != tt.wantSpec["type"] {
				t.Errorf("spec.type = %v, want %v", spec["type"], tt.wantSpec["type"])
			}

			update, found, err := unstructured.NestedMap(got.Object, "spec", "update")
			if err != nil || !found {
				t.Fatalf("spec.update not found: %v", err)
			}

			wantUpdate := tt.wantSpec["update"].(map[string]interface{})
			if update["fullScan"] != wantUpdate["fullScan"] {
				t.Errorf("spec.update.fullScan = %v, want %v", update["fullScan"], wantUpdate["fullScan"])
			}

			if update["timeout"] != wantUpdate["timeout"] {
				t.Errorf("spec.update.timeout = %v, want %v", update["timeout"], wantUpdate["timeout"])
			}
		})
	}
}

func TestNewCommand(t *testing.T) {
	cmd := NewCommand()

	if cmd.Use != "scan <repository-name>" {
		t.Errorf("Use = %v, want 'scan <repository-name>'", cmd.Use)
	}

	if cmd.ValidArgsFunction == nil {
		t.Error("ValidArgsFunction should be set for shell completion")
	}

	timeoutFlag := cmd.Flags().Lookup("timeout")
	if timeoutFlag == nil {
		t.Error("timeout flag not found")
	}
	if timeoutFlag.DefValue != "5m0s" {
		t.Errorf("timeout default = %v, want 5m0s", timeoutFlag.DefValue)
	}

	nameFlag := cmd.Flags().Lookup("name")
	if nameFlag == nil {
		t.Error("name flag not found")
	}

	dryRunFlag := cmd.Flags().Lookup("dry-run")
	if dryRunFlag == nil {
		t.Error("dry-run flag not found")
	}
	if dryRunFlag.DefValue != "false" {
		t.Errorf("dry-run default = %v, want false", dryRunFlag.DefValue)
	}
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
			if err != nil {
				t.Fatalf("failed to list repos: %v", err)
			}

			var gotNames []string
			for _, item := range list.Items {
				name := item.GetName()
				if tt.toComplete == "" || len(name) >= len(tt.toComplete) && name[:len(tt.toComplete)] == tt.toComplete {
					gotNames = append(gotNames, name)
				}
			}

			if len(gotNames) != tt.wantCount {
				t.Errorf("got %d names, want %d", len(gotNames), tt.wantCount)
				return
			}

			for _, name := range gotNames {
				if !tt.wantNames[name] {
					t.Errorf("unexpected name %q in results", name)
				}
			}
		})
	}
}
