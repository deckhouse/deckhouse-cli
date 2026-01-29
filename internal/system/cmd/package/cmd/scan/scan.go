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
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/cli"
	constants "github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/const"
)

var packageRepositoryGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "packagerepositories",
}

var packageRepositoryOperationGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "packagerepositoryoperations",
}

var scanLong = templates.LongDesc(`
Create a scan task for a PackageRepository.

This command creates a PackageRepositoryOperation resource that triggers
a full scan of the specified package repository.

© Flant JSC 2026`)

var scanExample = templates.Examples(`
  # Scan a package repository named "my-repo"
  d8 system package scan my-repo

  # Scan with a custom timeout
  d8 system package scan my-repo --timeout 10m

  # Scan with a custom operation name
  d8 system package scan my-repo --name my-scan-operation

  # Preview the operation without creating it
  d8 system package scan my-repo --dry-run
`)

type scanOptions struct {
	timeout       time.Duration
	operationName string
	dryRun        bool
}

func NewCommand() *cobra.Command {
	opts := &scanOptions{}

	scanCmd := &cobra.Command{
		Use:               "scan <repository-name>",
		Short:             "Create a scan task for a PackageRepository",
		Long:              scanLong,
		Example:           scanExample,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeRepositoryNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScan(cmd, args[0], opts)
		},
	}

	scanCmd.Flags().DurationVar(&opts.timeout, "timeout", 5*time.Minute, "Timeout for the scan operation")
	scanCmd.Flags().StringVar(&opts.operationName, "name", "", "Custom name for the PackageRepositoryOperation (auto-generated if not specified)")
	scanCmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Preview the operation without creating it")

	return scanCmd
}

func completeRepositoryNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	dynamicClient, err := cli.GetDynamicClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultAPITimeout)
	defer cancel()

	repoClient := dynamicClient.Resource(packageRepositoryGVR)
	list, err := repoClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	var names []string
	for _, item := range list.Items {
		name := item.GetName()
		if strings.HasPrefix(name, toComplete) {
			names = append(names, name)
		}
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}

func runScan(cmd *cobra.Command, repositoryName string, opts *scanOptions) error {
	dynamicClient, err := cli.GetDynamicClient(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultAPITimeout)
	defer cancel()

	repoClient := dynamicClient.Resource(packageRepositoryGVR)
	if _, err := repoClient.Get(ctx, repositoryName, metav1.GetOptions{}); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("PackageRepository '%s' not found", repositoryName)
		}
		return fmt.Errorf("failed to get PackageRepository: %w", err)
	}

	operation := buildPackageRepositoryOperation(opts.operationName, repositoryName, opts.timeout)

	if opts.dryRun {
		yamlBytes, err := yaml.Marshal(operation.Object)
		if err != nil {
			return fmt.Errorf("failed to marshal operation: %w", err)
		}
		fmt.Printf("%s Would create PackageRepositoryOperation:\n---\n%s", cli.MsgInfo, string(yamlBytes))
		return nil
	}

	operationClient := dynamicClient.Resource(packageRepositoryOperationGVR)
	created, err := operationClient.Create(ctx, operation, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create PackageRepositoryOperation: %w", err)
	}

	fmt.Printf("%s Created PackageRepositoryOperation '%s' for repository '%s'\n",
		cli.MsgInfo, created.GetName(), repositoryName)

	return nil
}

func buildPackageRepositoryOperation(customName, repositoryName string, timeout time.Duration) *unstructured.Unstructured {
	metadata := map[string]any{
		"annotations": map[string]any{
			"deckhouse.io/created-by": "deckhouse-cli",
		},
	}

	if customName != "" {
		metadata["name"] = customName
	}

	if customName == "" {
		metadata["generateName"] = repositoryName + "-scan-manual-"
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1alpha1",
			"kind":       "PackageRepositoryOperation",
			"metadata":   metadata,
			"spec": map[string]any{
				"packageRepositoryName": repositoryName,
				"type":                  "Update",
				"update": map[string]any{
					"fullScan": true,
					"timeout":  timeout.String(),
				},
			},
		},
	}
}
