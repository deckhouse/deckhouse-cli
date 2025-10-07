/*
Copyright 2024 Flant JSC

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

package edit

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var editLong = templates.LongDesc(`
Edit ModuleConfig resource for a module using default editor.

Example:
  d8 k edit control-plane-manager

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	editCmd := &cobra.Command{
		Use:           "edit <module_name>",
		Short:         "Edit module configuration.",
		Long:          editLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          editModule,
	}
	return editCmd
}

func editModule(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("this command requires exactly 1 argument: module name")
	}
	moduleName := args[0]

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig flag: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("failed to get context flag: %w", err)
	}

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Define GVR for ModuleConfig
	gvr := schema.GroupVersionResource{
		Group:    "deckhouse.io",
		Version:  "v1alpha1",
		Resource: "moduleconfigs",
	}

	// Get current ModuleConfig
	unstruct, err := dynamicClient.Resource(gvr).Get(cmd.Context(), moduleName, operatemodule.GetOptions)
	if err != nil {
		return fmt.Errorf("failed to get ModuleConfig %q: %w", moduleName, err)
	}

	// Convert to YAML
	yamlBytes, err := operatemodule.ToYAML(unstruct)
	if err != nil {
		return fmt.Errorf("failed to convert ModuleConfig to YAML: %w", err)
	}

	// Launch editor
	editedYAML, err := launchEditor(yamlBytes, moduleName)
	if err != nil {
		return fmt.Errorf("failed to edit ModuleConfig: %w", err)
	}

	// Compare original and edited YAML
	if bytes.Equal(bytes.TrimSpace(yamlBytes), bytes.TrimSpace(editedYAML)) {
		fmt.Println("No changes detected. ModuleConfig was not updated.")
		return nil
	}

	// Print diff
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(string(yamlBytes), string(editedYAML), false)
	diffStr := dmp.DiffPrettyText(diffs)

	fmt.Println("\n--- Diff (original vs edited) ---")
	fmt.Println(diffStr)

	// Ask for confirmation
	fmt.Print("Apply these changes? [y/N]: ")
	var response string
	_, err = fmt.Scanln(&response)
	if err != nil && err.Error() != "unexpected newline" {
		return fmt.Errorf("failed to read user input: %w", err)
	}
	response = strings.TrimSpace(strings.ToLower(response))
	if response != "y" && response != "yes" {
		fmt.Println("Aborted by user. No changes applied.")
		return nil
	}

	// Parse edited YAML back to unstructured
	editedUnstruct, err := operatemodule.FromYAML(editedYAML)
	if err != nil {
		return fmt.Errorf("failed to parse edited YAML: %w", err)
	}

	// Preserve resource version for optimistic concurrency
	editedUnstruct.SetResourceVersion(unstruct.GetResourceVersion())

	// Update the resource
	_, err = dynamicClient.Resource(gvr).Update(cmd.Context(), editedUnstruct, operatemodule.UpdateOptions)
	if err != nil {
		return fmt.Errorf("failed to update ModuleConfig %q: %w", moduleName, err)
	}

	fmt.Println("ModuleConfig", moduleName, "edited successfully")
	return nil
}

// launchEditor opens the default editor and returns edited content
func launchEditor(content []byte, filename string) ([]byte, error) {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vi" // fallback
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("moduleconfig-%s-*.yaml", filename))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name()) // clean up

	if _, err := tmpFile.Write(content); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("failed to write to temp file: %w", err)
	}
	tmpFile.Close()

	cmd := exec.Command(editor, tmpFile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("editor failed: %w", err)
	}

	editedContent, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read edited file: %w", err)
	}

	return editedContent, nil
}
