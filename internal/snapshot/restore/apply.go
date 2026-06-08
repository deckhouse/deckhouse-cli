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

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// Applier applies Kubernetes manifests via Server-Side Apply.
type Applier struct {
	Client       client.Client
	FieldManager string
	Log          *slog.Logger
}

// ResolvedOp pairs a ManifestOp with the (potentially editor-modified) data
// that passed its dry-run check.
type ResolvedOp struct {
	Op   ManifestOp
	Data []byte
}

// DryRunPhase performs a server dry-run for every manifest in the plan.
// On conflict (and when noEdit=false), it opens $EDITOR so the user can
// resolve the conflict. It returns a slice of ResolvedOps that can be fed
// to ApplyPhase. Any unresolvable error causes an early return.
func (a *Applier) DryRunPhase(ctx context.Context, plan *RestorePlan) ([]ResolvedOp, error) {
	resolved := make([]ResolvedOp, 0, len(plan.Manifests))

	for _, op := range plan.Manifests {
		data := op.Data

		err := a.applyOne(ctx, data, plan.Opts.TargetNamespace, true, false)
		if err != nil && (apierrors.IsConflict(err) || apierrors.IsInvalid(err)) && !plan.Opts.NoEdit {
			a.Log.Warn("dry-run conflict — opening editor",
				slog.String("kind", op.Kind),
				slog.String("name", op.Name),
				slog.String("err", err.Error()),
			)

			edited, editErr := editManifest(op, data, err)
			if editErr != nil {
				return nil, fmt.Errorf("editor for %s/%s: %w", op.Kind, op.Name, editErr)
			}

			data = edited
			err = a.applyOne(ctx, data, plan.Opts.TargetNamespace, true, false)
		}

		if err != nil {
			return nil, fmt.Errorf("dry-run failed for %s/%s %s: %w",
				op.APIVersion, op.Kind, op.Name, err)
		}

		resolved = append(resolved, ResolvedOp{Op: op, Data: data})
	}

	return resolved, nil
}

// ApplyPhase applies all resolved manifest ops using SSA.
func (a *Applier) ApplyPhase(ctx context.Context, ops []ResolvedOp, targetNS string, force bool) error {
	for _, rop := range ops {
		if err := a.applyOne(ctx, rop.Data, targetNS, false, force); err != nil {
			return fmt.Errorf("apply %s/%s %s: %w",
				rop.Op.APIVersion, rop.Op.Kind, rop.Op.Name, err)
		}

		a.Log.Info("applied",
			slog.String("kind", rop.Op.Kind),
			slog.String("name", rop.Op.Name),
		)
	}

	return nil
}

// applyOne applies a single JSON manifest via Server-Side Apply.
// When dryRun=true it performs server dry-run only.
// The namespace on namespaced objects is overridden to targetNS (if non-empty).
func (a *Applier) applyOne(ctx context.Context, data []byte, targetNS string, dryRun, force bool) error {
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, &u.Object); err != nil {
		return fmt.Errorf("parse manifest JSON: %w", err)
	}

	// Strip fields that must not be present in an SSA request.
	u.SetUID("")
	u.SetResourceVersion("")
	u.SetCreationTimestamp(metav1.Time{})
	u.SetManagedFields(nil)

	// Determine scope and set target namespace for namespaced objects.
	gvk := u.GroupVersionKind()
	if gvk.Kind == "" {
		return fmt.Errorf("manifest missing kind")
	}

	if targetNS != "" {
		mapping, err := a.Client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return fmt.Errorf("resolve REST mapping for %s: %w", gvk.Kind, err)
		}

		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			u.SetNamespace(targetNS)
		}
	}

	patchOpts := &client.PatchOptions{
		FieldManager: a.FieldManager,
	}

	if dryRun {
		patchOpts.DryRun = []string{metav1.DryRunAll}
	}

	if force {
		f := true
		patchOpts.Force = &f
	}

	return a.Client.Patch(ctx, u, client.Apply, patchOpts)
}

// editManifest serialises data as YAML, opens an editor showing the conflict
// error, waits for the user to save, then converts back to JSON.
func editManifest(op ManifestOp, data []byte, applyErr error) ([]byte, error) {
	// Convert JSON → YAML for the editor.
	yamlData, err := yaml.JSONToYAML(data)
	if err != nil {
		yamlData = data // fall back to raw JSON if conversion fails
	}

	header := buildEditorHeader(op, applyErr)
	editorInput := append([]byte(header), yamlData...)

	edited, err := OpenEditorYAML(editorInput)
	if err != nil {
		return nil, err
	}

	// Strip comment lines added by the header before converting back.
	jsonData, err := yaml.YAMLToJSON(edited)
	if err != nil {
		return nil, fmt.Errorf("convert edited YAML to JSON: %w", err)
	}

	return jsonData, nil
}

// buildEditorHeader builds a YAML comment block shown at the top of the editor
// file, explaining the conflict and instructing the user.
func buildEditorHeader(op ManifestOp, applyErr error) string {
	lines := []string{
		"# d8 snapshot restore — conflict resolution",
		fmt.Sprintf("# Object: %s/%s %q", op.APIVersion, op.Kind, op.Name),
		"#",
		"# Edit the manifest below to resolve the conflict, then save and exit.",
		"# Lines beginning with '#' are ignored.",
		"# Error:",
	}

	for _, line := range splitLines(applyErr.Error()) {
		lines = append(lines, "#   "+line)
	}

	lines = append(lines, "#", "---", "")

	result := ""
	for _, l := range lines {
		result += l + "\n"
	}

	return result
}

func splitLines(s string) []string {
	var out []string
	start := 0

	for i, c := range s {
		if c == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}

	if start < len(s) {
		out = append(out, s[start:])
	}

	return out
}
