package rules

import (
	"context"
	"os"
	"path/filepath"
	"slices"

	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: keep the UI-advanced marker at the top level of the settings schema, the
// only depth the UI reads it from.

// AdvancedRuleID is the stable identifier used to reference this rule in diagnostics.
const AdvancedRuleID = "advanced"

const (
	// settingsFile is the OpenAPI schema describing user-configurable settings.
	settingsFile = "settings.yaml"
	// advancedKey is the vendor extension that marks a setting as advanced in the UI.
	advancedKey = "x-deckhouse-ui-advanced"
	// maxAdvancedDepth is the deepest property level the marker is honoured at. Depth 0 is
	// the schema root and depth 1 its top-level settings; below that the UI ignores it.
	maxAdvancedDepth = 1
)

// AdvancedRule checks that the settings schema carries the advanced marker at its top
// level only.
type AdvancedRule struct {
	collector *diag.Collector
	path      string
}

// NewAdvancedRule constructs an AdvancedRule scoped to a package directory. The
// schema location inside it is fixed, so the rule resolves it from packageDir itself.
func NewAdvancedRule(packageDir string, collector *diag.Collector) *AdvancedRule {
	return &AdvancedRule{
		path: packageDir,
		collector: collector.With(
			diag.RuleID(AdvancedRuleID),
			diag.Path(filepath.Join(openAPIDir, settingsFile))),
	}
}

// Check reports every schema below the top level that carries the advanced marker. The
// marker is optional: a schema that never sets it, and one that sets it on the root object
// or on its top-level settings, all satisfy the rule. An absent settings.yaml means the
// package exposes no settings, which is not this rule's concern.
func (r *AdvancedRule) Check(_ context.Context) {
	raw, err := os.ReadFile(filepath.Join(r.path, openAPIDir, settingsFile))
	if os.IsNotExist(err) {
		return
	}

	if err != nil {
		r.collector.Error("failed to read %s: %v", settingsFile, err)

		return
	}

	var root map[string]any
	if err = yaml.Unmarshal(raw, &root); err != nil {
		r.collector.Error("failed to parse %s: %v", settingsFile, err)

		return
	}

	for _, pointer := range deepAdvanced(root) {
		r.collector.With(diag.Value(pointer)).
			Error("%s is allowed only on root properties", advancedKey)
	}

	r.collector.Commit()
}

// deepAdvanced returns the pointer of every schema below the top level of root that
// carries the advanced marker, sorted so the same schema always reports in the same order.
func deepAdvanced(root map[string]any) []string {
	var found []string

	walkSchemas(root, func(node map[string]any, pointer string, depth int) {
		if depth <= maxAdvancedDepth {
			return
		}

		if _, ok := node[advancedKey]; ok {
			found = append(found, pointer)
		}
	})

	slices.Sort(found)

	return found
}
