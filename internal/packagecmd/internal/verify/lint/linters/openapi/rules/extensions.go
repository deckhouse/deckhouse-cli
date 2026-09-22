package rules

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: keep every x-deckhouse- extension in a schema decodable by the runtime,
// which reads them into typed fields and rejects the whole package version otherwise.

// ExtensionsRuleID is the stable identifier used to reference this rule in diagnostics.
const ExtensionsRuleID = "extensions"

// extensionPrefix marks the vendor extensions the runtime reads from a schema.
const extensionPrefix = "x-deckhouse-"

const (
	// grantKey binds a settings field to a grantable cluster resource.
	grantKey = "x-deckhouse-grantable-resource"
	// validationsKey holds the CEL rules a value is validated against.
	validationsKey = "x-deckhouse-validations"
	// immutableKey freezes a setting after the application is created.
	immutableKey = "x-deckhouse-immutable"
	// orderKey sets the display order of a setting in the UI.
	orderKey = "x-deckhouse-ui-order"
	// validationMessageKey overrides the validation error shown by the UI.
	validationMessageKey = "x-deckhouse-ui-validation-message"
	// resourceNameKey binds a setting to a live selection of cluster resources.
	resourceNameKey = "x-deckhouse-ui-resource-name"
	// groupKey renders a setting inside a named group of the UI form.
	groupKey = "x-deckhouse-ui-group"
)

// stringType is the only schema type a grantable resource can be resolved into.
const stringType = "string"

// decodedSchemas are the schemas the runtime decodes into its typed schema structure, and
// therefore the only ones an extension has any effect in.
var decodedSchemas = []string{settingsFile, valuesFile}

// extensionChecks maps every extension the runtime reads to the check that its value fits
// the field it is decoded into. The source of truth for the shapes is OpenAPIV3Schema in
// deckhouse-controller/pkg/apis/deckhouse.io/openapi/types.go.
var extensionChecks = map[string]func(value any) []problem{
	grantKey:             checkGrant,
	validationsKey:       checkValidations,
	resourceNameKey:      checkResourceName,
	advancedKey:          expectBool,
	immutableKey:         expectBool,
	orderKey:             expectInteger,
	validationMessageKey: expectString,
	groupKey:             expectString,
}

// ExtensionsRule checks that the x-deckhouse- extensions of a package's schemas hold the
// values the runtime can decode.
type ExtensionsRule struct {
	collector *diag.Collector
	path      string
}

// NewExtensionsRule constructs an ExtensionsRule scoped to a package directory. Which
// schemas it reads is fixed, so the rule resolves them from packageDir itself.
func NewExtensionsRule(packageDir string, collector *diag.Collector) *ExtensionsRule {
	return &ExtensionsRule{
		path:      packageDir,
		collector: collector.With(diag.RuleID(ExtensionsRuleID)),
	}
}

// Check reports every extension value the runtime cannot decode, in the two schemas it
// decodes. An absent schema is not a finding: a package is free to ship neither.
//
// Findings are errors, because the runtime rejects the whole package version over one of
// them, except an unknown x-deckhouse- key, which it ignores and which is a warning.
func (r *ExtensionsRule) Check(_ context.Context) {
	for _, name := range decodedSchemas {
		r.checkSchema(name)
	}

	r.collector.Commit()
}

// checkSchema reports the undecodable extension values of one schema file.
func (r *ExtensionsRule) checkSchema(name string) {
	collector := r.collector.With(diag.Path(filepath.Join(openAPIDir, name)))

	raw, err := os.ReadFile(filepath.Join(r.path, openAPIDir, name))
	if os.IsNotExist(err) {
		return
	}

	if err != nil {
		collector.Error("failed to read %s: %v", name, err)

		return
	}

	var root map[string]any
	if err = yaml.Unmarshal(raw, &root); err != nil {
		collector.Error("failed to parse %s: %v", name, err)

		return
	}

	for _, finding := range invalidExtensions(root) {
		scoped := collector.With(diag.Value(finding.pointer))

		if finding.level == lint.Warn {
			scoped.Warn("%s", finding.message)

			continue
		}

		scoped.Error("%s", finding.message)
	}
}

// extensionFinding is one extension value the runtime cannot use.
type extensionFinding struct {
	// pointer locates the offending value inside the schema.
	pointer string
	// message states what the value breaks.
	message string
	// level is the severity the finding reports at.
	level lint.Level
}

// problem is one thing wrong with an extension value, located relative to the extension
// key itself: a nested pointer for a list entry or an object field, empty for the value
// taken as a whole.
type problem struct {
	pointer string
	reason  string
}

// invalidExtensions returns a finding for every extension in root the runtime cannot use,
// sorted so the same schema always reports in the same order.
func invalidExtensions(root map[string]any) []extensionFinding {
	var found []extensionFinding

	walkSchemas(root, func(node map[string]any, pointer string, _ int) {
		found = append(found, nodeExtensions(node, pointer)...)
	})

	slices.SortFunc(found, func(a, b extensionFinding) int {
		return cmp.Or(
			cmp.Compare(a.pointer, b.pointer),
			cmp.Compare(a.message, b.message))
	})

	return found
}

// nodeExtensions returns the findings of the extensions carried by one schema.
func nodeExtensions(node map[string]any, pointer string) []extensionFinding {
	var found []extensionFinding

	for key, value := range node {
		if !strings.HasPrefix(key, extensionPrefix) {
			continue
		}

		check, ok := extensionChecks[key]
		if !ok {
			found = append(found, extensionFinding{
				pointer: joinPointer(pointer, key),
				message: key + " is not read by the runtime and has no effect",
				level:   lint.Warn,
			})

			continue
		}

		for _, p := range check(value) {
			found = append(found, extensionFinding{
				pointer: joinPointer(pointer, key, p.pointer),
				message: joinPointer(key, p.pointer) + " " + p.reason,
				level:   lint.Error,
			})
		}

		// The grant is resolved into the value of the field it marks, so unlike the other
		// extensions it constrains the schema carrying it as well as its own value.
		if key == grantKey {
			if reason := grantTarget(node); reason != "" {
				found = append(found, extensionFinding{
					pointer: joinPointer(pointer, key),
					message: key + " " + reason,
					level:   lint.Error,
				})
			}
		}
	}

	return found
}

// checkGrant reports a grantable resource that is not a name the runtime can look up.
func checkGrant(value any) []problem {
	name, ok := value.(string)
	if !ok {
		return []problem{{reason: "must be the name of a grantable resource, got " + typeName(value)}}
	}

	if name == "" {
		return []problem{{reason: "must name a grantable resource, got an empty string"}}
	}

	return nil
}

// grantTarget reports why the schema carrying the grant cannot hold one, or an empty
// string when it can. The grant is resolved into the field's value, so the runtime accepts
// it on a string field only.
func grantTarget(node map[string]any) string {
	declared, ok := node["type"]
	if !ok {
		return "needs an explicit type: string on the field it marks"
	}

	switch typed := declared.(type) {
	case string:
		if typed == stringType {
			return ""
		}
	case []any:
		if slices.Contains(typed, any(stringType)) {
			return ""
		}
	}

	return "is only supported on a type: string field"
}

// checkValidations reports the CEL rules the runtime cannot read. A rule needs both an
// expression to evaluate and a message to fail with; the remaining keys are optional but
// still have to be strings.
func checkValidations(value any) []problem {
	list, ok := value.([]any)
	if !ok {
		return []problem{{reason: "must be a list of validation rules, got " + typeName(value)}}
	}

	var problems []problem

	for i, item := range list {
		index := strconv.Itoa(i)

		rule, ok := item.(map[string]any)
		if !ok {
			problems = append(problems, problem{pointer: index, reason: "must be a validation rule, got " + typeName(item)})

			continue
		}

		problems = append(problems,
			requiredString(rule, index, "expression"),
			requiredString(rule, index, "message"))

		for _, name := range []string{"messageExpression", "fieldPath", "reason"} {
			problems = append(problems, optionalString(rule, index, name))
		}
	}

	return slices.DeleteFunc(problems, func(p problem) bool { return p.reason == "" })
}

// checkResourceName reports a resource selector the runtime cannot read. The kind it
// selects takes both an apiVersion and a kind; the label selector narrowing it is optional.
func checkResourceName(value any) []problem {
	selector, ok := value.(map[string]any)
	if !ok {
		return []problem{{reason: "must be an object with apiVersion and kind, got " + typeName(value)}}
	}

	problems := []problem{
		requiredString(selector, "", "apiVersion"),
		requiredString(selector, "", "kind"),
	}

	if narrowing, ok := selector["labelSelector"]; ok {
		if _, ok := narrowing.(map[string]any); !ok {
			problems = append(problems, problem{pointer: "labelSelector", reason: "must be a label selector, got " + typeName(narrowing)})
		}
	}

	return slices.DeleteFunc(problems, func(p problem) bool { return p.reason == "" })
}

// requiredString reports a key of an extension object that is missing or is not a
// non-empty string. The returned problem is empty when the key is fine.
func requiredString(object map[string]any, parent, key string) problem {
	value, ok := object[key]
	if !ok {
		return problem{pointer: parent, reason: "must set " + key}
	}

	str, ok := value.(string)
	if !ok {
		return problem{pointer: joinPointer(parent, key), reason: "must be a string, got " + typeName(value)}
	}

	if str == "" {
		return problem{pointer: joinPointer(parent, key), reason: "must not be empty"}
	}

	return problem{}
}

// optionalString reports a key of an extension object that is set to something other than
// a string. The returned problem is empty when the key is absent or fine.
func optionalString(object map[string]any, parent, key string) problem {
	value, ok := object[key]
	if !ok {
		return problem{}
	}

	if _, ok := value.(string); !ok {
		return problem{pointer: joinPointer(parent, key), reason: "must be a string, got " + typeName(value)}
	}

	return problem{}
}

// expectString reports a value that is not a string.
func expectString(value any) []problem {
	if _, ok := value.(string); ok {
		return nil
	}

	return []problem{{reason: "must be a string, got " + typeName(value)}}
}

// expectBool reports a value that is not a boolean.
func expectBool(value any) []problem {
	if _, ok := value.(bool); ok {
		return nil
	}

	return []problem{{reason: "must be true or false, got " + typeName(value)}}
}

// expectInteger reports a value that is not a whole number. YAML numbers decode as
// floats, so the fractional part is what tells the two apart.
func expectInteger(value any) []problem {
	number, ok := value.(float64)
	if !ok {
		return []problem{{reason: "must be an integer, got " + typeName(value)}}
	}

	if number != math.Trunc(number) {
		return []problem{{reason: fmt.Sprintf("must be an integer, got %v", number)}}
	}

	return nil
}

// typeName names the type of a decoded value for the "got ..." half of a message.
func typeName(value any) string {
	switch value.(type) {
	case nil:
		return "nothing"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "a list"
	case map[string]any:
		return "an object"
	default:
		return fmt.Sprintf("%T", value)
	}
}
