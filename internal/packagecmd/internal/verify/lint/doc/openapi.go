package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/openapi"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/openapi/rules"
)

// openapiDoc documents the openapi linter.
var openapiDoc = Linter{
	ID:      openapi.LinterID,
	Summary: "The OpenAPI schemas a package ships under openapi/",
	Description: []string{
		"Checks the schemas that describe a package's settings, rather than the values rendered from them — a rendered manifest is the concern of the templates linter.",
		"Only openapi/settings.yaml is inspected, because it is the schema the UI builds a settings form from.",
	},
	Rules: []Rule{
		{
			ID:      rules.AdvancedRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "x-deckhouse-ui-advanced is allowed on top-level settings only",
			Description: []string{
				"The x-deckhouse-ui-advanced extension folds a setting behind an \"advanced\" toggle in the UI. It is read from the top level of openapi/settings.yaml — the root object and the entries of its properties map — and nowhere below.",
				"Deeper down the extension has no effect at all: the author writes it expecting one field of a nested object to be hidden, the UI ignores it, and the field is rendered like any other. Reporting it here is the only place that mistake becomes visible.",
				"The extension is optional. A schema that never sets it, and a schema that sets it on top-level settings only, both satisfy this rule.",
				"Depth is counted in settings, not in YAML nesting: allOf, anyOf, oneOf and not branches constrain the same object as their parent, so a branch stays at its parent's level. A oneOf branch that re-declares a top-level setting is therefore still top level, while properties, patternProperties, items and additionalProperties each descend a level.",
			},
			Reports: []string{
				"a schema more than one settings level below the root sets x-deckhouse-ui-advanced",
				"openapi/settings.yaml exists but cannot be read or is not valid YAML",
			},
			Example: Example{
				Reported: []string{
					"type: object",
					"properties:",
					"  storage:",
					"    type: object",
					"    x-deckhouse-ui-advanced: true       # top level, honoured",
					"    properties:",
					"      size:",
					"        type: string",
					"        x-deckhouse-ui-advanced: true   # nested, ignored by the UI",
				},
				Accepted: []string{
					"type: object",
					"properties:",
					"  storage:",
					"    type: object",
					"    x-deckhouse-ui-advanced: true       # hides the whole storage block",
					"    properties:",
					"      size:",
					"        type: string",
				},
			},
			Fix: "Mark the top-level setting that owns the nested schema as advanced, or drop x-deckhouse-ui-advanced from the nested schema.",
			Notes: []string{
				"The reported value is the schema pointer of the offending schema, keywords included, so it maps straight onto the lines of settings.yaml.",
				"A definitions or $defs entry stands in for a top-level setting, so its own body may carry the marker but the schemas inside it may not.",
				"A package without openapi/settings.yaml exposes no settings and reports nothing.",
			},
		},
	},
	Notes: []string{
		"The openapi linter is a hard schema contract: it has no .pkglint.yaml settings, and its rules report at their built-in severity.",
	},
}
