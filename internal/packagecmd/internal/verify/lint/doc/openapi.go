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
		"Only the top level of openapi/ is read, because that is where the runtime looks a package's schemas up. openapi/settings.yaml is the one every rule has something to say about, being the schema the UI builds a settings form from.",
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
		{
			ID:      rules.ExtensionsRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "Requires the x-deckhouse- extensions to hold the values the runtime decodes",
			Description: []string{
				"The runtime does not read a package's schemas as free-form JSON Schema: it decodes openapi/settings.yaml and openapi/values.yaml into a typed structure where every x-deckhouse- extension is a field of a fixed type. A value of the wrong type is not ignored — it fails the decode, and the whole package version is rejected with \"invalid JSON schema\" long after the package was built and published.",
				"That is a mistake nothing else catches. The document stays valid YAML and valid JSON Schema, the linters that read it are unaffected, and the package publishes cleanly; only the cluster that installs it disagrees.",
				"Every schema in the two files is checked, at any depth, through properties, definitions, items and the combinator branches — which is where the mistake tends to hide, a oneOf branch being the one place an author stops expecting their settings tree.",
				"An x-deckhouse- key the runtime knows nothing about is a separate case: it decodes fine and does nothing at all, which is what a misspelled extension looks like from the outside.",
			},
			Reports: []string{
				"x-deckhouse-grantable-resource, x-deckhouse-ui-validation-message or x-deckhouse-ui-group holds anything but a string",
				"x-deckhouse-ui-advanced or x-deckhouse-immutable holds anything but true or false",
				"x-deckhouse-ui-order holds anything but a whole number",
				"x-deckhouse-ui-resource-name is not an object, or misses apiVersion or kind",
				"x-deckhouse-validations is not a list of rules, or a rule misses its expression or its message",
				"x-deckhouse-grantable-resource is empty, or marks a field that is not type: string",
				"a key starting with x-deckhouse- is not an extension the runtime reads (warning)",
				"openapi/settings.yaml or openapi/values.yaml cannot be read or is not valid YAML",
			},
			Example: Example{
				Reported: []string{
					"properties:",
					"  storageClass:",
					"    type: string",
					"    # the object form belongs to x-deckhouse-ui-resource-name",
					"    x-deckhouse-grantable-resource:",
					"      apiVersion: storage.k8s.io/v1",
					"      kind: StorageClass",
				},
				Accepted: []string{
					"properties:",
					"  storageClass:",
					"    type: string",
					"    x-deckhouse-grantable-resource: storageclasses",
				},
			},
			Fix: "Give the extension the value its field takes: a resource name for x-deckhouse-grantable-resource, an apiVersion/kind object for x-deckhouse-ui-resource-name, a boolean for the markers, a list of expression/message pairs for x-deckhouse-validations.",
			Notes: []string{
				"The reported value is the pointer of the offending value, keywords and extension key included, so it lines up with the path the runtime names when it rejects the package.",
				"An unknown x-deckhouse- key reports as a warning rather than an error: the runtime ignores it, and a package built against a newer Deckhouse may legitimately carry an extension this plugin has not been taught yet.",
				"Only the two schemas the runtime decodes are checked. A doc-ru- translation carries descriptions rather than extensions, and is never decoded into the typed structure.",
				"x-deckhouse-validations needs a message on every rule. The decode accepts a rule without one, but the validator refuses to run it.",
			},
		},
		{
			ID:      rules.EnumRuleID,
			Impact:  lint.Warn.Ptr(),
			Summary: "Requires enum values to be CamelCase",
			Description: []string{
				"A setting that lists the values it accepts is declaring API constants, and the Kubernetes API conventions spell those in CamelCase with an initial capital: ClusterFirst, Pending, ClientIP.",
				"Every schema in openapi/ is checked, and every enum in it, however deeply nested. Translations and -tests.yaml fixtures are skipped: they carry descriptions and values rather than a schema.",
				"An acronym keeps all its letters capital, as in ClientIP or TCPDelay. Digits are allowed anywhere, and a dot is allowed inside a number, so Version2 and TLS1.3 are accepted while a space, hyphen or underscore is not.",
			},
			Reports: []string{
				"an enum value starts with a lower-case letter",
				"an enum value contains anything other than letters, digits and dots inside numbers",
				"a schema under openapi/ cannot be read or is not valid YAML",
			},
			Example: Example{
				Reported: []string{
					"properties:",
					"  logLevel:",
					"    type: string",
					"    enum:",
					"      - debug        # starts lower-case",
					"      - error-level  # hyphen",
				},
				Accepted: []string{
					"properties:",
					"  logLevel:",
					"    type: string",
					"    enum:",
					"      - Debug",
					"      - Error",
				},
			},
			Fix: "Rename the enum values to CamelCase and map them onto whatever the application expects in the template.",
			Notes: []string{
				"The reported value is the pointer of the enum holding the value, so it maps straight onto the lines of the schema.",
				"Values that are not strings are skipped, because a boolean or numeric constant carries no casing.",
				"This rule advises rather than blocks. An application whose settings pass values straight through to an upstream configuration file cannot follow the convention without translating every value back in the template, which is why a finding here is a warning.",
			},
		},
		{
			ID:      rules.BilingualRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "Requires a Russian translation for every settings schema",
			Description: []string{
				"Settings are published in both languages. The translation is a second schema next to the first, carrying the same property tree with Russian descriptions and nothing else, named after the schema it translates with a doc-ru- prefix.",
				"openapi/values.yaml is exempt. It describes the values computed for the templates rather than the settings a user writes, so it holds no user-facing descriptions to translate. A translation of it is accepted but never required.",
				"The check runs both ways, so a translation whose schema is missing is reported too — which is how a misspelled doc-ru- name surfaces instead of silently translating nothing.",
			},
			Reports: []string{
				"a schema under openapi/ other than values.yaml has no doc-ru- counterpart next to it",
				"a doc-ru- file under openapi/ has no schema of that name next to it",
			},
			Example: Example{
				Reported: []string{
					"my-package/openapi/",
					"  settings.yaml",
					"  values.yaml           # exempt",
				},
				Accepted: []string{
					"my-package/openapi/",
					"  settings.yaml",
					"  doc-ru-settings.yaml",
					"  values.yaml",
				},
			},
			Fix: "Add openapi/doc-ru-<name>.yaml next to every schema, holding the property tree of the schema with Russian descriptions.",
			Notes: []string{
				"A translation carries descriptions only. Types, defaults and enums stay in the schema itself, where the runtime validates against them.",
				"A package without openapi/ ships no schemas and reports nothing.",
			},
		},
	},
	Notes: []string{
		"The openapi linter is a hard schema contract: it has no .pkglint.yaml settings, and its rules report at their built-in severity.",
	},
}
