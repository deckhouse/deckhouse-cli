package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss/rules"
)

// ossDoc documents the oss linter.
var ossDoc = Linter{
	ID:      oss.LinterID,
	Summary: "Open source metadata in oss.yaml",
	Description: []string{
		"Checks the optional oss.yaml, which lists the open source components a package ships along with their licenses and versions.",
		"The file is optional: when it is absent the linter runs no rules. When it is present it must be complete.",
	},
	Rules: []Rule{
		{
			ID:      rules.ParseRuleID,
			Summary: "Requires oss.yaml to be valid YAML",
			Description: []string{
				"The remaining rules read parsed components, so a document that cannot be decoded is reported once and stops the linter.",
			},
			Reports: []string{
				"oss.yaml exists but cannot be parsed as a list of components",
			},
			Example: Example{
				Reported: []string{
					"components:            # oss.yaml is a list, not a mapping",
					"  - id: echo-server",
				},
				Accepted: []string{
					"- id: echo-server",
					"  name: Echoserver",
				},
			},
			Fix:     "Fix the YAML syntax, or the shape of the component entries.",
			Tunable: true,
		},
		{
			ID:      rules.FieldsRuleID,
			Summary: "Requires every component field to be filled in",
			Description: []string{
				"Each component is a license attribution, which is only useful when it names the software, points at it and states its license.",
			},
			Reports: []string{
				"a component leaves id, name, description, link or license empty or blank",
			},
			Example: Example{
				Reported: []string{
					"- id: echo-server",
					"  name: Echoserver",
					"  link: https://ealenn.github.io/Echo-Server/",
					"  version: \"1.0\"",
					"  # description and license are missing",
				},
				Accepted: []string{
					"- id: echo-server",
					"  name: Echoserver",
					"  description: An echo server.",
					"  link: https://ealenn.github.io/Echo-Server/",
					"  license: Apache License 2.0",
					"  version: \"1.0\"",
				},
			},
			Fix:     "Fill in id, name, description, link and license for every component.",
			Tunable: true,
		},
		{
			ID:      rules.VersionRuleID,
			Summary: "Validates component version fields",
			Description: []string{
				"A component pins either a single version or a list of named versions, never both and never neither.",
			},
			Reports: []string{
				"a component sets both version and versions",
				"a component sets neither version nor versions",
				"an entry in versions leaves name or version empty",
			},
			Example: Example{
				Reported: []string{
					"- id: echo-server",
					"  version: \"1.0\"",
					"  versions:            # both version and versions are set",
					"    - name: server",
					"      version: \"1.0\"",
				},
				Accepted: []string{
					"- id: echo-server",
					"  versions:",
					"    - name: server",
					"      version: \"1.0\"",
					"    - name: client",
					"      version: \"0.9\"",
				},
			},
			Fix:     "Use version for a single version, or versions with a name and a version per entry.",
			Tunable: true,
		},
	},
	Notes: []string{
		"Not run against the release image: it carries release metadata only, without oss.yaml.",
	},
}
