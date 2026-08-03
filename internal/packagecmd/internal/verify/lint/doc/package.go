package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	pkglint "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/package"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/package/rules"
)

// packageDoc documents the package linter.
var packageDoc = Linter{
	ID:      pkglint.LinterID,
	Summary: "The package as an artifact: root shape and package.yaml metadata",
	Description: []string{
		"Checks the package itself rather than anything inside it — the entry points every package must ship, the build artifacts that must never be committed, and the requirements declared in package.yaml.",
		"Only presence is checked here. File contents are the concern of the docs, icon, images and oss linters.",
	},
	Rules: []Rule{
		{
			ID:      rules.NoWerfRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "Root-level werf files must not be committed",
			Description: []string{
				"delivery-kit owns generated werf files in the package root. A package can carry custom build logic under hooks/ or images/<name>/, but root-level werf files make the artifact ambiguous.",
			},
			Reports: []string{
				"a .werf directory or a werf.yaml file exists in the package root",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  package.yaml",
					"  werf.yaml        # build config is generated",
				},
				Accepted: []string{
					"my-package/",
					"  package.yaml",
					"  images/nginx/werf.inc.yaml",
				},
			},
			Fix: "Delete root-level werf files, and move custom build logic to hooks/ or images/<name>/werf.inc.yaml.",
			Notes: []string{
				"This rule applies to the source tree only. Images do not carry root-level werf files.",
			},
		},
		{
			ID:      rules.NoChartRuleID,
			Impact:  lint.Warn.Ptr(),
			Summary: "Chart.yaml must not be committed to the package root",
			Description: []string{
				"package.yaml is the root metadata source for a package. A root-level Chart.yaml is misleading and is not part of the package contract.",
			},
			Reports: []string{
				"a Chart.yaml file exists in the package root",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  Chart.yaml       # metadata belongs in package.yaml",
					"  package.yaml",
				},
				Accepted: []string{
					"my-package/",
					"  package.yaml",
				},
			},
			Fix: "Delete Chart.yaml and keep package metadata in package.yaml.",
			Notes: []string{
				"This rule applies to the source tree only.",
			},
		},
		{
			ID:      rules.NoHelmignoreRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: ".helmignore must not be committed to the package root",
			Description: []string{
				".helmignore is generated during build. A committed copy can drift from generated packaging behavior.",
			},
			Reports: []string{
				"a .helmignore file exists in the package root",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  .helmignore      # generated at build time",
					"  package.yaml",
				},
				Accepted: []string{
					"my-package/",
					"  package.yaml",
				},
			},
			Fix: "Delete .helmignore from the source tree.",
			Notes: []string{
				"This rule applies to the source tree only. Images can carry a generated .helmignore on purpose.",
			},
		},
		{
			ID:      rules.HasGitignoreRuleID,
			Impact:  lint.Warn.Ptr(),
			Summary: "A package source tree must carry .gitignore",
			Description: []string{
				"A package source tree should carry source-control helper files that are intentionally absent from published package images.",
			},
			Reports: []string{
				"no .gitignore file exists in the package root",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  package.yaml",
				},
				Accepted: []string{
					"my-package/",
					"  .gitignore",
					"  package.yaml",
				},
			},
			Fix: "Add .gitignore to the package root.",
			Notes: []string{
				"This rule applies to the source tree only because published images do not carry source-control files.",
			},
		},
		{
			ID:      rules.HasChangelogRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "A package must carry changelog.yaml",
			Description: []string{
				"Every package must carry release history in its root so users can inspect changes in the source tree and published images.",
			},
			Reports: []string{
				"no changelog.yaml file exists in the package root",
				"changelog.yaml exists as a directory",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  package.yaml",
				},
				Accepted: []string{
					"my-package/",
					"  changelog.yaml",
					"  package.yaml",
				},
			},
			Fix: "Add changelog.yaml to the package root.",
		},
		{
			ID:      rules.HasDocsRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "A package must carry docs/",
			Description: []string{
				"Every package must carry documentation in its root so users can inspect it in the source tree and published images.",
			},
			Reports: []string{
				"no docs directory exists in the package root",
				"docs exists as a file",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  package.yaml",
				},
				Accepted: []string{
					"my-package/",
					"  docs/README.md",
					"  package.yaml",
				},
			},
			Fix: "Add docs/ to the package root.",
		},
		{
			ID:      rules.HasIconRuleID,
			Impact:  lint.Warn.Ptr(),
			Summary: "A package icon must exist",
			Description: []string{
				"Every package should carry a supported icon file at its docs.",
				"Only presence is checked here. Format, file size and dimensions are checked by the icon linter.",
			},
			Reports: []string{
				"no " + iconutil.Expected() + " file exists in the package docs",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  package.yaml",
				},
				Accepted: []string{
					"my-package/",
					"  icon.svg",
					"  package.yaml",
				},
			},
			Fix: "Add a supported icon file to the package docs.",
			Notes: []string{
				"Format, file size and dimensions are checked by the icon linter.",
			},
		},
		{
			ID:      rules.ValidRequirementsRuleID,
			Impact:  lint.Error.Ptr(),
			Summary: "Requires package.yaml requirements to be valid",
			Description: []string{
				"The optional requirements block declares the Kubernetes and Deckhouse versions a package needs and the modules it depends on.",
				"A group gates the package on a set of modules: every anyOf group needs at least one of its members enabled, and no member of a noneOf group may be enabled.",
				"A malformed constraint or a module placed in contradictory buckets can never be satisfied, so the package would be rejected at install time. Reporting it here shows the author the problem while the package is still being built.",
			},
			Reports: []string{
				"requirements.kubernetes.constraint is set but is not valid semver",
				"requirements.deckhouse.constraint is set but is not valid semver",
				"a mandatory, conditional, anyOf or noneOf module sets a constraint that is not valid semver",
				"a group leaves its name empty, or repeats the name of another group in the same bucket",
				"a group lists no modules",
				"a module entry leaves its name empty, or repeats another entry of the same group",
				"a module is both mandatory and conditional",
				"a module is both a group member and a mandatory or conditional dependency",
				"a module belongs to an anyOf group and a noneOf group at once",
			},
			Example: Example{
				Reported: []string{
					"requirements:",
					"  kubernetes:",
					"    constraint: \"1.27+\"        # not a semver expression",
					"  modules:",
					"    mandatory:",
					"      - name: ingress-nginx",
					"    noneOf:",
					"      - name: conflicting-ingress",
					"        modules:",
					"          - name: ingress-nginx   # required and forbidden at once",
				},
				Accepted: []string{
					"requirements:",
					"  kubernetes:",
					"    constraint: \">= 1.27\"",
					"  deckhouse:",
					"    constraint: \">= 1.66.0\"",
					"  modules:",
					"    mandatory:",
					"      - name: ingress-nginx",
					"        constraint: \">= 1.0.0\"",
					"    anyOf:",
					"      - name: storage",
					"        description: A storage backend is required.",
					"        modules:",
					"          - name: sds-local-volume",
					"          - name: ceph-csi",
				},
			},
			Fix: "Write constraints as semver expressions, give every group a unique name and at least one uniquely named module, and keep each module in a single bucket.",
		},
	},
	Notes: []string{
		"The package linter is a hard package contract: it has no .pkglint.yaml settings, and its rules report at their built-in severity.",
		"The requirements block is optional: a package that declares none runs valid_requirements against an empty declaration and reports nothing.",
	},
}
