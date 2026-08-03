package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon/rules"
)

// iconDoc documents the icon linter.
var iconDoc = Linter{
	ID:      icon.LinterID,
	Summary: "Content of the package icon",
	Description: []string{
		"Discovers " + iconutil.Expected() + " in the package root once, decodes it, and checks the resulting format, file size and dimensions against the catalog UI limits.",
		"A file that cannot be decoded as the format its extension claims is reported by the linter itself, before the rules run.",
	},
	Rules: []Rule{
		{
			ID:      rules.ExtRuleID,
			Summary: "Requires a supported icon format",
			Description: []string{
				"Only formats the catalog can render are accepted: " + iconutil.Expected() + ".",
			},
			Reports: []string{
				"the icon extension is not one of the supported formats",
			},
			Example: Example{
				Reported: []string{
					"my-package/",
					"  icon.gif",
				},
				Accepted: []string{
					"my-package/",
					"  icon.png",
				},
			},
			Fix:     "Convert the icon to a supported format and name it accordingly.",
			Tunable: true,
		},
		{
			ID:      rules.SizeRuleID,
			Summary: "Caps the icon file size",
			Description: []string{
				"The icon travels in every published image, so it is kept small. The limit is 150 KB on disk.",
			},
			Reports: []string{
				"the icon file exceeds 150 KB",
			},
			Example: Example{
				Reported: []string{
					"$ ls -l my-package/icon.png",
					"  312K  icon.png",
				},
				Accepted: []string{
					"$ ls -l my-package/icon.png",
					"  84K   icon.png",
				},
			},
			Fix:     "Compress the icon, or export it at a smaller resolution.",
			Tunable: true,
		},
		{
			ID:      rules.ShapeRuleID,
			Summary: "Caps the rendered icon dimensions",
			Description: []string{
				"The catalog UI renders the icon in a fixed box, so neither side may exceed 300 pixels.",
			},
			Reports: []string{
				"the decoded icon is wider or taller than 300x300 pixels",
			},
			Example: Example{
				Reported: []string{
					"$ file my-package/icon.png",
					"  PNG image data, 512 x 512",
				},
				Accepted: []string{
					"$ file my-package/icon.png",
					"  PNG image data, 256 x 256",
				},
			},
			Fix:     "Export the icon at 300x300 pixels or smaller.",
			Tunable: true,
			Notes: []string{
				"Vector icons with no intrinsic rasterized size are skipped.",
			},
		},
	},
	Notes: []string{
		"Existence of the icon belongs to the package linter's has_icon rule. When no icon is present, this linter records an ignored-level note and runs no rules.",
	},
}
