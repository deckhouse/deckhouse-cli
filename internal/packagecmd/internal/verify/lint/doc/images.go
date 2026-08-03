package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images/rules"
)

// imagesDoc documents the images linter.
var imagesDoc = Linter{
	ID:      images.LinterID,
	Summary: "Image build inputs under images/",
	Description: []string{
		"Checks the per-image build directories a package ships: how patches are laid out and documented, and whether image names are usable as werf image names.",
		"The linter is skipped when the package carries no images/ directory.",
	},
	Rules: []Rule{
		{
			ID:      rules.PatchesRuleID,
			Summary: "Validates patch placement, naming and documentation",
			Description: []string{
				"A patch changes upstream source, so it must be findable and explained. The rule ties every .patch file to a location, an ordered name and a README section.",
			},
			Reports: []string{
				"a .patch file lives outside images/<image>/patches/",
				"a patch file name does not match XXX-<patch-name>.patch, three digits first",
				"a directory holding patches has no README.md",
				"the README.md of a patch directory has no '# <patch file name>' heading for a patch",
			},
			Example: Example{
				Reported: []string{
					"my-package/images/nginx/",
					"  fix-cve.patch          # wrong directory and name, undocumented",
					"  werf.inc.yaml",
				},
				Accepted: []string{
					"my-package/images/nginx/",
					"  patches/",
					"    001-fix-cve-2024-1234.patch",
					"    README.md            # contains '# 001-fix-cve-2024-1234.patch'",
					"  werf.inc.yaml",
				},
			},
			Fix:     "Move the patch to images/<image>/patches/, rename it to XXX-<patch-name>.patch, and document it under a '# <patch file name>' heading in that directory's README.md.",
			Tunable: true,
			Notes: []string{
				"Patch files are searched for across the whole package, so a stray patch outside images/ is reported rather than ignored.",
			},
		},
		{
			ID:      rules.ImageNameRuleID,
			Summary: "Forbids underscores in image names",
			Description: []string{
				"werf builds each image as images/<name>, and an underscore there is not a valid image name.",
				"Only directories that actually build something are checked: those carrying a Dockerfile or a werf.inc.yaml.",
			},
			Reports: []string{
				"an images/<name> build directory name contains an underscore",
				"a top-level image: declaration in images/<name>/werf.inc.yaml contains an underscore",
			},
			Example: Example{
				Reported: []string{
					"my-package/images/my_app/werf.inc.yaml",
					"  image: my_app",
				},
				Accepted: []string{
					"my-package/images/my-app/werf.inc.yaml",
					"  image: my-app",
				},
			},
			Fix:     "Rename the image using dashes instead of underscores, in both the directory name and the image: declaration.",
			Tunable: true,
		},
	},
	Notes: []string{
		"Runs against the source tree only: images/ holds build inputs and is never packaged into an image.",
	},
}
