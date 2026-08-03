package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs/rules"
)

// docsDoc documents the documentation linter.
var docsDoc = Linter{
	ID:      docs.LinterID,
	Summary: "Package documentation under docs/",
	Description: []string{
		"Checks the documentation shipped with the package: the entry point, the Russian translations and the language purity of the English files.",
		"Only the top level of docs/ is checked. Nested directories are walked but their files are left to the writer's discretion.",
	},
	Rules: []Rule{
		{
			ID:      rules.ReadmeRuleID,
			Summary: "Requires a non-empty docs/README.md",
			Description: []string{
				"docs/README.md is the page the catalog opens for a package, so an absent or empty file leaves the package undocumented.",
			},
			Reports: []string{
				"docs/README.md does not exist",
				"docs/README.md exists but is empty",
			},
			Example: Example{
				Reported: []string{
					"my-package/docs/",
					"  configuration.md",
				},
				Accepted: []string{
					"my-package/docs/",
					"  README.md          # what the package installs, how to configure it",
					"  configuration.md",
				},
			},
			Fix:     "Write docs/README.md describing what the package installs and how to configure it.",
			Tunable: true,
		},
		{
			ID:      rules.BilingualRuleID,
			Summary: "Requires a Russian counterpart for every English document",
			Description: []string{
				"Documentation is published in both languages, so each top-level English markdown file in docs/ needs a translation next to it.",
			},
			Reports: []string{
				"a top-level docs/<name>.md has no docs/<name>.ru.md next to it",
			},
			Example: Example{
				Reported: []string{
					"my-package/docs/",
					"  README.md",
					"  README.ru.md",
					"  configuration.md      # no translation",
				},
				Accepted: []string{
					"my-package/docs/",
					"  README.md",
					"  README.ru.md",
					"  configuration.md",
					"  configuration.ru.md",
				},
			},
			Fix:     "Add docs/<name>.ru.md for every docs/<name>.md.",
			Tunable: true,
			Notes: []string{
				"The legacy <name>_ru.md spelling is still accepted, but new translations should use the .ru.md suffix.",
			},
		},
		{
			ID:      rules.CyrillicInEnglishRuleID,
			Summary: "Rejects cyrillic text in English documentation",
			Description: []string{
				"Cyrillic in an English file is almost always untranslated text left behind when a document was copied from its Russian counterpart.",
				"Findings quote the offending line with a cursor marking each cyrillic character.",
			},
			Reports: []string{
				"a top-level English markdown file in docs/ contains cyrillic characters",
			},
			Example: Example{
				Reported: []string{
					"docs/README.md",
					"  ## Installation",
					"  Установите пакет через каталог.",
				},
				Accepted: []string{
					"docs/README.md",
					"  ## Installation",
					"  Install the package from the catalog.",
					"",
					"docs/README.ru.md",
					"  ## Установка",
					"  Установите пакет через каталог.",
				},
			},
			Fix:     "Translate the quoted text, or move it to the .ru.md counterpart.",
			Tunable: true,
			Notes: []string{
				"Russian files (.ru.md and the legacy _ru.md) are excluded from the scan.",
			},
		},
	},
}
