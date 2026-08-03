package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs"
	docsrules "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs/rules"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon"
	iconrules "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon/rules"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images"
	imagesrules "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images/rules"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	ossrules "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss/rules"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
	templatesrules "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates/rules"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/settings"
)

// impacts holds the built-in severities of one linter.
type impacts struct {
	// linter is the linter-level severity cap.
	linter *lint.Level
	// rules maps a rule ID to its own severity. A rule absent from the map is not
	// tunable and runs at the linter severity.
	rules map[string]*lint.Level
}

// defaultImpacts indexes the built-in severity of every linter and rule by linter ID.
var defaultImpacts = buildDefaultImpacts()

// buildDefaultImpacts flattens the default settings tree into a per-linter index
// keyed by linter and rule identifiers. It mirrors the structure of the settings
// tree, not its values: a rule added there and forgotten here documents no
// severity rather than a stale one.
func buildDefaultImpacts() map[string]impacts {
	root := settings.Defaults()

	return map[string]impacts{
		templates.LinterID: {
			linter: root.Templates.LinterSettings.Impact,
			rules: map[string]*lint.Level{
				templatesrules.PDBRuleID:         root.Templates.RulesSettings.PDB.Impact,
				templatesrules.ServicePortRuleID: root.Templates.RulesSettings.ServicePort.Impact,
				templatesrules.VPARuleID:         root.Templates.RulesSettings.VPA.Impact,
			},
		},
		docs.LinterID: {
			linter: root.Documentation.LinterSettings.Impact,
			rules: map[string]*lint.Level{
				docsrules.ReadmeRuleID:            root.Documentation.RulesSettings.Readme.Impact,
				docsrules.BilingualRuleID:         root.Documentation.RulesSettings.Bilingual.Impact,
				docsrules.CyrillicInEnglishRuleID: root.Documentation.RulesSettings.CyrillicInEnglish.Impact,
			},
		},
		images.LinterID: {
			linter: root.Images.LinterSettings.Impact,
			rules: map[string]*lint.Level{
				imagesrules.PatchesRuleID:   root.Images.RulesSettings.Patches.Impact,
				imagesrules.ImageNameRuleID: root.Images.RulesSettings.ImageName.Impact,
			},
		},
		icon.LinterID: {
			linter: root.Icon.LinterSettings.Impact,
			rules: map[string]*lint.Level{
				iconrules.ExtRuleID:   root.Icon.RulesSettings.Ext.Impact,
				iconrules.SizeRuleID:  root.Icon.RulesSettings.Size.Impact,
				iconrules.ShapeRuleID: root.Icon.RulesSettings.Shape.Impact,
			},
		},
		oss.LinterID: {
			linter: root.OSS.LinterSettings.Impact,
			rules: map[string]*lint.Level{
				ossrules.ParseRuleID:   root.OSS.RulesSettings.Parse.Impact,
				ossrules.FieldsRuleID:  root.OSS.RulesSettings.Fields.Impact,
				ossrules.VersionRuleID: root.OSS.RulesSettings.Version.Impact,
			},
		},
	}
}

// linterImpact returns the built-in severity cap of a linter, or an empty string
// when the linter carries no indexed severity.
func linterImpact(linterID string) string {
	return levelString(defaultImpacts[linterID].linter)
}

// ruleImpact returns the built-in severity of a rule, or an empty string when the
// rule is not tunable and therefore has none of its own.
func ruleImpact(linterID, ruleID string) string {
	return levelString(defaultImpacts[linterID].rules[ruleID])
}

// levelString renders a severity, returning an empty string for an unset one.
func levelString(level *lint.Level) string {
	if level == nil {
		return ""
	}

	return level.String()
}
