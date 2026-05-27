package docs

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "docs"

// Config holds the path and settings required to construct a Linter.
type Config struct {
	Path     string
	Settings LinterSettings
}

// LinterSettings combines the shared linter-level severity with the per-rule settings for this linter.
type LinterSettings struct {
	lint.LinterSettings
	RulesSettings
}

// RulesSettings holds the severity configuration for each rule in the docs linter.
type RulesSettings struct {
	Readme            lint.RuleSettings
	Bilingual         lint.RuleSettings
	CyrillicInEnglish lint.RuleSettings
}

// NewLinter constructs a Linter from cfg, scoping its diagnostics to this linter and capping severity at the configured level.
func NewLinter(cfg Config, res *diag.Collector) *Linter {
	return &Linter{
		settings: cfg.Settings.RulesSettings,
		config:   cfg,
		collector: res.With(
			diag.LinterID(LinterID),
			diag.Path(cfg.Path),
			diag.MaxLevel(cfg.Settings.LinterSettings.Impact)),
	}
}

// Linter runs documentation rules against a package directory.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// Lint executes all documentation rules against the configured package path.
func (l *Linter) Lint(ctx context.Context) {
	if !hasDocsDir(l.config.Path) {
		l.collector.With(diag.MaxLevel(lint.Ignored.Ptr())).Warn("docs not found in package root")
		return
	}

	rules.NewReadmeRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Readme.Impact))).Check(ctx)
	rules.NewBilingualRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Bilingual.Impact))).Check(ctx)
	rules.NewCyrillicInEnglishRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.CyrillicInEnglish.Impact))).Check(ctx)
}

// hasDocsDir reports whether docs/ exists as a directory in the package root.
func hasDocsDir(path string) bool {
	info, err := os.Stat(filepath.Join(path, "docs"))
	return err == nil && info.IsDir()
}
