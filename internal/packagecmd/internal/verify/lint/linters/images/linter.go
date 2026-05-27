package images

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "images"

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

// RulesSettings holds the severity configuration for each rule in the images linter.
type RulesSettings struct {
	Patches lint.RuleSettings
}

// NewLinter constructs a Linter from cfg, scoping its diagnostics to this linter and capping severity at the configured level.
func NewLinter(cfg Config, res *diag.Collector) *Linter {
	return &Linter{
		settings: cfg.Settings.RulesSettings,
		config:   cfg,
		collector: res.With(
			diag.LinterID(LinterID),
			diag.MaxLevel(cfg.Settings.LinterSettings.Impact)),
	}
}

// Linter runs image-related rules against a package directory.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// Lint executes all image rules against the configured package path.
func (l *Linter) Lint(ctx context.Context) {
	if !hasImagesDir(l.config.Path) {
		l.collector.With(diag.MaxLevel(lint.Ignored.Ptr())).Warn("images not found in path")
		return
	}

	rules.NewPatchesRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Patches.Impact))).Check(ctx)
}

// hasImagesDir reports whether images/ exists as a directory in the package root.
func hasImagesDir(path string) bool {
	info, err := os.Stat(filepath.Join(path, "images"))
	return err == nil && info.IsDir()
}
