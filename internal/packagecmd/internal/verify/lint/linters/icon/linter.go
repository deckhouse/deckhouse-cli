// Package icon validates package-icon content (format, size, dimensions). The
// companion layout linter owns the existence check; this linter silently
// no-ops at Ignored level when no icon is present.
package icon

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "icon"

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

// RulesSettings holds the severity configuration for each rule in the icon linter.
type RulesSettings struct {
	Ext   lint.RuleSettings
	Size  lint.RuleSettings
	Shape lint.RuleSettings
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

// Linter runs icon content rules against a package directory.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// Lint discovers the package icon once and runs each rule against the captured
// Icon value. Existence of the icon file itself is the layout linter's concern;
// when no icon is present, this linter logs an Ignored-level note and exits.
func (l *Linter) Lint(ctx context.Context) {
	icon, err := iconutil.Find(l.config.Path)
	if errors.Is(err, iconutil.ErrNoIcon) {
		l.collector.With(diag.MaxLevel(lint.Ignored.Ptr())).Warn("icon not found in package root")
		return
	}

	// Scope rule diagnostics to the icon file path once at the linter level so
	// individual rules don't repeat diag.Path on every emit.
	collector := l.collector.With(diag.Path(filepath.Base(icon.Path)))

	if err != nil {
		collector.Error("%v", err)
		return
	}

	rules.NewExtRule(icon, collector.With(diag.MaxLevel(l.settings.Ext.Impact))).Check(ctx)
	rules.NewSizeRule(icon, collector.With(diag.MaxLevel(l.settings.Size.Impact))).Check(ctx)
	rules.NewShapeRule(icon, collector.With(diag.MaxLevel(l.settings.Shape.Impact))).Check(ctx)
}
