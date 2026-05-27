// Package oss validates optional oss.yaml package metadata.
package oss

import (
	"context"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss/model"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss/rules"
)

const (
	// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
	LinterID = "oss"
	// ossFile is the package metadata file describing bundled open source software.
	ossFile = "oss.yaml"
)

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

// RulesSettings holds the severity configuration for each rule in the oss linter.
type RulesSettings struct {
	Parse   lint.RuleSettings
	Fields  lint.RuleSettings
	Version lint.RuleSettings
}

// Linter runs oss.yaml rules against a package directory.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// NewLinter constructs a Linter from cfg, scoping its diagnostics to this linter and capping severity at the configured level.
func NewLinter(cfg Config, res *diag.Collector) *Linter {
	return &Linter{
		settings: cfg.Settings.RulesSettings,
		config:   cfg,
		collector: res.With(
			diag.LinterID(LinterID),
			diag.Path(ossFile),
			diag.MaxLevel(cfg.Settings.LinterSettings.Impact)),
	}
}

// Lint executes all oss rules against the configured package path.
func (l *Linter) Lint(ctx context.Context) {
	components, ok := l.loadComponents()
	if !ok {
		return
	}

	rules.NewFieldsRule(components, l.collector.With(diag.MaxLevel(l.settings.Fields.Impact))).Check(ctx)
	rules.NewVersionRule(components, l.collector.With(diag.MaxLevel(l.settings.Version.Impact))).Check(ctx)
}

// loadComponents reads and parses oss.yaml. It returns ok=false when the file is absent or invalid.
func (l *Linter) loadComponents() ([]model.Component, bool) {
	path := filepath.Join(l.config.Path, ossFile)

	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, false
	}

	if err != nil {
		l.collector.Error("failed to read %s: %v", ossFile, err)
		return nil, false
	}

	var components []model.Component
	if err := yaml.Unmarshal(raw, &components); err != nil {
		l.collector.With(diag.MaxLevel(l.settings.Parse.Impact)).Error("failed to parse %s: %v", ossFile, err)
		return nil, false
	}

	if components == nil {
		components = []model.Component{}
	}

	return components, true
}
