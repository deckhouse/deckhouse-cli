// Package openapi validates the OpenAPI schemas a package ships under openapi/. Its
// rules encode hard schema contracts rather than preferences, so the linter carries no
// .pkglint.yaml settings and its rules always report at their built-in severity.
package openapi

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/openapi/rules"
)

// LinterID is the stable identifier used to reference this linter in diagnostics.
const LinterID = "openapi"

// Scopes lists the verification targets this linter is processed in. openapi/ is packaged
// into both images, so the schemas are checked everywhere they ship.
var Scopes = lint.EveryType(lint.AllScopes...)

// Config holds the path required to construct a Linter.
type Config struct {
	Path string
}

// NewLinter constructs a Linter from cfg, scoping its diagnostics to this linter.
func NewLinter(cfg Config, res *diag.Collector) *Linter {
	return &Linter{
		config:    cfg,
		collector: res.With(diag.LinterID(LinterID)),
	}
}

// Linter runs OpenAPI schema rules against a package directory.
type Linter struct {
	config Config

	collector *diag.Collector
}

// Lint executes the openapi rules against the configured package path.
func (l *Linter) Lint(ctx context.Context) {
	rules.NewAdvancedRule(l.config.Path, l.collector).Check(ctx)
}
