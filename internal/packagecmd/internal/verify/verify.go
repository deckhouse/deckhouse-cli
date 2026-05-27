package verify

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/find"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/imagefs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/layout"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/settings"
)

// linter is the common interface implemented by all lint passes.
type linter interface {
	Lint(ctx context.Context)
}

// ErrVerifyFailed is returned by Verify when one or more linters report errors.
var ErrVerifyFailed = errors.New("verify failed")

// Options controls which diagnostic severities are surfaced in the output.
type Options struct {
	HideWarnings bool   // suppress Warn-level diagnostics from output
	ShowIgnored  bool   // include Ignored-level diagnostics in output
	Remote       string // registry reference to verify instead of the current package directory
	LintConfig   string // lint config path to use instead of package-relative discovery
}

// Verify runs all linters against the selected package source and prints the results.
// By default it verifies the current package directory; when Options.Remote is set,
// it verifies the filesystem extracted from that image reference.
func Verify(ctx context.Context, opts Options) error {
	var (
		path string
		err  error
	)

	if len(opts.Remote) > 0 {
		// Remote verification uses the extracted image filesystem as the package root.
		path, err = imagefs.ExtractToTemp(ctx, opts.Remote)
		if err != nil {
			return fmt.Errorf("extract image to temp: %w", err)
		}
		defer os.RemoveAll(path)
	} else {
		path, err = find.PackageDir()
		if err != nil {
			return fmt.Errorf("find package dir: %w", err)
		}
	}

	root, err := settings.LoadRoot(path, opts.LintConfig)
	if err != nil {
		return fmt.Errorf("load root settings: %w", err)
	}

	collector := diag.NewCollector().With(diag.RootPath(path))

	linters, err := buildLinters(ctx, root, path, collector)
	if err != nil {
		return fmt.Errorf("build linters: %w", err)
	}

	for _, l := range linters {
		l.Lint(ctx)
	}

	collector.Print(opts.ShowIgnored, opts.HideWarnings)

	if collector.HasErrors() {
		return ErrVerifyFailed
	}

	return nil
}

// buildLinters loads the package definition and settings from path and constructs the list of linters to run.
func buildLinters(ctx context.Context, root *settings.Root, path string, collector *diag.Collector) ([]linter, error) {
	def, err := packages.LoadDefinitionByDir(path)
	if err != nil {
		return nil, fmt.Errorf("load definition: %w", err)
	}

	if def.Type != packages.TypeApplication {
		return nil, errors.New("unsupported package type")
	}

	if def.Name == "" {
		return nil, errors.New("package name is required")
	}

	rendered, err := packages.Render(ctx, def, path)
	if err != nil {
		return nil, fmt.Errorf("render templates: %w", err)
	}

	collector = collector.With(diag.PackageID(def.Name))

	layoutLinter := layout.NewLinter(layout.Config{
		Settings: root.Layout,
		Path:     path,
	}, collector)

	templatesLinter := templates.NewLinter(templates.Config{
		Settings: root.Templates,
		Rendered: rendered,
	}, collector)

	docsLinter := docs.NewLinter(docs.Config{
		Settings: root.Documentation,
		Path:     path,
	}, collector)

	imagesLinter := images.NewLinter(images.Config{
		Settings: root.Images,
		Path:     path,
	}, collector)

	iconLinter := icon.NewLinter(icon.Config{
		Settings: root.Icon,
		Path:     path,
	}, collector)

	ossLinter := oss.NewLinter(oss.Config{
		Settings: root.OSS,
		Path:     path,
	}, collector)

	return []linter{
		layoutLinter,
		templatesLinter,
		docsLinter,
		imagesLinter,
		iconLinter,
		ossLinter,
	}, nil
}
