package verify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Masterminds/semver/v3"
	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/find"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/imagefs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/registry"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/openapi"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	pkglint "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/package"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/settings"
)

const (
	// releaseRepoSuffix is appended to the package repository to address the release
	// image. It tracks the layout build publishes to: the bundle image is the
	// repository itself, and the release image sits one level below it.
	releaseRepoSuffix = "version"

	// templatesDirName is the package subdirectory holding renderable Helm templates.
	// The release image ships without it, which is what makes rendering conditional.
	templatesDirName = "templates"
)

// linter is the common interface implemented by all lint passes.
type linter interface {
	Lint(ctx context.Context)
}

// ErrVerifyFailed is returned by Verify when one or more linters report errors.
var ErrVerifyFailed = errors.New("verify failed")

// Options controls what is verified and which diagnostic severities are surfaced.
type Options struct {
	HideWarnings bool         // suppress Warn-level diagnostics from output
	ShowIgnored  bool         // include Ignored-level diagnostics in output
	LintConfig   string       // lint config path to use instead of package-relative discovery
	Remote       RemoteTarget // published package to verify instead of the current directory
}

// RemoteTarget addresses a published package version. Build publishes two images per
// version under the same tag, so one version selects both: the bundle image is always
// verified, and Release opts the release image in alongside it.
type RemoteTarget struct {
	// Repository is the package repository path, <registry-path>/<package>, without a tag.
	Repository string
	// Version tags both images and identifies the published version to verify.
	// Empty resolves to the highest version published in the repository.
	Version string
	// Release additionally verifies the release image published for Version.
	Release bool
}

// Enabled reports whether remote verification was requested.
func (t RemoteTarget) Enabled() bool {
	return t.Repository != ""
}

// Validate reports whether the target names a repository. The version is optional:
// an unset one is resolved from the tags the repository publishes.
func (t RemoteTarget) Validate() error {
	if t.Repository == "" {
		return errors.New("repository is required")
	}

	return nil
}

// BundleRef returns the reference of the bundle image, <repository>:<version>.
func (t RemoteTarget) BundleRef() string {
	return fmt.Sprintf("%s:%s", t.Repository, t.Version)
}

// ReleaseRef returns the reference of the release image, <repository>/version:<version>.
func (t RemoteTarget) ReleaseRef() string {
	return fmt.Sprintf("%s/%s:%s", t.Repository, releaseRepoSuffix, t.Version)
}

// Verify runs all linters against the selected package sources and prints the results.
// By default it verifies the current package directory using the static settings scope;
// when Options.Remote is set, it verifies whichever published images that target selects,
// each against its own remote settings scope.
func Verify(ctx context.Context, opts Options) error {
	collector := diag.NewCollector()

	var err error
	if opts.Remote.Enabled() {
		err = verifyRemote(ctx, opts, collector)
	} else {
		err = verifyStatic(ctx, opts, collector)
	}

	if err != nil {
		return err
	}

	collector.Print(opts.ShowIgnored, opts.HideWarnings)

	if collector.HasErrors() {
		return ErrVerifyFailed
	}

	return nil
}

// verifyStatic lints the package directory discovered from the current working directory.
func verifyStatic(ctx context.Context, opts Options, collector *diag.Collector) error {
	path, err := find.PackageDir()
	if err != nil {
		return fmt.Errorf("find package dir: %w", err)
	}

	set, err := settings.Load(path, opts.LintConfig)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	return lintPath(ctx, path, lint.ScopeStatic, set.Scope(lint.ScopeStatic), collector)
}

// verifyRemote lints the selected images of a published package. Every selected image
// is checked for existence before any of them is pulled, so a typo in one reference
// fails fast instead of after a needless download; the images are then extracted and
// linted concurrently into the shared collector.
func verifyRemote(ctx context.Context, opts Options, collector *diag.Collector) error {
	if err := opts.Remote.Validate(); err != nil {
		return err
	}

	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	// Remote verification reads its config from the caller's tree, not from the
	// image: published images do not carry a .pkglint.yaml.
	set, err := settings.Load(dir, opts.LintConfig)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	target, err := resolveTarget(ctx, opts.Remote)
	if err != nil {
		return err
	}

	sources := remoteSources(target, set)

	if err = ensureExists(ctx, sources); err != nil {
		return err
	}

	eg, egCtx := errgroup.WithContext(ctx)

	for _, src := range sources {
		eg.Go(func() error {
			return lintRemoteSource(egCtx, src, collector)
		})
	}

	return eg.Wait()
}

// resolveTarget fills in the version when the caller left it out, so a target always
// names a concrete published version by the time its image references are built.
func resolveTarget(ctx context.Context, target RemoteTarget) (RemoteTarget, error) {
	if target.Version != "" {
		return target, nil
	}

	version, err := latestVersion(ctx, target.Repository)
	if err != nil {
		return RemoteTarget{}, err
	}

	fmt.Fprintf(os.Stderr, "No version given, verifying the latest published version %s\n", version)

	target.Version = version

	return target, nil
}

// latestVersion returns the highest version published in repository. Tags that are not
// valid semver are skipped rather than compared as strings, so a channel alias or an
// index marker sharing the repository cannot be mistaken for a version. The tag is
// returned verbatim, because the published tag is what the image reference needs.
func latestVersion(ctx context.Context, repository string) (string, error) {
	tags, err := registry.Tags(ctx, repository)
	if err != nil {
		return "", fmt.Errorf("list published versions: %w", err)
	}

	var (
		highest *semver.Version
		latest  string
	)

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		if highest == nil || version.GreaterThan(highest) {
			highest, latest = version, tag
		}
	}

	if latest == "" {
		return "", fmt.Errorf("no published versions found in %q", repository)
	}

	return latest, nil
}

// remoteSources returns one source per image the target selects, in bundle-then-release
// order. The bundle image carries the package itself and is always verified; the release
// image is opted in, because a caller checking a version usually cares about the bundle.
func remoteSources(target RemoteTarget, set *settings.Settings) []remoteSource {
	sources := make([]remoteSource, 0, 2)

	sources = append(sources, remoteSource{
		scope:    lint.ScopeBundle,
		ref:      target.BundleRef(),
		settings: set.Scope(lint.ScopeBundle),
	})

	if target.Release {
		sources = append(sources, remoteSource{
			scope:    lint.ScopeRelease,
			ref:      target.ReleaseRef(),
			settings: set.Scope(lint.ScopeRelease),
		})
	}

	return sources
}

// remoteSource pairs an image reference with the scope that governs it: the scope both
// selects which linters and rules are processed and which severities apply.
type remoteSource struct {
	// scope names the verification target and labels diagnostics from this image.
	scope lint.Scope
	// ref is the fully qualified image reference.
	ref string
	// settings holds the runtime linter severities for this image.
	settings *settings.Root
}

// ensureExists checks every source manifest concurrently and reports all missing
// images at once, rather than surfacing only the first failure.
func ensureExists(ctx context.Context, sources []remoteSource) error {
	errs := make([]error, len(sources))

	var wg sync.WaitGroup

	for i, src := range sources {
		wg.Go(func() {
			if err := registry.Exists(ctx, src.ref); err != nil {
				errs[i] = fmt.Errorf("%s image: %w", src.scope, err)
			}
		})
	}

	wg.Wait()

	return errors.Join(errs...)
}

// lintRemoteSource extracts one image into a temp directory and lints it, tagging every
// finding with the scope name so concurrent results stay attributable.
func lintRemoteSource(ctx context.Context, src remoteSource, collector *diag.Collector) error {
	path, err := imagefs.ExtractToTemp(ctx, src.ref)
	if err != nil {
		return fmt.Errorf("extract %s image: %w", src.scope, err)
	}

	defer func() {
		if removeErr := os.RemoveAll(path); removeErr != nil {
			fmt.Fprintf(os.Stderr, "WARN remove %s temp directory: %s\n", src.scope, removeErr)
		}
	}()

	if err = lintPath(ctx, path, src.scope, src.settings, collector.With(diag.Source(string(src.scope)))); err != nil {
		return fmt.Errorf("verify %s image: %w", src.scope, err)
	}

	return nil
}

// lintPath builds and runs the linters that apply to scope against path, recording
// findings in collector.
func lintPath(ctx context.Context, path string, scope lint.Scope, root *settings.Root, collector *diag.Collector) error {
	collector = collector.With(diag.RootPath(path))

	linters, err := buildLinters(ctx, root, path, scope, collector)
	if err != nil {
		return fmt.Errorf("build linters: %w", err)
	}

	for _, l := range linters {
		l.Lint(ctx)
	}

	return nil
}

// buildLinters loads the package definition and constructs the linters processed for the
// resulting target. A linter whose declared targets exclude this one is never built, so
// it neither runs nor reports — unlike an ignored impact, which silences a linter that ran.
func buildLinters(ctx context.Context, root *settings.Root, path string, scope lint.Scope, collector *diag.Collector) ([]linter, error) {
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

	target := lint.Target{Type: lint.PackageType(def.Type), Scope: scope}
	collector = collector.With(diag.PackageID(def.Name))

	linters := make([]linter, 0, 7)

	if pkglint.Scopes.Contains(target) {
		linters = append(linters, pkglint.NewLinter(pkglint.Config{
			Definition: def,
			Path:       path,
			Target:     target,
		}, collector))
	}

	if openapi.Scopes.Contains(target) {
		linters = append(linters, openapi.NewLinter(openapi.Config{
			Path: path,
		}, collector))
	}

	if templates.Scopes.Contains(target) {
		rendered, err := renderTemplates(ctx, def, path)
		if err != nil {
			return nil, err
		}

		linters = append(linters, templates.NewLinter(templates.Config{
			Settings: root.Templates,
			Rendered: rendered,
			Target:   target,
		}, collector))
	}

	if docs.Scopes.Contains(target) {
		linters = append(linters, docs.NewLinter(docs.Config{
			Settings: root.Documentation,
			Path:     path,
		}, collector))
	}

	if images.Scopes.Contains(target) {
		linters = append(linters, images.NewLinter(images.Config{
			Settings: root.Images,
			Path:     path,
		}, collector))
	}

	if icon.Scopes.Contains(target) {
		linters = append(linters, icon.NewLinter(icon.Config{
			Settings: root.Icon,
			Path:     path,
		}, collector))
	}

	if oss.Scopes.Contains(target) {
		linters = append(linters, oss.NewLinter(oss.Config{
			Settings: root.OSS,
			Path:     path,
		}, collector))
	}

	return linters, nil
}

// renderTemplates renders the package manifests. Rendering only happens for targets the
// templates linter is processed in, but a source tree can still legitimately lack
// templates/, so an absent directory yields no objects rather than an error.
func renderTemplates(ctx context.Context, def packages.Definition, path string) ([]render.Object, error) {
	if _, err := os.Stat(filepath.Join(path, templatesDirName)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("stat templates dir: %w", err)
	}

	rendered, err := packages.Render(ctx, def, path)
	if err != nil {
		return nil, fmt.Errorf("render templates: %w", err)
	}

	return rendered, nil
}
