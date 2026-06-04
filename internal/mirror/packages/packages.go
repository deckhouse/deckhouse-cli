/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package packages mirrors Deckhouse "packages", which are structurally
// identical to modules but live under a different set of registry segments:
//
//	<root>/packages/<name>:<version>             - package main image
//	<root>/packages/<name>/version:<channel>     - package version-channel metadata
//	<root>/packages/<name>/version:<vX.Y.Z>      - package version-tagged release metadata
//	<root>/packages/<name>/extra/<extra>:<tag>   - package extra images
//
// The pull pipeline, filtering and version selection are intentionally the
// same as for modules, so the generic version-selection vocabulary
// (Filter, Module, VersionConstraint, ProbeAvailableVersions) is reused from
// the modules package rather than duplicated here.
package packages

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/errmatch"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/pack"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Options contains configuration options for the packages service.
type Options struct {
	// Filter is the package filter (whitelist/blacklist). It reuses the
	// modules filter because package selection works exactly like module
	// selection (names + semver constraints).
	Filter *modules.Filter
	// OnlyExtraImages pulls only extra images without main package images
	OnlyExtraImages bool
	// SkipVexImages allows skipping VEX images
	SkipVexImages bool
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
	// Timeout is the timeout for the packages access check
	Timeout time.Duration
	// DryRun prints the pull plan without downloading any image blobs
	DryRun bool
	// ProxyRegistry replaces catalog-based discovery with a sequential probe
	// of individual version tags derived from the user's --include-package
	// version constraint. See the modules service for the full rationale.
	ProxyRegistry bool
}

type Service struct {
	workingDir string

	// packagesService handles Deckhouse packages registry operations
	packagesService *registryservice.PackagesService
	// layout manages the OCI image layouts for different components
	layout *PackagesImageLayouts
	// packagesDownloadList manages the list of images to be downloaded
	packagesDownloadList *PackagesDownloadList
	// pullerService handles the pulling of images
	pullerService *puller.PullerService

	// options contains service configuration
	options *Options

	// packageStats accumulates per-package pull accounting, keyed by package name.
	// See packagePullStat for what each field holds and when it is filled.
	packageStats map[packageName]packagePullStat

	// rootURL is the base registry URL for packages images
	rootURL string

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewService(
	registryService *registryservice.Service,
	workingDir string,
	options *Options,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.Infof("Creating OCI Image Layouts for Packages")

	if options == nil {
		options = &Options{}
	}

	// Create default filter (blacklist with no items = accept all)
	if options.Filter == nil {
		filter, _ := modules.NewFilter(nil, modules.FilterTypeBlacklist)
		options.Filter = filter
	}

	// rootURL must include the edition segment (e.g. .../deckhouse/fe) so that
	// per-package references like <rootURL>/packages/<name>:<tag> resolve to
	// the same path served by registryService.PackageService().
	rootURL := registryService.GetEditionRoot()

	return &Service{
		workingDir:           workingDir,
		packagesService:      registryService.PackageService(),
		packagesDownloadList: NewPackagesDownloadList(rootURL),
		pullerService:        puller.NewPullerService(logger, userLogger),
		options:              options,
		packageStats:         make(map[packageName]packagePullStat),
		rootURL:              rootURL,
		logger:               logger,
		userLogger:           userLogger,
	}
}

// PullPackages pulls the Deckhouse packages.
// It validates access to the registry and pulls the package images.
func (svc *Service) PullPackages(ctx context.Context) error {
	err := svc.validatePackagesAccess(ctx)
	if err != nil {
		return fmt.Errorf("validate packages access: %w", err)
	}

	err = svc.pullPackages(ctx)
	if err != nil {
		return fmt.Errorf("pull packages: %w", err)
	}

	return nil
}

// PullPackageVersions pulls the release/version images of every package and
// packs them into a single shared archive (package-versions.tar).
//
// Unlike PullPackages this runs on EVERY mirror operation, independently of
// which components are being mirrored and of the --no-packages flag, so the
// package release-image catalog is always cloned into the bundle. Release
// images may therefore be duplicated between this archive and the per-package
// archives produced by PullPackages — that duplication is intentional.
func (svc *Service) PullPackageVersions(ctx context.Context) error {
	logger := svc.userLogger

	names, err := svc.discoverPackageNames(ctx)
	if err != nil {
		// A registry without a packages catalog must never break the rest of
		// the mirror operation: just skip the package-versions archive.
		logger.Warnf("Skipping package release images (package-versions): %v", err)
		return nil
	}

	if len(names) == 0 {
		return nil
	}

	if svc.options.DryRun {
		logger.InfoLn("[dry-run] package-versions archive would contain release images for: " + strings.Join(names, ", "))
		return nil
	}

	// Dedicated working dir so this never collides with the per-package pull
	// working dir (".../packages") used by PullPackages.
	versionsRoot := filepath.Join(svc.workingDir, "package-versions")

	sources := make([]bundle.PackSource, 0, len(names))

	err = logger.Process("Pull Package Release Images", func() error {
		for i, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}

			logger.Infof("[%d/%d] Pulling release images for package: %s", i+1, len(names), name)

			versionDir := filepath.Join(versionsRoot, name, internal.PackagesVersionSegment)

			versionLayout, err := regimage.NewImageLayout(versionDir)
			if err != nil {
				return fmt.Errorf("create version layout for package %s: %w", name, err)
			}

			if err := svc.pullAllVersionImages(ctx, name, versionLayout); err != nil {
				if isContextErr(err) {
					return err
				}

				logger.Warnf("Failed to pull release images for package %s: %v", name, err)

				continue
			}

			if err := layouts.SortIndexManifests(versionLayout.Path()); err != nil {
				return fmt.Errorf("sort index manifests for package %s: %w", name, err)
			}

			// Skip packages whose version repo produced no images.
			if !LayoutHasManifests(versionLayout.Path()) {
				continue
			}

			sources = append(sources, bundle.PackSource{
				Dir:    versionDir,
				Prefix: filepath.Join(internal.PackagesSegment, name, internal.PackagesVersionSegment),
			})
		}

		return nil
	})
	if err != nil {
		return err
	}

	if len(sources) == 0 {
		logger.InfoLn("No package release images found, skipping package-versions archive")
		return nil
	}

	return logger.Process("Pack package-versions.tar", func() error {
		return pack.Bundle(ctx, svc.options.BundleDir, "package-versions.tar", svc.options.BundleChunkSize, func(w io.Writer) error {
			return bundle.PackSourcesWithPrefix(ctx, w, sources...)
		})
	})
}

// pullAllVersionImages pulls every release/version image for a single package
// (default release channels, the LTS channel when present, and the version
// tags advertised by those channels) into the provided layout.
func (svc *Service) pullAllVersionImages(ctx context.Context, packageName string, versionLayout *regimage.ImageLayout) error {
	pkgSvc := svc.packagesService.Package(packageName)

	imageSet := make(map[string]*puller.ImageMeta)

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		imageSet[svc.versionRef(packageName, channel)] = nil
	}

	if err := pkgSvc.VersionChannels().CheckImageExists(ctx, internal.LTSChannel); err == nil {
		imageSet[svc.versionRef(packageName, internal.LTSChannel)] = nil
	}

	// Version tags (vX.Y.Z) advertised by the channels' version.json.
	for _, version := range svc.extractVersionsFromVersionChannels(ctx, packageName) {
		imageSet[svc.versionRef(packageName, version)] = nil
	}

	// Every tag served under packages/<name>/version, so that all version
	// images are cloned regardless of the --include-package filter and even
	// when the package publishes no release channels (e.g. dev packages that
	// only have a packages/<name>/version:vX.Y.Z tag). Channel-only discovery
	// above would otherwise miss those and skip the package entirely.
	for _, tag := range svc.listAllVersionTags(ctx, packageName) {
		imageSet[svc.versionRef(packageName, tag)] = nil
	}

	if len(imageSet) == 0 {
		return nil
	}

	config := puller.PullConfig{
		Name:             packageName + " release images",
		ImageSet:         imageSet,
		Layout:           versionLayout,
		AllowMissingTags: true,
		GetterService:    pkgSvc.VersionChannels(),
	}

	return svc.pullerService.PullImages(ctx, config)
}

// listAllVersionTags enumerates every tag published under
// packages/<name>/version (both release channels and vX.Y.Z version tags).
//
// The package-versions archive must clone all version images independently of
// the --include-package filter, so this intentionally does not consult the
// filter constraint. A registry that refuses to enumerate the version repo
// (missing repo or a proxy registry without catalog support) is not fatal: the
// channel-based discovery in pullAllVersionImages still applies and we just
// skip the extra tags here.
func (svc *Service) listAllVersionTags(ctx context.Context, packageName string) []string {
	tags, err := svc.packagesService.Package(packageName).VersionChannels().ListTags(ctx)
	if err != nil {
		svc.logger.Debug(fmt.Sprintf("Failed to list version tags for package %s: %v", packageName, err))
		return nil
	}

	return tags
}

// validatePackagesAccess validates access to the packages registry.
func (svc *Service) validatePackagesAccess(ctx context.Context) error {
	svc.logger.Debug("Validating access to the packages registry")

	// Proxy registries typically refuse the catalog API entirely. The CLI has
	// already required --include-package so we know exactly which packages to
	// probe, and per-tag CheckImageExists/GetImage calls work fine against a
	// proxy.
	if svc.options.ProxyRegistry {
		return nil
	}

	// A registry without a packages catalog reports it as either ErrImageNotFound
	// or a NAME_UNKNOWN transport error (the public registry returns the latter).
	// Both mean "no packages here": skip the phase instead of failing the pull,
	// matching the graceful skip in PullPackageVersions.
	_, err := svc.packagesService.ListTags(ctx)
	if errors.Is(err, client.ErrImageNotFound) || errmatch.IsRepoNotFound(err) {
		svc.userLogger.Warnf("Skipping pull of packages: %v", err)

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to list packages from registry: %w", err)
	}

	return nil
}

// packageData represents a package with its metadata.
type packageData struct {
	name         string
	registryPath string
}

func (svc *Service) pullPackages(ctx context.Context) error {
	logger := svc.userLogger

	// Temporary workspace for package OCI layouts.
	tmpDir := filepath.Join(svc.workingDir, internal.PackagesSegment)

	packageNames, err := svc.discoverPackageNames(ctx)
	if err != nil {
		return err
	}

	if len(packageNames) == 0 {
		logger.WarnLn("Packages were not found, check your source repository address and packages path suffix")
		return nil
	}

	// Filter-out packages that are not allowed by the filter (blacklist or whitelist)
	filteredPackages := make([]packageData, 0)

	for _, packageName := range packageNames {
		pkg := &modules.Module{
			Name:         packageName,
			RegistryPath: filepath.Join(svc.rootURL, internal.PackagesSegment, packageName),
		}
		if svc.options.Filter.Match(pkg) {
			filteredPackages = append(filteredPackages, packageData{
				name:         packageName,
				registryPath: pkg.RegistryPath,
			})
			logger.Infof("Package found: %s", packageName)
		} else {
			logger.Debugf("Package %s filtered out", packageName)
		}
	}

	if len(filteredPackages) == 0 {
		logger.WarnLn("No packages matched the filter criteria")
		return nil
	}

	logger.Infof("Repo contains %d packages to pull", len(filteredPackages))

	packageImagesLayout, err := createOCIImageLayoutsForPackages(tmpDir, getPackageNames(filteredPackages))
	if err != nil {
		return fmt.Errorf("create OCI image layouts for packages: %w", err)
	}

	svc.layout = packageImagesLayout

	processName := "Pull Packages"
	if svc.options.OnlyExtraImages {
		processName = "Pull Extra Images"
	}

	// pullCancelled is set when the user interrupts mid-pull. In that case we
	// still want the post-processing + packing phase to finalize whatever was
	// successfully downloaded so far.
	pullCancelled := false

	err = logger.Process(processName, func() error {
		for i, pkg := range filteredPackages {
			if err := ctx.Err(); err != nil {
				logger.Warnf("Pull cancelled; %d/%d packages attempted, will pack already-downloaded packages", i, len(filteredPackages))

				pullCancelled = true

				return nil
			}

			logger.Infof("[%d/%d] Processing package: %s", i+1, len(filteredPackages), pkg.name)

			if err := svc.pullSinglePackage(ctx, pkg); err != nil {
				if isContextErr(err) {
					logger.Warnf("Pull of package %s cancelled, will pack already-downloaded packages", pkg.name)

					pullCancelled = true

					return nil
				}

				return fmt.Errorf("pull package %s: %w", pkg.name, err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Skip OCI layout post-processing in dry-run (layouts are empty)
	if svc.options.DryRun {
		return nil
	}

	// Decouple the post-pull phase from the cancellation context so that the
	// last bit of packing finalizes for the packages that *did* download.
	postCtx := ctx
	if pullCancelled {
		postCtx = context.WithoutCancel(ctx)
	}

	err = logger.Process("Processing packages image indexes", func() error {
		for _, l := range svc.layout.AsList() {
			err = layouts.SortIndexManifests(l)
			if err != nil {
				return fmt.Errorf("sorting index manifests of %s: %w", l, err)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("processing packages image indexes: %w", err)
	}

	// Apply channel aliases if needed (not for OnlyExtraImages mode)
	if !svc.options.OnlyExtraImages {
		for _, pkg := range filteredPackages {
			if err := svc.applyChannelAliases(pkg.name); err != nil {
				return fmt.Errorf("apply channel aliases for package %s: %w", pkg.name, err)
			}
		}
	}

	// Capture per-package manifest counts before packing: bundle.Pack deletes
	// every layout file as it tars it, so counting after the pack step would
	// read emptied layouts and report zero.
	svc.capturePulledImages(filteredPackages)

	// Pack each package into separate tar
	if err := svc.packPackages(postCtx, filteredPackages); err != nil {
		return err
	}

	return nil
}

// isContextErr reports whether err is one of the context cancellation errors,
// either directly or wrapped.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (svc *Service) pullSinglePackage(ctx context.Context, pkg packageData) error {
	downloadList := NewImageDownloadList(filepath.Join(svc.rootURL, internal.PackagesSegment, pkg.name))
	svc.packagesDownloadList.list[pkg.name] = downloadList

	channelVersions, err := svc.discoverChannelVersions(ctx, pkg.name, downloadList)
	if err != nil {
		return err
	}

	tags, err := svc.listTagsIfConstrained(ctx, pkg.name)
	if err != nil {
		return err
	}

	packageVersions := svc.mergeAndDedupeVersions(pkg.name, pkg.registryPath, channelVersions, tags)

	// Record the resolved versions for the summary. Happens before download, so
	// it is populated in dry-run too.
	stat := svc.packageStats[pkg.name]
	stat.versions = packageVersions
	svc.packageStats[pkg.name] = stat

	if svc.options.DryRun {
		svc.printDryRunPlan(pkg.name, downloadList, packageVersions)

		// Record the planned version images so the end-of-pull summary counts
		// them, mirroring the references printDryRunPlan prints. Extra images are
		// not resolved in dry-run (they require a real pull), so the per-package
		// count stays "version channels + versions".
		for _, version := range packageVersions {
			downloadList.Package[svc.packageRef(pkg.name, version)] = nil
		}

		return nil
	}

	if !svc.options.OnlyExtraImages {
		if err := svc.pullPackageImages(ctx, pkg.name, packageVersions, downloadList); err != nil {
			return err
		}

		svc.pullVersionReleaseImages(ctx, pkg.name, packageVersions, downloadList)
		svc.pullInternalDigestImages(ctx, pkg.name, packageVersions, downloadList)
	}

	if err := svc.pullExtraImages(ctx, pkg.name, packageVersions, downloadList); err != nil {
		return err
	}

	if !svc.options.SkipVexImages {
		svc.pullVexImages(ctx, pkg.name, downloadList)
	}

	return nil
}

// discoverChannelVersions enqueues version-channel refs into downloadList,
// optionally pulls them (skipped on DryRun), and returns versions extracted
// from those channels.
func (svc *Service) discoverChannelVersions(ctx context.Context, packageName string, downloadList *ImageDownloadList) ([]string, error) {
	if !svc.options.Filter.ShouldMirrorReleaseChannels(packageName) || svc.options.OnlyExtraImages {
		return nil, nil
	}

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		downloadList.PackageVersionChannels[svc.versionRef(packageName, channel)] = nil
	}

	// Add LTS channel if it exists
	if err := svc.packagesService.Package(packageName).VersionChannels().CheckImageExists(ctx, internal.LTSChannel); err == nil {
		downloadList.PackageVersionChannels[svc.versionRef(packageName, internal.LTSChannel)] = nil
	}

	if !svc.options.DryRun {
		config := puller.PullConfig{
			Name:             packageName + " version channels",
			ImageSet:         downloadList.PackageVersionChannels,
			Layout:           svc.layout.Package(packageName).PackageVersionChannels,
			AllowMissingTags: true,
			GetterService:    svc.packagesService.Package(packageName).VersionChannels(),
		}

		if err := svc.pullerService.PullImages(ctx, config); err != nil {
			return nil, fmt.Errorf("pull version channels: %w", err)
		}
	}

	// Calls GetImage() directly against the remote registry, not from the local
	// OCI layout, so it works in DryRun too.
	return svc.extractVersionsFromVersionChannels(ctx, packageName), nil
}

// discoverPackageNames returns the list of package names this run should
// consider. The behaviour mirrors the modules service: ListTags on the
// packages root by default, or the whitelist filter when --proxy-registry
// is set.
func (svc *Service) discoverPackageNames(ctx context.Context) ([]string, error) {
	if svc.options.ProxyRegistry {
		if svc.options.Filter == nil || !svc.options.Filter.IsWhitelist() {
			return nil, fmt.Errorf("--proxy-registry requires a whitelist of packages (--include-package)")
		}

		return svc.options.Filter.ModuleNames(), nil
	}

	packageNames, err := svc.packagesService.ListTags(ctx)
	if err != nil {
		// A missing packages repository (ErrImageNotFound, or a NAME_UNKNOWN
		// transport error from the public registry) means there are no packages
		// to mirror: report an empty set rather than failing the pull.
		if errors.Is(err, client.ErrImageNotFound) || errmatch.IsRepoNotFound(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("list packages: %w", err)
	}

	return packageNames, nil
}

// listTagsIfConstrained returns the package's tag list, but only when the
// filter has a non-exact (semver) constraint.
func (svc *Service) listTagsIfConstrained(ctx context.Context, packageName string) ([]string, error) {
	constraint, hasConstraint := svc.options.Filter.GetConstraint(packageName)
	if !hasConstraint || constraint.IsExact() {
		return nil, nil
	}

	if svc.options.ProxyRegistry {
		return svc.probePackageTags(ctx, packageName, constraint)
	}

	tags, err := svc.packagesService.Package(packageName).ListTags(ctx)
	switch {
	case errors.Is(err, client.ErrImageNotFound):
		svc.userLogger.Warnf("Skipping tag list for package %s: %v", packageName, err)
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("list tags for package %s: %w", packageName, err)
	}

	return tags, nil
}

// probePackageTags walks tags for a single package via HEAD requests instead
// of asking the registry to enumerate them.
func (svc *Service) probePackageTags(ctx context.Context, packageName string, constraint modules.VersionConstraint) ([]string, error) {
	semverConstraints := modules.SemverConstraintsOf(constraint)
	if len(semverConstraints) == 0 {
		return nil, fmt.Errorf("package %s: --proxy-registry only supports semver-style constraints, got %T", packageName, constraint)
	}

	check := func(ctx context.Context, v *semver.Version) (bool, error) {
		tag := "v" + v.String()

		err := svc.packagesService.Package(packageName).CheckImageExists(ctx, tag)
		if err == nil {
			return true, nil
		}

		if errors.Is(err, client.ErrImageNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("check package %s tag %q: %w", packageName, tag, err)
	}

	tags := make([]string, 0)

	for _, semverConstraint := range semverConstraints {
		versions, err := modules.ProbeAvailableVersions(ctx, semverConstraint, check)
		if err != nil {
			return nil, fmt.Errorf("probe tags for package %s: %w", packageName, err)
		}

		for _, v := range versions {
			tags = append(tags, "v"+v.String())
		}
	}

	return tags, nil
}

// mergeAndDedupeVersions merges channel-derived versions with versions resolved
// from filter constraints over the given tags, then deduplicates.
func (svc *Service) mergeAndDedupeVersions(packageName, registryPath string, channelVersions, tags []string) []string {
	versions := append([]string(nil), channelVersions...)

	pkg := &modules.Module{
		Name:         packageName,
		RegistryPath: registryPath,
		Releases:     tags,
	}
	versions = append(versions, svc.options.Filter.VersionsToMirror(pkg)...)

	return deduplicateStrings(versions)
}

// printDryRunPlan prints the set of refs that would be pulled, without downloading.
func (svc *Service) printDryRunPlan(packageName string, downloadList *ImageDownloadList, versions []string) {
	svc.userLogger.InfoLn("[dry-run] Package '" + packageName + "' images that would be pulled:")

	for ref := range downloadList.PackageVersionChannels {
		svc.userLogger.InfoLn("  " + ref)
	}

	for _, version := range versions {
		svc.userLogger.InfoLn("  " + svc.packageRef(packageName, version))
	}

	if len(versions) > 0 {
		svc.userLogger.InfoLn("  (extra images discovery requires a real pull)")
	}
}

// pullPackageImages pulls package images for the given versions (packages/<name>:vX.Y.Z).
func (svc *Service) pullPackageImages(ctx context.Context, packageName string, versions []string, downloadList *ImageDownloadList) error {
	if len(versions) == 0 {
		return nil
	}

	for _, version := range versions {
		downloadList.Package[svc.packageRef(packageName, version)] = nil
	}

	config := puller.PullConfig{
		Name:             packageName + " images",
		ImageSet:         downloadList.Package,
		Layout:           svc.layout.Package(packageName).Packages,
		AllowMissingTags: true,
		GetterService:    svc.packagesService.Package(packageName),
	}

	if err := svc.pullerService.PullImages(ctx, config); err != nil {
		return fmt.Errorf("pull package images: %w", err)
	}

	return nil
}

// pullVersionReleaseImages pulls packages/<name>/version:vX.Y.Z tags in addition
// to channel tags (alpha, beta, ...). These may not exist for every version, so
// errors are logged at Debug and not propagated.
func (svc *Service) pullVersionReleaseImages(ctx context.Context, packageName string, versions []string, downloadList *ImageDownloadList) {
	if len(versions) == 0 {
		return
	}

	versionReleaseSet := make(map[string]*puller.ImageMeta)
	for _, version := range versions {
		versionReleaseSet[svc.versionRef(packageName, version)] = nil
		downloadList.PackageVersionChannels[svc.versionRef(packageName, version)] = nil
	}

	config := puller.PullConfig{
		Name:             packageName + " version releases",
		ImageSet:         versionReleaseSet,
		Layout:           svc.layout.Package(packageName).PackageVersionChannels,
		AllowMissingTags: true,
		GetterService:    svc.packagesService.Package(packageName).VersionChannels(),
	}

	if err := svc.pullerService.PullImages(ctx, config); err != nil {
		svc.logger.Debug(fmt.Sprintf("Failed to pull version release images for %s: %v", packageName, err))
	}
}

// pullInternalDigestImages discovers and pulls images referenced by
// images_digests.json inside each package version.
func (svc *Service) pullInternalDigestImages(ctx context.Context, packageName string, versions []string, downloadList *ImageDownloadList) {
	digestImages := svc.extractInternalDigestImages(ctx, packageName, versions)
	if len(digestImages) == 0 {
		return
	}

	digestImageSet := make(map[string]*puller.ImageMeta)
	for _, digestRef := range digestImages {
		digestImageSet[digestRef] = nil
		downloadList.Package[digestRef] = nil
	}

	config := puller.PullConfig{
		Name:             packageName + " internal images",
		ImageSet:         digestImageSet,
		Layout:           svc.layout.Package(packageName).Packages,
		AllowMissingTags: true,
		GetterService:    svc.packagesService.Package(packageName),
	}

	if err := svc.pullerService.PullImages(ctx, config); err != nil {
		svc.logger.Debug(fmt.Sprintf("Failed to pull internal digest images for %s: %v", packageName, err))
	}
}

// pullExtraImages discovers extra images declared by each package version and
// pulls them into per-extra layouts (packages/<name>/extra/<extra-name>/).
func (svc *Service) pullExtraImages(ctx context.Context, packageName string, versions []string, downloadList *ImageDownloadList) error {
	extraImagesByName := svc.findExtraImages(ctx, packageName, versions)

	for extraName, images := range extraImagesByName {
		if len(images) == 0 {
			continue
		}

		extraLayout, err := svc.layout.Package(packageName).GetOrCreateExtraLayout(extraName)
		if err != nil {
			return fmt.Errorf("create layout for extra image %s: %w", extraName, err)
		}

		imageSet := make(map[string]*puller.ImageMeta)
		for _, img := range images {
			imageSet[img.FullRef] = nil
			downloadList.PackageExtra[img.FullRef] = nil
		}

		config := puller.PullConfig{
			Name:             packageName + "/" + extraName,
			ImageSet:         imageSet,
			Layout:           extraLayout,
			AllowMissingTags: true,
			GetterService:    svc.packagesService.Package(packageName).ExtraImage(extraName),
		}

		if err := svc.pullerService.PullImages(ctx, config); err != nil {
			return fmt.Errorf("pull extra image %s: %w", extraName, err)
		}
	}

	return nil
}

// pullVexImages finds and pulls VEX attestation images for package images.
func (svc *Service) pullVexImages(ctx context.Context, packageName string, downloadList *ImageDownloadList) {
	allImages := make([]string, 0, len(downloadList.Package)+len(downloadList.PackageExtra))

	for img := range downloadList.Package {
		allImages = append(allImages, img)
	}

	for img := range downloadList.PackageExtra {
		allImages = append(allImages, img)
	}

	vexImageSet := make(map[string]*puller.ImageMeta)

	for _, img := range allImages {
		vexImageName, err := svc.findVexImage(ctx, packageName, img)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to find VEX image for %s: %v", img, err))
			continue
		}

		if vexImageName != "" {
			svc.logger.Debug(fmt.Sprintf("Found VEX image: %s", vexImageName))
			vexImageSet[vexImageName] = nil
			downloadList.Package[vexImageName] = nil
		}
	}

	if len(vexImageSet) > 0 {
		config := puller.PullConfig{
			Name:             packageName + " VEX images",
			ImageSet:         vexImageSet,
			Layout:           svc.layout.Package(packageName).Packages,
			AllowMissingTags: true,
			GetterService:    svc.packagesService.Package(packageName),
		}

		if err := svc.pullerService.PullImages(ctx, config); err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to pull VEX images for %s: %v", packageName, err))
		}
	}
}

// findVexImage checks if a VEX attestation image exists for the given image.
func (svc *Service) findVexImage(ctx context.Context, packageName string, imageRef string) (string, error) {
	vexImageName := strings.Replace(strings.Replace(imageRef, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"

	splitIndex := strings.LastIndex(vexImageName, ":")
	if splitIndex == -1 {
		return "", nil
	}

	tag := vexImageName[splitIndex+1:]

	err := svc.packagesService.Package(packageName).CheckImageExists(ctx, tag)
	if errors.Is(err, client.ErrImageNotFound) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return vexImageName, nil
}

// extractVersionsFromVersionChannels extracts version tags from pulled version channel images.
func (svc *Service) extractVersionsFromVersionChannels(ctx context.Context, packageName string) []string {
	versions := make([]string, 0)

	channels := internal.GetAllDefaultReleaseChannels()

	if err := svc.packagesService.Package(packageName).VersionChannels().CheckImageExists(ctx, internal.LTSChannel); err == nil {
		channels = append(channels, internal.LTSChannel)
	}

	for _, channel := range channels {
		img, err := svc.packagesService.Package(packageName).VersionChannels().GetImage(ctx, channel)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to get version channel image for %s/%s: %v", packageName, channel, err))
			continue
		}

		versionJSON, err := extractVersionJSON(img)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to extract version.json for %s/%s: %v", packageName, channel, err))
			continue
		}

		if versionJSON.Version != "" {
			version := versionJSON.Version
			if !strings.HasPrefix(version, "v") {
				version = "v" + version
			}

			versions = append(versions, version)
		}
	}

	return versions
}

type versionJSON struct {
	Version string `json:"version"`
}

// extractVersionJSON extracts version.json from an image using the Extract method.
func extractVersionJSON(img interface{ Extract() io.ReadCloser }) (*versionJSON, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("version.json not found in image")
		}

		if err != nil {
			return nil, err
		}

		if hdr.Name == "version.json" {
			var version versionJSON
			if err := json.NewDecoder(tr).Decode(&version); err != nil {
				return nil, fmt.Errorf("parse version.json: %w", err)
			}

			return &version, nil
		}
	}
}

// extraImageInfo holds information about an extra image to pull.
type extraImageInfo struct {
	Name    string
	Tag     string
	FullRef string
}

// findExtraImages finds extra images from package images.
// Extra images are stored under: packages/<name>/extra/<extra-name>:<tag>
func (svc *Service) findExtraImages(ctx context.Context, packageName string, versions []string) map[string][]extraImageInfo {
	extraImages := make(map[string][]extraImageInfo)

	for _, version := range versions {
		if strings.Contains(version, "@sha256:") {
			continue
		}

		tag := version
		if strings.Contains(version, ":") {
			parts := strings.SplitN(version, ":", 2)
			tag = parts[1]
		}

		img, err := svc.packagesService.Package(packageName).GetImage(ctx, tag)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to get package image %s:%s: %v", packageName, tag, err))
			continue
		}

		extraImagesJSON, err := extractExtraImagesJSON(img)
		if err != nil {
			continue // No extra_images.json in this version
		}

		for imageName, tagValue := range extraImagesJSON {
			var imageTag string

			switch v := tagValue.(type) {
			case float64:
				imageTag = fmt.Sprintf("%.0f", v)
			case int:
				imageTag = fmt.Sprintf("%d", v)
			case string:
				imageTag = v
			default:
				continue
			}

			fullImagePath := svc.rootURL + "/" + internal.PackagesSegment + "/" + packageName + "/" + internal.PackagesExtraSegment + "/" + imageName + ":" + imageTag

			extraImages[imageName] = append(extraImages[imageName], extraImageInfo{
				Name:    imageName,
				Tag:     imageTag,
				FullRef: fullImagePath,
			})
		}
	}

	return extraImages
}

// extractExtraImagesJSON extracts extra_images.json from an image.
func extractExtraImagesJSON(img interface{ Extract() io.ReadCloser }) (map[string]interface{}, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("extra_images.json not found in image")
		}

		if err != nil {
			return nil, err
		}

		if hdr.Name == "extra_images.json" {
			var extraImages map[string]interface{}
			if err := json.NewDecoder(tr).Decode(&extraImages); err != nil {
				return nil, fmt.Errorf("parse extra_images.json: %w", err)
			}

			return extraImages, nil
		}
	}
}

// extractInternalDigestImages extracts internal digest images from package versions.
func (svc *Service) extractInternalDigestImages(ctx context.Context, packageName string, versions []string) []string {
	seenDigests := make(map[string]struct{})

	var digestRefs []string

	packageRepo := svc.rootURL + "/" + internal.PackagesSegment + "/" + packageName

	for _, version := range versions {
		if strings.Contains(version, "@sha256:") {
			continue
		}

		tag := version
		if strings.Contains(version, ":") {
			parts := strings.SplitN(version, ":", 2)
			tag = parts[1]
		}

		img, err := svc.packagesService.Package(packageName).GetImage(ctx, tag)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to get package image %s:%s for digest extraction: %v", packageName, tag, err))
			continue
		}

		digests, err := extractImagesDigestsJSON(img)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("No images_digests.json in %s:%s: %v", packageName, tag, err))
			continue
		}

		svc.logger.Debug(fmt.Sprintf("Found %d internal digests in %s:%s", len(digests), packageName, tag))

		for _, digest := range digests {
			if _, seen := seenDigests[digest]; seen {
				continue
			}

			seenDigests[digest] = struct{}{}

			digestRef := packageRepo + "@" + digest
			digestRefs = append(digestRefs, digestRef)
		}
	}

	return digestRefs
}

// digestRegex matches sha256 digests in images_digests.json.
var digestRegex = regexp.MustCompile(`sha256:[a-f0-9]{64}`)

// extractImagesDigestsJSON extracts images_digests.json from a package image
// and returns a list of sha256 digests.
func extractImagesDigestsJSON(img interface{ Extract() io.ReadCloser }) ([]string, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("images_digests.json not found in image")
		}

		if err != nil {
			return nil, err
		}

		if hdr.Name == "images_digests.json" {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("read images_digests.json: %w", err)
			}

			digests := digestRegex.FindAllString(string(data), -1)

			return digests, nil
		}
	}
}

// applyChannelAliases applies version channel tags to images with exact tag constraints.
func (svc *Service) applyChannelAliases(packageName string) error {
	constraint, ok := svc.options.Filter.GetConstraint(packageName)
	if !ok || !constraint.IsExact() {
		return nil
	}

	packageLayout := svc.layout.Package(packageName)
	if packageLayout == nil || packageLayout.PackageVersionChannels == nil {
		return nil
	}

	exacts := modules.ExactConstraintsOf(constraint)

	// A single pinned tag without an explicit +channel suffix is published to
	// every release channel. When several tags are pinned at once, propagating
	// each to all channels would clobber one another, so only tags that name
	// their own channel (=vX.Y.Z+stable) are aliased; the rest are pulled as-is.
	propagateToAllChannels := len(exacts) == 1 && !exacts[0].HasChannelAlias()

	for _, exact := range exacts {
		desc, err := layouts.FindImageDescriptorByTag(packageLayout.PackageVersionChannels.Path(), exact.Tag())
		if err != nil {
			if errors.Is(err, layouts.ErrImageNotFound) {
				continue
			}

			return err
		}

		var channels []string

		switch {
		case exact.HasChannelAlias():
			channels = []string{exact.Channel()}
		case propagateToAllChannels:
			channels = append(internal.GetAllDefaultReleaseChannels(), internal.LTSChannel)
		}

		for _, channel := range channels {
			if err := layouts.TagImage(packageLayout.PackageVersionChannels.Path(), desc.Digest, channel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (svc *Service) packPackages(ctx context.Context, pkgs []packageData) error {
	logger := svc.userLogger

	bundleDir := svc.options.BundleDir
	bundleChunkSize := svc.options.BundleChunkSize

	for _, pkg := range pkgs {
		if err := ctx.Err(); err != nil {
			return err
		}

		pkgName := "package-" + pkg.name + ".tar"

		packageLayout := svc.layout.Package(pkg.name)
		if packageLayout == nil {
			return fmt.Errorf("no layout found for package %s", pkg.name)
		}

		// Skip packages that produced no images silently.
		if !packageLayout.HasImages() {
			continue
		}

		if err := logger.Process(fmt.Sprintf("Pack %s", pkgName), func() error {
			// Pack from the package's working directory with prefix to create
			// the correct registry structure (packages/<name>/index.json ...).
			packageDir := filepath.Join(svc.layout.workingDir, pkg.name)
			tarPrefix := filepath.Join(internal.PackagesSegment, pkg.name)

			return pack.Bundle(ctx, bundleDir, pkgName, bundleChunkSize, func(w io.Writer) error {
				return bundle.PackWithPrefix(ctx, packageDir, tarPrefix, w)
			})
		}); err != nil {
			return err
		}
	}

	return nil
}

// packageRef builds the registry reference for a package main image tag.
func (svc *Service) packageRef(packageName, tag string) string {
	return svc.rootURL + "/" + internal.PackagesSegment + "/" + packageName + ":" + tag
}

// versionRef builds the registry reference for a package version-channel tag.
func (svc *Service) versionRef(packageName, tag string) string {
	return svc.rootURL + "/" + internal.PackagesSegment + "/" + packageName + "/" + internal.PackagesVersionSegment + ":" + tag
}

func getPackageNames(pkgs []packageData) []string {
	names := make([]string, len(pkgs))
	for i, p := range pkgs {
		names[i] = p.name
	}

	return names
}

func deduplicateStrings(items []string) []string {
	seen := make(map[string]struct{})

	result := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; !ok {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}

	return result
}

func createOCIImageLayoutsForPackages(
	rootFolder string,
	pkgs []string,
) (*PackagesImageLayouts, error) {
	layouts := NewPackagesImageLayouts(rootFolder)

	for _, packageName := range pkgs {
		packageLayouts, err := createOCIImageLayoutsForPackage(
			filepath.Join(rootFolder, packageName),
		)
		if err != nil {
			return nil, fmt.Errorf("create OCI image layouts for package %s: %w", packageName, err)
		}

		layouts.list[packageName] = packageLayouts
	}

	return layouts, nil
}

func createOCIImageLayoutsForPackage(
	rootFolder string,
) (*ImageLayouts, error) {
	layouts := NewImageLayouts(rootFolder)

	// Only create layouts for main package and version channels.
	// Extra image layouts are created dynamically when extra images are discovered.
	mirrorTypes := []internal.MirrorType{
		internal.MirrorTypePackages,
		internal.MirrorTypePackagesVersionChannels,
	}

	for _, mtype := range mirrorTypes {
		err := layouts.setLayoutByMirrorType(rootFolder, mtype)
		if err != nil {
			return nil, fmt.Errorf("set layout by mirror type %v: %w", mtype, err)
		}
	}

	return layouts, nil
}
