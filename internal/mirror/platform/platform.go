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

package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/manifests"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Options contains configuration options for the platform service
type Options struct {
	// SinceVersion specifies the minimum version to start mirroring from (optional)
	SinceVersion *semver.Version
	// TargetTag specifies a specific tag to mirror instead of determining versions automatically
	// it can be:
	// semver f.e. vX.Y.Z
	// channel f.e. alpha/beta/stable
	// any other tag
	TargetTag string
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
	// IgnoreSuspend allows mirroring even if release channels are suspended
	IgnoreSuspend bool
}

type Service struct {
	// deckhouseService handles Deckhouse platform registry operations
	deckhouseService *registryservice.DeckhouseService
	// layout manages the OCI image layouts for different components
	layout *ImageLayouts
	// downloadList manages the list of images to be downloaded
	downloadList *ImageDownloadList
	// pullerService handles the pulling of images
	pullerService *puller.PullerService

	// options contains service configuration
	options *Options

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
	userLogger.Infof("Creating OCI Image Layouts")

	if options == nil {
		options = &Options{}
	}

	tmpDir := filepath.Join(workingDir, "platform")

	layout, err := createOCIImageLayoutsForPlatform(tmpDir)
	if err != nil {
		//TODO: handle error
		userLogger.Warnf("Create OCI Image Layouts: %v", err)
	}

	rootURL := registryService.GetRoot()

	return &Service{
		deckhouseService: registryService.DeckhouseService(),
		layout:           layout,
		downloadList:     NewImageDownloadList(rootURL),
		pullerService:    puller.NewPullerService(logger, userLogger),
		options:          options,
		logger:           logger,
		userLogger:       userLogger,
	}
}

// PullPlatform pulls the Deckhouse platform images and metadata
// It validates access to the registry, determines which versions to mirror,
// and prepares the image layouts for mirroring
func (svc *Service) PullPlatform(ctx context.Context) error {
	err := svc.validatePlatformAccess(ctx)
	if err != nil {
		return fmt.Errorf("validate platform access: %w", err)
	}

	tagsToMirror, channelsToMirror, err := svc.findTagsToMirror(ctx)
	if err != nil {
		return fmt.Errorf("find tags to mirror: %w", err)
	}

	svc.downloadList.FillDeckhouseImages(tagsToMirror)
	svc.downloadList.FillForChannels(channelsToMirror)

	err = svc.pullDeckhousePlatform(ctx, tagsToMirror)
	if err != nil {
		return fmt.Errorf("pull deckhouse platform: %w", err)
	}

	return nil
}

// validatePlatformAccess validates access to the platform registry
// It checks if the target tag or channel exists in the source registry
// with a timeout to prevent hanging on network issues
func (svc *Service) validatePlatformAccess(ctx context.Context) error {
	// Default to stable channel if no specific tag is set
	targetTag := internal.StableChannel

	if svc.options.TargetTag != "" {
		targetTag = svc.options.TargetTag
	}

	svc.logger.Debug("Validating access to the source registry", slog.String("tag", targetTag))

	// Add timeout to prevent hanging on slow/unreachable registries
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Check if target is a release channel (like "stable", "beta") or a specific tag
	if internal.ChannelIsValid(targetTag) {
		err := svc.deckhouseService.ReleaseChannels().CheckImageExists(ctx, targetTag)
		if err != nil {
			return fmt.Errorf("failed to check release channel %q exists in registry: %w", targetTag, err)
		}

		return nil
	}

	// For specific tags, check if the tag exists
	err := svc.deckhouseService.CheckImageExists(ctx, targetTag)
	if err != nil {
		return fmt.Errorf("failed to check Deckhouse tag %q exists in registry: %w", targetTag, err)
	}

	return nil
}

// findTagsToMirror determines which Deckhouse release tags should be mirrored
// If a specific target tag is set, it returns only that tag
// Otherwise, it finds all relevant versions that should be mirrored based on channels and version ranges
func (svc *Service) findTagsToMirror(ctx context.Context) ([]string, []string, error) {
	strickTags := []string{}
	if svc.options.TargetTag != "" {
		strickTags = append(strickTags, svc.options.TargetTag)
	}

	result, err := svc.versionsToMirror(ctx, strickTags)
	if err != nil {
		return nil, nil, fmt.Errorf("Find versions to mirror: %w", err)
	}

	svc.userLogger.Infof("Deckhouse releases to pull: %+v", result.Versions)

	vers := make([]string, 0, len(result.Versions)+len(result.CustomTags))
	for _, v := range result.Versions {
		vers = append(vers, "v"+v.String())
	}
	// Add custom tags as-is (without "v" prefix)
	vers = append(vers, result.CustomTags...)

	return vers, result.Channels, nil
}

type releaseChannelVersionResult struct {
	ver *semver.Version
	err error
}

// VersionsToMirrorResult contains the result of versionsToMirror operation
type VersionsToMirrorResult struct {
	// Versions contains semver versions to mirror
	Versions []semver.Version
	// Channels contains release channels to mirror
	Channels []string
	// CustomTags contains custom tags (non-semver, non-channel tags) to mirror
	CustomTags []string
}

// parsedTags represents the parsed input tags categorized by type
type parsedTags struct {
	semverVersions []*semver.Version
	customTags     []string
}

// channelVersions represents the fetched versions from release channels
type channelVersions map[string]*semver.Version

// versionsToMirror determines which Deckhouse release versions should be mirrored
// It collects current versions from all release channels and filters available releases
// to include only versions that should be mirrored based on the mirroring strategy
func (svc *Service) versionsToMirror(ctx context.Context, tagsToMirror []string) (*VersionsToMirrorResult, error) {
	if len(tagsToMirror) > 0 {
		svc.userLogger.Infof("Skipped releases lookup as tag %q is specifically requested with --deckhouse-tag", svc.options.TargetTag)
	}

	// Parse input tags into categories
	parsed := svc.parseInputTags(tagsToMirror)

	// Fetch current versions from all release channels
	channelVersions, err := svc.fetchReleaseChannelVersions(ctx)
	if err != nil {
		return nil, err
	}

	// Match channels and versions based on requested tags
	versions, matchedChannels := svc.matchChannelsToTags(tagsToMirror, channelVersions, parsed.semverVersions)

	// If specific tags requested, return immediately
	if len(tagsToMirror) > 0 {
		return &VersionsToMirrorResult{
			Versions:   deduplicateVersions(versions),
			Channels:   matchedChannels,
			CustomTags: parsed.customTags,
		}, nil
	}

	// For full discovery mode, expand version range
	expandedVersions, err := svc.expandVersionRange(ctx, channelVersions, versions)
	if err != nil {
		return nil, err
	}

	// Filter out channels that are below the minimum version (SinceVersion/rock-solid)
	minVersion := svc.determineMinimumVersion(channelVersions)
	filteredChannels := make([]string, 0, len(matchedChannels))
	for _, ch := range matchedChannels {
		if minVersion != nil {
			if v, ok := channelVersions[ch]; ok && v != nil && v.LessThan(minVersion) {
				continue
			}
		}
		filteredChannels = append(filteredChannels, ch)
	}

	return &VersionsToMirrorResult{
		Versions:   deduplicateVersions(expandedVersions),
		Channels:   filteredChannels,
		CustomTags: parsed.customTags,
	}, nil
}

// parseInputTags categorizes input tags into semver versions and custom tags
func (svc *Service) parseInputTags(tags []string) parsedTags {
	result := parsedTags{
		semverVersions: make([]*semver.Version, 0, len(tags)),
		customTags:     make([]string, 0),
	}

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			// Not a valid semver - check if it's a custom tag (not a channel name)
			if !internal.ChannelIsValid(tag) {
				result.customTags = append(result.customTags, tag)
			}
			continue
		}
		result.semverVersions = append(result.semverVersions, version)
	}

	return result
}

// fetchReleaseChannelVersions retrieves current versions from all release channels
func (svc *Service) fetchReleaseChannelVersions(ctx context.Context) (channelVersions, error) {
	channelsToFetch := append(internal.GetAllDefaultReleaseChannels(), internal.LTSChannel)

	// Fetch versions from all channels
	channelResults := make(map[string]releaseChannelVersionResult, len(channelsToFetch))
	for _, channel := range channelsToFetch {
		version, err := svc.getReleaseChannelVersionFromRegistry(ctx, channel)

		// LTS channel is optional - warn and continue if missing
		if err != nil && channel == internal.LTSChannel {
			svc.userLogger.Warnf("Skipping LTS channel: %v", err)
			continue
		}

		channelResults[channel] = releaseChannelVersionResult{ver: version, err: err}
	}

	// Validate and extract successful channel versions
	return svc.validateChannelResults(channelResults)
}

// validateChannelResults validates channel fetch results and extracts successful versions
func (svc *Service) validateChannelResults(results map[string]releaseChannelVersionResult) (channelVersions, error) {
	versions := make(channelVersions, len(results))
	_, ltsExists := results[internal.LTSChannel]

	for channel, result := range results {
		// If LTS doesn't exist, all other channels must succeed
		if !ltsExists && result.err != nil {
			return nil, fmt.Errorf("get %s release version from registry: %w", channel, result.err)
		}

		if result.err == nil {
			versions[channel] = result.ver
		}
	}

	return versions, nil
}

// matchChannelsToTags matches requested tags to channel versions and returns matching versions and channels
func (svc *Service) matchChannelsToTags(requestedTags []string, channelVersions channelVersions, semverVersions []*semver.Version) ([]*semver.Version, []string) {
	versions := make([]*semver.Version, 0, len(semverVersions))
	versions = append(versions, semverVersions...)

	matchedChannels := make(map[string]struct{})

	// If no specific tags requested, mirror all channels
	if len(requestedTags) == 0 {
		for channel, version := range channelVersions {
			versions = append(versions, version)
			matchedChannels[channel] = struct{}{}
		}
		return versions, mapKeysToSlice(matchedChannels)
	}

	// Match specific tags to channels
	for channel, version := range channelVersions {
		for _, tag := range requestedTags {
			if svc.tagMatchesChannel(tag, channel, version) {
				versions = append(versions, version)
				matchedChannels[channel] = struct{}{}
				break
			}
		}
	}

	return versions, mapKeysToSlice(matchedChannels)
}

// tagMatchesChannel checks if a tag matches a channel (by name or version)
func (svc *Service) tagMatchesChannel(tag, channelName string, channelVersion *semver.Version) bool {
	return tag == channelName || tag == "v"+channelVersion.String()
}

// expandVersionRange expands the version range for full discovery mode
func (svc *Service) expandVersionRange(ctx context.Context, channelVersions channelVersions, baseVersions []*semver.Version) ([]*semver.Version, error) {
	minVersion := svc.determineMinimumVersion(channelVersions)
	maxVersion := channelVersions[internal.AlphaChannel]

	if maxVersion == nil {
		// No alpha channel - return base versions only
		return baseVersions, nil
	}

	svc.userLogger.Debugf("listing deckhouse releases")

	// Fetch all available tags
	allTags, err := svc.deckhouseService.ReleaseChannels().ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("get tags from Deckhouse registry: %w", err)
	}

	// Filter and get latest patches
	filteredVersions := filterVersionsBetween(minVersion, maxVersion, allTags)
	latestPatches := filterOnlyLatestPatches(filteredVersions)

	// Filter base channel versions by minVersion as well
	filteredBase := baseVersions
	if minVersion != nil {
		nb := make([]*semver.Version, 0, len(baseVersions))
		for _, v := range baseVersions {
			if v == nil || v.LessThan(minVersion) {
				continue
			}
			nb = append(nb, v)
		}
		filteredBase = nb
	}

	return append(filteredBase, latestPatches...), nil
}

// determineMinimumVersion determines the minimum version for mirroring based on configuration
func (svc *Service) determineMinimumVersion(channelVersions channelVersions) *semver.Version {
	rockSolidVersion := channelVersions[internal.RockSolidChannel]
	if rockSolidVersion == nil {
		return nil
	}

	// Use rock-solid as baseline
	minVersion := rockSolidVersion

	// If SinceVersion is provided and is newer than rock-solid, start from SinceVersion
	// (user wants to mirror from a later version than rock-solid)
	if svc.options.SinceVersion != nil && svc.options.SinceVersion.GreaterThan(minVersion) {
		minVersion = svc.options.SinceVersion
	}

	return minVersion
}

// mapKeysToSlice converts a map's keys to a slice
func mapKeysToSlice(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// getReleaseChannelVersionFromRegistry retrieves the current version for a specific release channel
// It fetches the release image and metadata, validates the channel is not suspended,
// and stores the image in the layout for later use
func (svc *Service) getReleaseChannelVersionFromRegistry(ctx context.Context, releaseChannel string) (*semver.Version, error) {
	image, err := svc.deckhouseService.ReleaseChannels().GetImage(ctx, releaseChannel)
	if err != nil {
		return nil, fmt.Errorf("get %s release channel image: %w", releaseChannel, err)
	}

	meta, err := svc.deckhouseService.ReleaseChannels().GetMetadata(ctx, releaseChannel)
	if err != nil {
		return nil, fmt.Errorf("cannot get %s release channel version.json: %w", releaseChannel, err)
	}

	if meta.Suspend && !svc.options.IgnoreSuspend {
		return nil, fmt.Errorf("source registry contains suspended release channel %q, try again later (use --ignore-suspend to override)", releaseChannel)
	}

	ver, err := semver.NewVersion(meta.Version)
	if err != nil {
		return nil, fmt.Errorf("release channel version is not semver %q: %w", meta.Version, err)
	}

	digest, err := image.Digest()
	if err != nil {
		return nil, fmt.Errorf("cannot get %s release channel image digest: %w", releaseChannel, err)
	}

	imageMeta, err := image.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("cannot get %s release channel image tag reference: %w", releaseChannel, err)
	}

	svc.userLogger.Debugf("image reference: %s@%s", imageMeta, digest.String())

	// Don't add to layout here - pullDeckhouseReleaseChannels will add it
	// Just record in downloadList for later pull
	svc.downloadList.DeckhouseReleaseChannel[imageMeta.GetTagReference()] = puller.NewImageMeta(meta.Version, imageMeta.GetTagReference(), &digest)

	return ver, nil
}

func (svc *Service) pullDeckhousePlatform(ctx context.Context, tagsToMirror []string) error {
	logger := svc.userLogger

	err := logger.Process("Pull release channels and installers", func() error {
		if err := svc.pullDeckhouseReleaseChannels(ctx); err != nil {
			return fmt.Errorf("pull release channels: %w", err)
		}

		if err := svc.pullInstallers(ctx); err != nil {
			return fmt.Errorf("pull installers: %w", err)
		}

		if err := svc.pullStandaloneInstallers(ctx); err != nil {
			return fmt.Errorf("pull standalone installers: %w", err)
		}

		if err := svc.pullDeckhouseImages(ctx); err != nil {
			return fmt.Errorf("pull deckhouse images: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// We should not generate deckhousereleases.yaml manifest for tag-based pulls
	if svc.options.TargetTag == "" {
		if err = svc.generateDeckhouseReleaseManifests(tagsToMirror); err != nil {
			logger.WarnLn(err.Error())
		}
	}

	logger.Infof("Searching for Deckhouse built-in modules digests")

	var uniqueImages = make(map[string]string, 0)
	for _, imageMeta := range svc.downloadList.DeckhouseInstall {
		if _, ok := uniqueImages[imageMeta.DigestReference]; ok {
			continue
		}

		uniqueImages[imageMeta.DigestReference] = imageMeta.ImageTag
	}

	var prevDigests = make(map[string]struct{}, 0)
	for _, tag := range uniqueImages {
		svc.userLogger.Infof("Extracting images digests from Deckhouse installer %s", tag)

		digests, err := svc.ExtractImageDigestsFromDeckhouseInstallerNew(tag, prevDigests)
		if err != nil {
			return fmt.Errorf("extract images digests: %w", err)
		}

		maps.Copy(svc.downloadList.Deckhouse, digests)
	}

	logger.Infof("Found %d images", len(svc.downloadList.Deckhouse))

	if err = logger.Process("Pull Deckhouse images", func() error {
		if err := svc.pullDeckhouseImages(ctx); err != nil {
			return fmt.Errorf("pull deckhouse images: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("Pull Deckhouse images: %w", err)
	}

	err = logger.Process("Processing image indexes", func() error {
		if svc.options.TargetTag != "" {
			// If we are pulling some build by tag, propagate release channel image of it to all channels if it exists.
			releaseChannel, err := svc.layout.DeckhouseReleaseChannel.GetImage(svc.options.TargetTag)

			switch {
			case errors.Is(err, layouts.ErrImageNotFound):
				logger.WarnLn("Registry does not contain release channels, release channels images will not be added to bundle")
				// TODO: remove goto
				goto sortManifests
			case err != nil:
				return fmt.Errorf("Find release-%s channel descriptor: %w", svc.options.TargetTag, err)
			}

			digest, err := releaseChannel.Digest()
			if err != nil {
				return fmt.Errorf("cannot get release channel image digest: %w", err)
			}

			for _, channel := range internal.GetAllDefaultReleaseChannels() {
				if err = svc.layout.DeckhouseReleaseChannel.TagImage(digest, channel); err != nil {
					return fmt.Errorf("tag release channel: %w", err)
				}
			}
		}

	sortManifests:
		for _, l := range svc.layout.AsList() {
			err = layouts.SortIndexManifests(l)
			if err != nil {
				return fmt.Errorf("Sorting index manifests of %s: %w", l, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("Processing image indexes: %w", err)
	}

	if err := logger.Process("Pack Deckhouse images into platform.tar", func() error {
		bundleChunkSize := svc.options.BundleChunkSize
		bundleDir := svc.options.BundleDir

		var platform io.Writer = chunked.NewChunkedFileWriter(
			bundleChunkSize,
			bundleDir,
			"platform.tar",
		)

		if bundleChunkSize == 0 {
			platform, err = os.Create(filepath.Join(bundleDir, "platform.tar"))
			if err != nil {
				return fmt.Errorf("create platform.tar: %w", err)
			}
		}

		if err := bundle.Pack(context.Background(), svc.layout.workingDir, platform); err != nil {
			return fmt.Errorf("pack platform.tar: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (svc *Service) pullDeckhouseReleaseChannels(ctx context.Context) error {
	config := puller.PullConfig{
		Name:             "Deckhouse release channels information",
		ImageSet:         svc.downloadList.DeckhouseReleaseChannel,
		Layout:           svc.layout.DeckhouseReleaseChannel,
		AllowMissingTags: svc.options.TargetTag != "",
		GetterService:    svc.deckhouseService.ReleaseChannels(),
	}

	return svc.pullerService.PullImages(ctx, config)
}

func (svc *Service) pullInstallers(ctx context.Context) error {
	config := puller.PullConfig{
		Name:             "installers",
		ImageSet:         svc.downloadList.DeckhouseInstall,
		Layout:           svc.layout.DeckhouseInstall,
		AllowMissingTags: true, // Allow missing installer images
		GetterService:    svc.deckhouseService.Installer(),
	}

	return svc.pullerService.PullImages(ctx, config)
}

func (svc *Service) pullStandaloneInstallers(ctx context.Context) error {
	config := puller.PullConfig{
		Name:             "standalone installers",
		ImageSet:         svc.downloadList.DeckhouseInstallStandalone,
		Layout:           svc.layout.DeckhouseInstallStandalone,
		AllowMissingTags: true,
		GetterService:    svc.deckhouseService.StandaloneInstaller(),
	}

	return svc.pullerService.PullImages(ctx, config)
}

func (svc *Service) pullDeckhouseImages(ctx context.Context) error {
	config := puller.PullConfig{
		Name:             "Deckhouse releases",
		ImageSet:         svc.downloadList.Deckhouse,
		Layout:           svc.layout.Deckhouse,
		AllowMissingTags: false,
		GetterService:    svc.deckhouseService,
	}

	return svc.pullerService.PullImages(ctx, config)
}

func (svc *Service) generateDeckhouseReleaseManifests(
	tagsToMirror []string,
) error {
	svc.userLogger.Infof("Generating DeckhouseRelease manifests")

	deckhouseReleasesManifestFile := filepath.Join(svc.options.BundleDir, "deckhousereleases.yaml")

	err := manifests.GenerateDeckhouseReleaseManifestsForVersionsNew(
		tagsToMirror,
		deckhouseReleasesManifestFile,
		svc.layout.Deckhouse,
	)
	if err != nil {
		return fmt.Errorf("generate DeckhouseRelease manifests: %w", err)
	}

	return nil
}

func (svc *Service) ExtractImageDigestsFromDeckhouseInstallerNew(
	tag string,
	prevDigests map[string]struct{},
) (map[string]*puller.ImageMeta, error) {
	logger := svc.userLogger

	logger.Debugf("Extracting images digests from Deckhouse installer %s", tag)

	img, err := svc.layout.DeckhouseInstall.GetImage(tag)
	if err != nil {
		return nil, fmt.Errorf("get installer image %q from layout: %w", tag, err)
	}

	images, err := extractDeckhouseReleaseExtraImages(img.Extract(), svc.deckhouseService.GetRoot())
	if err != nil {
		return nil, fmt.Errorf("extract extra images from installer %q: %w", tag, err)
	}

	logger.Infof("Deckhouse digests found: %d", len(images))

	logger.Infof("Searching for VEX images")

	vex := make([]string, 0)
	result := make(map[string]*puller.ImageMeta, len(images))

	const scanPrintInterval = 20
	counter := 0
	for image := range images {
		counter++
		if counter%scanPrintInterval == 0 {
			logger.Infof("[%d / %d] Scanning images for VEX", counter, len(images))
		}

		if _, ok := prevDigests[image]; ok {
			continue
		}

		vexImageName := strings.Replace(strings.Replace(image, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"
		if _, ok := prevDigests[vexImageName]; ok {
			continue
		}

		vexImageName, err := svc.FindVexImage(image)
		if err != nil {
			return nil, fmt.Errorf("find VEX image for digest %q: %w", image, err)
		}

		if vexImageName != "" {
			logger.Debugf("Vex image found %s", vexImageName)
			vex = append(vex, vexImageName)
			result[vexImageName] = nil
		}

		prevDigests[image] = struct{}{}
		prevDigests[vexImageName] = struct{}{}

		result[image] = nil
	}

	logger.Infof("[%d / %d] Scanning images for VEX", counter, len(images))

	logger.Infof("Deckhouse digests found: %d", len(images))
	logger.Infof("VEX images found: %d", len(vex))

	return result, nil
}

func extractDeckhouseReleaseExtraImages(rc io.ReadCloser, rootURL string) (map[string]struct{}, error) {
	var images = make(map[string]struct{}, 0)

	defer rc.Close()

	drr := &deckhouseInstallerReader{
		imageDigestsReader: bytes.NewBuffer(nil),
		imageTagsReader:    bytes.NewBuffer(nil),
	}

	err := drr.untarMetadata(rc)
	if err != nil {
		return nil, err
	}

	var tags map[string]map[string]string
	if drr.imageTagsReader.Len() > 0 {
		err = json.NewDecoder(drr.imageTagsReader).Decode(&tags)
		if err != nil {
			return nil, err
		}

		for _, nameDigestTuple := range tags {
			for _, imageID := range nameDigestTuple {
				images[rootURL+":"+imageID] = struct{}{}
			}
		}

		return images, nil
	}

	var digests map[string]map[string]string
	if drr.imageDigestsReader.Len() > 0 {
		err = json.NewDecoder(drr.imageDigestsReader).Decode(&digests)
		if err != nil {
			return nil, err
		}

		for _, nameDigestTuple := range digests {
			for _, imageID := range nameDigestTuple {
				images[rootURL+"@"+imageID] = struct{}{}
			}
		}

		return images, nil
	}

	return nil, fmt.Errorf("both files is not found in installer")
}

func (svc *Service) FindVexImage(
	digest string,
) (string, error) {
	logger := svc.userLogger

	// vex image reference check
	vexImageName := strings.Replace(strings.Replace(digest, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"

	logger.Debugf("Checking vex image from %s", vexImageName)

	splitIndex := strings.LastIndex(vexImageName, ":")
	tag := vexImageName[splitIndex+1:]

	err := svc.deckhouseService.CheckImageExists(context.TODO(), tag)
	if errors.Is(err, client.ErrImageNotFound) {
		// Image not found, which is expected for non-vulnerable images
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("check VEX image exists: %w", err)
	}

	return vexImageName, nil
}

// filterVersionsBetween filters release tags to include only versions
// that are above the minimum version and below the maximum version.
func filterVersionsBetween(
	minVersion *semver.Version,
	maxVersion *semver.Version,
	tags []string,
) []*semver.Version {
	result := make([]*semver.Version, 0)

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			// TODO: debug log here
			continue
		}

		if minVersion.GreaterThan(version) || version.GreaterThan(maxVersion) {
			continue
		}

		result = append(result, version)
	}

	return result
}

// filterOnlyLatestPatches reduces the list of versions to include only the latest patch version
// for each major.minor release. For example, if versions include 1.2.1, 1.2.2, and 1.2.3,
// only 1.2.3 will be kept. This prevents mirroring multiple patches of the same release.
func filterOnlyLatestPatches(versions []*semver.Version) []*semver.Version {
	type majorMinor [2]uint64

	patches := map[majorMinor]uint64{}

	for _, version := range versions {
		release := majorMinor{version.Major(), version.Minor()}

		if patch := patches[release]; patch <= version.Patch() {
			patches[release] = version.Patch()
		}
	}

	topPatches := make([]*semver.Version, 0, len(patches))
	for majMin, patch := range patches {
		// Use of semver.MustParse instead of semver.New is important here since we use those versions as map keys,
		// structs must be comparable via == operator and semver.New does not provide structs identical to semver.MustParse.
		topPatches = append(topPatches, semver.MustParse(fmt.Sprintf("v%d.%d.%d", majMin[0], majMin[1], patch)))
	}

	return topPatches
}

// deduplicateVersions removes duplicate versions from the list.
// This is necessary because channel versions and filtered versions might overlap.
func deduplicateVersions(versions []*semver.Version) []semver.Version {
	m := map[string]struct{}{}

	for _, v := range versions {
		if v == nil {
			continue
		}
		m[v.String()] = struct{}{}
	}

	vers := make([]semver.Version, 0, len(m))
	for s := range m {
		// semver.MustParse returns a canonical semver.Version value suitable for comparison
		vers = append(vers, *semver.MustParse("v" + s))
	}

	return vers
}

func createOCIImageLayoutsForPlatform(
	rootFolder string,
) (*ImageLayouts, error) {
	layouts := NewImageLayouts(rootFolder)

	mirrorTypes := []internal.MirrorType{
		internal.MirrorTypeDeckhouse,
		internal.MirrorTypeDeckhouseReleaseChannels,
		internal.MirrorTypeDeckhouseInstall,
		internal.MirrorTypeDeckhouseInstallStandalone,
	}

	for _, mtype := range mirrorTypes {
		err := layouts.setLayoutByMirrorType(rootFolder, mtype)
		if err != nil {
			return nil, fmt.Errorf("set layout by mirror type %v: %w", mtype, err)
		}
	}

	return layouts, nil
}
