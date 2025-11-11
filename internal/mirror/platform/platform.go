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
	"github.com/samber/lo"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/manifests"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type Service struct {
	// deckhouseService handles Deckhouse platform registry operations
	deckhouseService *registryservice.DeckhouseService
	// layout manages the OCI image layouts for different components
	layout *ImageLayouts
	// downloadList manages the list of images to be downloaded
	downloadList *ImageDownloadList
	// pullerService handles the pulling of images
	pullerService *puller.PullerService

	// sinceVersion specifies the minimum version to start mirroring from (optional)
	sinceVersion *semver.Version
	// targetTag specifies a specific tag to mirror instead of determining versions automatically
	targetTag string

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewService(
	deckhouseService *registryservice.DeckhouseService,
	sinceVersion *semver.Version,
	workingDir string,
	targetTag string,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.Infof("Creating OCI Image Layouts")

	tmpDir := filepath.Join(workingDir, "platform")

	layout, err := createOCIImageLayoutsForPlatform(tmpDir)
	if err != nil {
		//TODO: handle error
		userLogger.Warnf("Create OCI Image Layouts: %v", err)
	}

	return &Service{
		deckhouseService: deckhouseService,
		layout:           layout,
		downloadList:     NewImageDownloadList(deckhouseService.GetRoot()),
		pullerService:    puller.NewPullerService(deckhouseService, logger, userLogger),
		sinceVersion:     sinceVersion,
		targetTag:        targetTag,
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

	tagsToMirror, err := svc.findTagsToMirror(ctx)
	if err != nil {
		return fmt.Errorf("find tags to mirror: %w", err)
	}

	svc.downloadList.FillDeckhouseImages(tagsToMirror)
	svc.downloadList.FillForTag(svc.targetTag)

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

	if svc.targetTag != "" {
		targetTag = svc.targetTag
	}

	svc.logger.Debug("Validating access to the source registry", slog.String("tag", targetTag))

	// Add timeout to prevent hanging on slow/unreachable registries
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Check if target is a release channel (like "stable", "beta") or a specific tag
	if internal.ChannelIsValid(targetTag) {
		err := svc.deckhouseService.ReleaseChannels().CheckImageExists(ctx, targetTag)
		if err != nil {
			return fmt.Errorf("failed to check release exists: %w", err)
		}

		return nil
	}

	// For specific tags, check if the tag exists
	err := svc.deckhouseService.CheckImageExists(ctx, targetTag)
	if err != nil {
		return fmt.Errorf("failed to check tag exists: %w", err)
	}

	return nil
}

// findTagsToMirror determines which Deckhouse release tags should be mirrored
// If a specific target tag is set, it returns only that tag
// Otherwise, it finds all relevant versions that should be mirrored based on channels and version ranges
func (svc *Service) findTagsToMirror(ctx context.Context) ([]string, error) {
	// If a specific tag is requested, skip the complex version determination logic
	if svc.targetTag != "" {
		svc.userLogger.Infof("Skipped releases lookup as tag %q is specifically requested with --deckhouse-tag", svc.targetTag)

		return []string{svc.targetTag}, nil
	}

	// Determine which versions should be mirrored based on release channels and version constraints
	versionsToMirror, err := svc.versionsToMirrorFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("find versions to mirror: %w", err)
	}

	svc.userLogger.Infof("Deckhouse releases to pull: %+v", versionsToMirror)

	// Convert versions to tag format (add "v" prefix)
	return lo.Map(
		versionsToMirror,
		func(v semver.Version, _ int) string {
			return "v" + v.String()
		},
	), nil
}

// versionsToMirrorFunc determines which Deckhouse release versions should be mirrored
// It collects current versions from all release channels and filters available releases
// to include only versions that should be mirrored based on the mirroring strategy
func (svc *Service) versionsToMirrorFunc(ctx context.Context) ([]semver.Version, error) {
	logger := svc.userLogger

	releaseChannelsToCopy := internal.GetAllDefaultReleaseChannels()
	releaseChannelsToCopy = append(releaseChannelsToCopy, internal.LTSChannel)

	releaseChannelsVersions := make(map[string]*semver.Version, len(releaseChannelsToCopy))
	for _, channel := range releaseChannelsToCopy {
		version, err := svc.getReleaseChannelVersionFromRegistry(ctx, channel)
		if err != nil {
			if channel == internal.LTSChannel {
				if !errors.Is(err, client.ErrImageNotFound) {
					svc.userLogger.Warnf("Skipping LTS channel: %v", err)
				} else {
					svc.userLogger.Warnf("Skipping LTS channel, because it's not required")
				}

				continue
			}

			return nil, fmt.Errorf("get %s release version from registry: %w", channel, err)
		}

		if version == nil {
			// Channel was skipped (e.g., suspended and ignoreSuspendedChannels is true)
			continue
		}

		releaseChannelsVersions[channel] = version
	}

	rockSolidVersion := releaseChannelsVersions[internal.RockSolidChannel]

	mirrorFromVersion := *rockSolidVersion

	if svc.sinceVersion != nil {
		if svc.sinceVersion.LessThan(rockSolidVersion) {
			mirrorFromVersion = *svc.sinceVersion
		}
	}

	logger.Debugf("listing deckhouse releases")

	tags, err := svc.deckhouseService.ReleaseChannels().ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("get tags from Deckhouse registry: %w", err)
	}

	alphaChannelVersion := releaseChannelsVersions[internal.AlphaChannel]

	versionsAboveMinimal := filterVersionsBetween(&mirrorFromVersion, alphaChannelVersion, tags)
	versionsAboveMinimal = filterOnlyLatestPatches(versionsAboveMinimal)

	vers := make([]*semver.Version, 0, len(releaseChannelsVersions))
	for _, v := range releaseChannelsVersions {
		vers = append(vers, v)
	}

	return deduplicateVersions(append(vers, versionsAboveMinimal...)), nil
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

	if meta.Suspend {
		return nil, fmt.Errorf("source registry contains suspended release channel %q, try again later", releaseChannel)
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

	err = svc.layout.DeckhouseReleaseChannel.AddImage(image, imageMeta.GetTagReference())
	if err != nil {
		return nil, fmt.Errorf("append %s release channel image to layout: %w", releaseChannel, err)
	}

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
	if svc.targetTag == "" {
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
		if svc.targetTag != "" {
			// If we are pulling some build by tag, propagate release channel image of it to all channels if it exists.
			releaseChannel, err := svc.layout.DeckhouseReleaseChannel.GetImage(svc.targetTag)

			switch {
			case errors.Is(err, layouts.ErrImageNotFound):
				logger.WarnLn("Registry does not contain release channels, release channels images will not be added to bundle")
				// TODO: remove goto
				goto sortManifests
			case err != nil:
				return fmt.Errorf("Find release-%s channel descriptor: %w", svc.targetTag, err)
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
		bundleChunkSize := pullflags.ImagesBundleChunkSizeGB * 1000 * 1000 * 1000
		bundleDir := pullflags.ImagesBundlePath

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
		AllowMissingTags: svc.targetTag != "",
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

	deckhouseReleasesManifestFile := filepath.Join(pullflags.ImagesBundlePath, "deckhousereleases.yaml")

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
	m := map[semver.Version]struct{}{}

	for _, v := range versions {
		m[*v] = struct{}{}
	}

	vers := make([]semver.Version, 0, len(m))
	for k := range maps.Keys(m) {
		vers = append(vers, k)
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
