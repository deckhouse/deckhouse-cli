package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/manifests"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/samber/lo"
)

type Service struct {
	// deckhouseService handles Deckhouse platform registry operations
	deckhouseService *registryservice.DeckhouseService
	// layout manages the OCI image layouts for different components
	layout *ImageLayouts

	// sinceVersion specifies the minimum version to start mirroring from (optional)
	sinceVersion *semver.Version
	// targetTag specifies a specific tag to mirror instead of determining versions automatically
	targetTag string

	// ignoreSuspendedChannels specifies whether to skip suspended release channels instead of failing
	ignoreSuspendedChannels bool

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
	ignoreSuspendedChannels bool,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.InfoF("Creating OCI Image Layouts")

	tmpDir := filepath.Join(workingDir, "platform")

	layout, err := createOCIImageLayoutsForDeckhouse(tmpDir, deckhouseService.GetRoot())
	if err != nil {
		//TODO: handle error
		userLogger.WarnF("Create OCI Image Layouts: %v", err)
	}

	return &Service{
		deckhouseService:        deckhouseService,
		layout:                  layout,
		sinceVersion:            sinceVersion,
		targetTag:               targetTag,
		ignoreSuspendedChannels: ignoreSuspendedChannels,
		logger:                  logger,
		userLogger:              userLogger,
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

	svc.layout.FillDeckhouseImages(tagsToMirror)
	svc.layout.FillForTag(svc.targetTag)

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
		err := svc.deckhouseService.CheckReleaseExists(ctx, targetTag)
		if err != nil {
			return fmt.Errorf("failed to check release exists: %w", err)
		}

		return nil
	}

	// For specific tags, check if the tag exists
	err := svc.deckhouseService.CheckTagExists(ctx, targetTag)
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
		svc.userLogger.InfoF("Skipped releases lookup as tag %q is specifically requested with --deckhouse-tag", svc.targetTag)

		return []string{svc.targetTag}, nil
	}

	// Determine which versions should be mirrored based on release channels and version constraints
	versionsToMirror, err := svc.versionsToMirrorFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("find versions to mirror: %w", err)
	}

	svc.userLogger.InfoF("Deckhouse releases to pull: %+v", versionsToMirror)

	// Convert versions to tag format (add "v" prefix)
	return lo.Map(
		versionsToMirror,
		func(v semver.Version, index int) string {
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
		v, err := svc.getReleaseChannelVersionFromRegistry(ctx, channel)
		if err != nil {
			if channel == internal.LTSChannel {
				svc.userLogger.WarnF("Skipping LTS channel: %v", err)

				continue
			}

			return nil, fmt.Errorf("get %s release version from registry: %w", channel, err)
		}

		if v == nil {
			// Channel was skipped (e.g., suspended and ignoreSuspendedChannels is true)
			continue
		}

		releaseChannelsVersions[channel] = v
	}

	rockSolidVersion := releaseChannelsVersions[internal.RockSolidChannel]

	mirrorFromVersion := *rockSolidVersion

	if svc.sinceVersion != nil {
		if svc.sinceVersion.LessThan(rockSolidVersion) {
			mirrorFromVersion = *svc.sinceVersion
		}
	}

	logger.DebugF("listing deckhouse releases")

	tags, err := svc.deckhouseService.ListReleaseTags(ctx)
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
	image, err := svc.deckhouseService.GetReleaseImage(ctx, releaseChannel)
	if err != nil {
		return nil, fmt.Errorf("get %s release channel image: %w", releaseChannel, err)
	}

	meta, err := svc.deckhouseService.GetReleaseMetadata(ctx, releaseChannel)
	if err != nil {
		return nil, fmt.Errorf("cannot get %s release channel version.json: %w", releaseChannel, err)
	}

	if meta.Suspend {
		if !svc.ignoreSuspendedChannels {
			return nil, fmt.Errorf("source registry contains suspended release channel %q, try again later", releaseChannel)
		}
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

	svc.userLogger.DebugF("image reference: %s@%s", imageMeta, digest.String())

	err = svc.layout.DeckhouseReleaseChannel.AddImage(image, releaseChannel)
	if err != nil {
		return nil, fmt.Errorf("append %s release channel image to layout: %w", releaseChannel, err)
	}

	svc.layout.ReleaseChannelImages[imageMeta.GetTagReference()] = NewImageMeta(meta.Version, imageMeta.GetTagReference(), &digest)

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

		if err := svc.pullDeckhouseReleases(ctx); err != nil {
			return fmt.Errorf("pull deckhouse releases: %w", err)
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

	logger.InfoF("Searching for Deckhouse built-in modules digests")

	var prevDigests = make(map[string]struct{}, 0)
	for _, imageMeta := range svc.layout.InstallImages {
		digests, err := svc.ExtractImageDigestsFromDeckhouseInstallerNew(imageMeta.ImageTag, prevDigests)
		if err != nil {
			return fmt.Errorf("Extract images digests: %w", err)
		}
		_ = digests
		// maps.Copy(svc.layout.DeckhouseImages, digests)
	}

	// logger.InfoF("Found %d images", len(svc.layout.DeckhouseImages))

	// if err = logger.Process("Pull Deckhouse images", func() error {
	// 	return layouts.PullDeckhouseImages(pullParams, imageLayouts, client)
	// }); err != nil {
	// 	return fmt.Errorf("Pull Deckhouse images: %w", err)
	// }

	// err = logger.Process("Processing image indexes", func() error {
	// 	if pullParams.DeckhouseTag != "" {
	// 		// If we are pulling some build by tag, propagate release channel image of it to all channels if it exists.
	// 		releaseChannel, err := layouts.FindImageDescriptorByTag(imageLayouts.ReleaseChannel, pullParams.DeckhouseTag)
	// 		switch {
	// 		case errors.Is(err, layouts.ErrImageNotFound):
	// 			logger.WarnLn("Registry does not contain release channels, release channels images will not be added to bundle")
	// 			goto sortManifests
	// 		case err != nil:
	// 			return fmt.Errorf("Find release-%s channel descriptor: %w", pullParams.DeckhouseTag, err)
	// 		}

	// 		for _, channel := range internal.GetAllDefaultReleaseChannels() {
	// 			if err = layouts.TagImage(imageLayouts.ReleaseChannel, releaseChannel.Digest, channel); err != nil {
	// 				return fmt.Errorf("tag release channel: %w", err)
	// 			}
	// 		}
	// 	}

	// sortManifests:
	// 	for _, l := range imageLayouts.AsList() {
	// 		err = layouts.SortIndexManifests(l)
	// 		if err != nil {
	// 			return fmt.Errorf("Sorting index manifests of %s: %w", l, err)
	// 		}
	// 	}
	// 	return nil
	// })
	// if err != nil {
	// 	return fmt.Errorf("Processing image indexes: %w", err)
	// }

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
	svc.userLogger.InfoLn("Beginning to pull Deckhouse release channels information")

	svc.userLogger.InfoLn("Pull deckhouse release channels meta")
	for image, meta := range svc.layout.ReleaseChannelImages {
		if meta != nil {
			continue
		}

		_, tag := splitImageRefByRepoAndTag(image)

		digest, err := svc.deckhouseService.GetDigest(ctx, tag)
		if err != nil {
			return fmt.Errorf("get digest: %w", err)
		}

		svc.layout.ReleaseChannelImages[image] = NewImageMeta(tag, image, digest)
	}
	svc.userLogger.InfoLn("All required deckhouse release channels meta are pulled!")

	if err := svc.PullReleaseImageSet(ctx, svc.layout.ReleaseChannelImages, svc.layout.DeckhouseReleaseChannel, svc.targetTag != ""); err != nil {
		return err
	}

	svc.userLogger.InfoLn("Deckhouse release channels are pulled!")

	return nil
}

func (svc *Service) pullInstallers(ctx context.Context) error {
	svc.userLogger.InfoLn("Beginning to pull installers")

	svc.userLogger.InfoLn("Pull installers meta")
	for image, meta := range svc.layout.InstallImages {
		if meta != nil {
			continue
		}

		_, tag := splitImageRefByRepoAndTag(image)

		digest, err := svc.deckhouseService.GetDigest(ctx, tag)
		if err != nil {
			return fmt.Errorf("get digest: %w", err)
		}

		svc.layout.InstallImages[image] = NewImageMeta(tag, image, digest)
	}
	svc.userLogger.InfoLn("All required installers meta are pulled!")

	if err := svc.PullImageSet(ctx, svc.layout.InstallImages, svc.layout.DeckhouseInstall, svc.targetTag != ""); err != nil {
		return err
	}

	svc.userLogger.InfoLn("All required installers are pulled!")

	return nil
}

func (svc *Service) pullStandaloneInstallers(ctx context.Context) error {
	svc.userLogger.InfoLn("Beginning to pull standalone installers")

	svc.userLogger.InfoLn("Pull standalone installers meta")
	for image, meta := range svc.layout.InstallStandaloneImages {
		if meta != nil {
			continue
		}

		_, tag := splitImageRefByRepoAndTag(image)

		digest, err := svc.deckhouseService.GetDigest(ctx, tag)
		if err != nil {
			return fmt.Errorf("get digest: %w", err)
		}

		svc.layout.InstallStandaloneImages[image] = NewImageMeta(tag, image, digest)
	}
	svc.userLogger.InfoLn("All required standalone installers meta are pulled!")

	if err := svc.PullImageSet(ctx, svc.layout.InstallStandaloneImages, svc.layout.DeckhouseInstallStandalone, true); err != nil {
		return err
	}

	svc.userLogger.InfoLn("All required standalone installers are pulled!")

	return nil
}

func (svc *Service) pullDeckhouseReleases(ctx context.Context) error {
	svc.userLogger.InfoLn("Beginning to pull Deckhouse releases")

	svc.userLogger.InfoLn("Pull deckhouse releases meta")
	for image, meta := range svc.layout.DeckhouseImages {
		if meta != nil {
			continue
		}

		_, tag := splitImageRefByRepoAndTag(image)

		digest, err := svc.deckhouseService.GetDigest(ctx, tag)
		if err != nil {
			return fmt.Errorf("get digest: %w", err)
		}

		svc.layout.DeckhouseImages[image] = NewImageMeta(tag, image, digest)
	}
	svc.userLogger.InfoLn("All required deckhouse releases meta are pulled!")

	if err := svc.PullImageSet(ctx, svc.layout.DeckhouseImages, svc.layout.Deckhouse, svc.targetTag != ""); err != nil {
		return err
	}

	svc.userLogger.InfoLn("All required Deckhouse releases are pulled!")

	return nil
}

// ImageGetter is a function type for getting images from the registry
type ImageGetter func(ctx context.Context, tag string) (pkg.RegistryImage, error)

func (svc *Service) PullReleaseImageSet(ctx context.Context, imageSet map[string]*ImageMeta, imageSetLayout *registry.ImageLayout, allowMissingTags bool) error {
	return svc.pullImageSet(ctx, imageSet, imageSetLayout, allowMissingTags, svc.deckhouseService.GetReleaseImage)
}

func (svc *Service) PullImageSet(ctx context.Context, imageSet map[string]*ImageMeta, imageSetLayout *registry.ImageLayout, allowMissingTags bool) error {
	return svc.pullImageSet(ctx, imageSet, imageSetLayout, allowMissingTags, svc.deckhouseService.GetImage)
}

func (svc *Service) pullImageSet(
	ctx context.Context,
	imageSet map[string]*ImageMeta,
	imageSetLayout *registry.ImageLayout,
	allowMissingTags bool,
	imageGetter ImageGetter,
) error {
	logger := svc.userLogger

	pullCount, totalCount := 1, len(imageSet)

	for _, imageMeta := range imageSet {
		logger.DebugF("Preparing to pull image %s", imageMeta.TagReference)

		logger.DebugF("Pulling image path %s: tag %s", imageMeta.ImageRepo, imageMeta.ImageTag)

		err := retry.RunTask(
			ctx,
			svc.userLogger,
			fmt.Sprintf("[%d / %d] Pulling %s ", pullCount, totalCount, imageMeta.TagReference),
			task.WithConstantRetries(5, 10*time.Second, func(ctx context.Context) error {
				img, err := imageGetter(ctx, "@"+imageMeta.Digest.String())
				if err != nil {
					if errors.Is(err, registry.ErrImageNotFound) && allowMissingTags {
						logger.WarnLn("⚠️ Not found in registry, skipping pull")

						return nil
					}

					logger.DebugF("failed to pull image %s: %v", imageMeta.TagReference, err)

					return fmt.Errorf("pull image metadata: %w", err)
				}

				img.SetMetadata(&registry.ImageMeta{
					TagReference:    imageMeta.TagReference,
					DigestReference: "@" + imageMeta.Digest.String(),
					Digest:          imageMeta.Digest,
				})

				err = imageSetLayout.AddImage(img, imageMeta.ImageTag)
				if err != nil {
					logger.DebugF("failed to add image %s: %v", imageMeta.ImageTag, err)

					return fmt.Errorf("add image to layout: %w", err)
				}

				return nil
			}))
		if err != nil {
			return fmt.Errorf("pull image %q: %w", imageMeta.TagReference, err)
		}

		pullCount++
	}

	return nil
}

func (svc *Service) generateDeckhouseReleaseManifests(
	tagsToMirror []string,
) error {
	svc.userLogger.InfoF("Generating DeckhouseRelease manifests")

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
) (map[string]struct{}, error) {
	logger := svc.userLogger

	logger.DebugF("Extracting images digests from Deckhouse installer %s", tag)

	img, err := svc.layout.DeckhouseInstall.GetImage(tag)
	if err != nil {
		return nil, fmt.Errorf("get installer image %q from layout: %w", tag, err)
	}

	result, err := extractDeckhouseReleaseMetadata(img.Extract())
	if err != nil {
		// TODO: handle error
	}

	_ = result

	tagsCompatMode := false
	imagesJSON, err := images.ExtractFileFromImage(img, "deckhouse/candi/images_digests.json")
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Older images had lists of deckhouse images tags instead of digests
		tagsCompatMode = true
		imagesJSON, err = images.ExtractFileFromImage(img, "deckhouse/candi/images_tags.json")
		if err != nil {
			return nil, fmt.Errorf("read tags from %q: %w", tag, err)
		}
	case err != nil:
		return nil, fmt.Errorf("read digests from %q: %w", tag, err)
	}

	images := map[string]struct{}{}
	if err = parseImagesFromJSON(svc.deckhouseService.GetRoot(), imagesJSON, images, tagsCompatMode); err != nil {
		return nil, fmt.Errorf("cannot parse images list from json: %w", err)
	}

	logger.InfoF("Deckhouse digests found: %d", len(images))

	logger.InfoF("Searching for VEX images")

	vex := make([]string, 0)
	const scanPrintInterval = 20
	counter := 0
	for image := range images {
		counter++
		if counter%scanPrintInterval == 0 {
			logger.InfoF("[%d / %d] Scanning images for VEX", counter, len(images))
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
			logger.DebugF("Vex image found %s", vexImageName)
			vex = append(vex, vexImageName)
		}

		prevDigests[image] = struct{}{}
		prevDigests[vexImageName] = struct{}{}
	}

	logger.InfoF("[%d / %d] Scanning images for VEX", counter, len(images))

	logger.InfoF("Deckhouse digests found: %d", len(images))
	logger.InfoF("VEX images found: %d", len(vex))

	for _, v := range vex {
		images[v] = struct{}{}
	}

	return images, nil
}

func extractDeckhouseReleaseMetadata(rc io.ReadCloser) (map[string]struct{}, error) {
	var meta = make(map[string]struct{}, 0)

	defer rc.Close()

	drr := &deckhouseInstallerReader{
		imageDigestsReader: bytes.NewBuffer(nil),
		imageTagsReader:    bytes.NewBuffer(nil),
	}

	err := drr.untarMetadata(rc)
	if err != nil {
		return nil, err
	}

	var tags map[string]any
	if drr.imageTagsReader.Len() > 0 {
		err = json.NewDecoder(drr.imageTagsReader).Decode(&tags)
		if err != nil {
			return nil, err
		}

		for _, v := range tags {
			if tagStr, ok := v.(string); ok {
				meta[tagStr] = struct{}{}
			}
		}

		return meta, nil
	}

	var digests map[string]any
	if drr.imageDigestsReader.Len() > 0 {
		err = json.NewDecoder(drr.imageDigestsReader).Decode(&digests)
		if err != nil {
			return nil, err
		}

		for _, v := range digests {
			if digestStr, ok := v.(string); ok {
				meta[digestStr] = struct{}{}
			}
		}

		return meta, nil
	}

	return nil, fmt.Errorf("both files is not found in installer")
}

func (svc *Service) FindVexImage(
	digest string,
) (string, error) {
	logger := svc.userLogger

	// vex image reference check
	vexImageName := strings.Replace(strings.Replace(digest, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"

	logger.DebugF("Checking vex image from %s", vexImageName)

	split := strings.SplitN(vexImageName, ":", 2)
	// imagePath := split[0]
	tag := split[1]

	// imageSegmentsRaw := strings.TrimPrefix(imagePath, svc.deckhouseService.GetRoot())
	// imageSegments := strings.Split(imageSegmentsRaw, "/")

	// for i, segment := range imageSegments {
	// 	client = client.WithSegment(segment)
	// 	logger.DebugF("Segment %d: %s", i, segment)
	// }

	err := svc.deckhouseService.CheckTagExists(context.TODO(), tag)
	if errors.Is(err, registry.ErrImageNotFound) {
		// Image not found, which is expected for non-vulnerable images
		return "", nil
	}

	return vexImageName, nil
}

func parseImagesFromJSON(registryRepo string, jsonDigests io.Reader, dst map[string]struct{}, tagsCompatMode bool) error {
	digestsByModule := map[string]map[string]string{}
	if err := json.NewDecoder(jsonDigests).Decode(&digestsByModule); err != nil {
		return fmt.Errorf("parse images from json: %w", err)
	}

	for _, nameDigestTuple := range digestsByModule {
		for _, imageID := range nameDigestTuple {
			if tagsCompatMode {
				dst[registryRepo+":"+imageID] = struct{}{}
				continue
			}

			dst[registryRepo+"@"+imageID] = struct{}{}
		}
	}

	return nil
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

func createOCIImageLayoutsForDeckhouse(
	rootFolder string,
	rootUrl string,
) (*ImageLayouts, error) {
	var err error

	layouts := NewImageLayouts(rootFolder, rootUrl)

	fsPath := rootFolder
	layoutPtr, err := createEmptyImageLayout(fsPath)
	if err != nil {
		return nil, fmt.Errorf("create OCI Image Layout at %s: %w", fsPath, err)
	}

	layouts.Deckhouse = registry.NewImageLayout(layoutPtr)

	mirrorTypes := []internal.MirrorType{
		internal.MirrorTypeDeckhouse,
		internal.MirrorTypeDeckhouseReleaseChannels,
		internal.MirrorTypeDeckhouseInstall,
		internal.MirrorTypeDeckhouseInstallStandalone,
	}

	for _, mtype := range mirrorTypes {
		fsPath = filepath.Join(rootFolder, internal.InstallSegmentByMirrorType(mtype))
		layoutPtr, err = createEmptyImageLayout(fsPath)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", fsPath, err)
		}

		layouts.setLayoutByMirrorType(mtype, registry.NewImageLayout(layoutPtr))
	}

	return layouts, nil
}

func createEmptyImageLayout(path string) (layout.Path, error) {
	layoutFilePath := filepath.Join(path, "oci-layout")
	indexFilePath := filepath.Join(path, "index.json")
	blobsPath := filepath.Join(path, "blobs")

	if err := os.MkdirAll(blobsPath, 0o755); err != nil {
		return "", fmt.Errorf("mkdir for blobs: %w", err)
	}

	layoutContents := ociLayout{ImageLayoutVersion: "1.0.0"}
	indexContents := indexSchema{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.index.v1+json",
	}

	rawJSON, err := json.MarshalIndent(indexContents, "", "    ")
	if err != nil {
		return "", fmt.Errorf("create index.json: %w", err)
	}
	if err = os.WriteFile(indexFilePath, rawJSON, 0o644); err != nil {
		return "", fmt.Errorf("create index.json: %w", err)
	}

	rawJSON, err = json.MarshalIndent(layoutContents, "", "    ")
	if err != nil {
		return "", fmt.Errorf("create oci-layout: %w", err)
	}
	if err = os.WriteFile(layoutFilePath, rawJSON, 0o644); err != nil {
		return "", fmt.Errorf("create oci-layout: %w", err)
	}

	return layout.Path(path), nil
}

type indexSchema struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	Manifests     []struct {
		MediaType string `json:"mediaType,omitempty"`
		Size      int    `json:"size,omitempty"`
		Digest    string `json:"digest,omitempty"`
	} `json:"manifests"`
}

type ociLayout struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}
