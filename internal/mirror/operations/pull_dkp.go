package operations

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/manifests"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

func PullDeckhousePlatform(pullParams *params.PullParams, tagsToMirror []string) error {
	logger := pullParams.Logger
	tmpDir := filepath.Join(pullParams.WorkingDir, "platform")

	logger.InfoF("Creating OCI Image Layouts")
	imageLayouts, err := layouts.CreateOCIImageLayoutsForDeckhouse(tmpDir, nil)
	if err != nil {
		return fmt.Errorf("Create OCI Image Layouts: %w", err)
	}

	layouts.FillLayoutsWithBasicDeckhouseImages(pullParams, imageLayouts, tagsToMirror)
	logger.InfoF("Resolving tags")
	if err = imageLayouts.TagsResolver.ResolveTagsDigestsForImageLayouts(&pullParams.BaseParams, imageLayouts); err != nil {
		return fmt.Errorf("Resolve images tags to digests: %w", err)
	}

	if err = logger.Process("Pull release channels and installers", func() error {
		if err = layouts.PullDeckhouseReleaseChannels(pullParams, imageLayouts); err != nil {
			return fmt.Errorf("Pull release channels: %w", err)
		}

		if err = layouts.PullInstallers(pullParams, imageLayouts); err != nil {
			return fmt.Errorf("Pull installers: %w", err)
		}

		if err = layouts.PullStandaloneInstallers(pullParams, imageLayouts); err != nil {
			return fmt.Errorf("Pull standalone installers: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	// We should not generate deckhousereleases.yaml manifest for bundles that don't have release channels
	if pullParams.DeckhouseTag == "" {
		if err = generateDeckhouseReleaseManifests(pullParams, tagsToMirror, imageLayouts, logger); err != nil {
			return err
		}
	}

	logger.InfoF("Searching for Deckhouse built-in modules digests")
	for imageTag := range imageLayouts.InstallImages {
		digests, err := images.ExtractImageDigestsFromDeckhouseInstaller(pullParams, imageTag, imageLayouts.Install)
		if err != nil {
			return fmt.Errorf("Extract images digests: %w", err)
		}
		maps.Copy(imageLayouts.DeckhouseImages, digests)
	}
	logger.InfoF("Found %d images", len(imageLayouts.DeckhouseImages))

	if err = logger.Process("Pull Deckhouse images", func() error {
		return layouts.PullDeckhouseImages(pullParams, imageLayouts)
	}); err != nil {
		return fmt.Errorf("Pull Deckhouse images: %w", err)
	}

	logger.InfoLn("Processing image indexes")
	for _, l := range imageLayouts.Layouts() {
		err = layouts.SortIndexManifests(l)
		if err != nil {
			return fmt.Errorf("Sorting index manifests of %s: %w", l, err)
		}
	}

	if err = logger.Process("Pack Deckhouse images into platform.tar", func() error {
		var platform io.Writer = chunked.NewChunkedFileWriter(
			pullParams.BundleChunkSize,
			pullParams.BundleDir,
			"platform.tar",
		)
		if pullParams.BundleChunkSize == 0 {
			platform, err = os.Create(filepath.Join(pullParams.BundleDir, "platform.tar"))
			if err != nil {
				return fmt.Errorf("Create platform.tar: %w", err)
			}
		}

		if err = bundle.Pack(context.Background(), tmpDir, platform); err != nil {
			return fmt.Errorf("Pack platform.tar: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func generateDeckhouseReleaseManifests(
	pullParams *params.PullParams,
	tagsToMirror []string,
	imageLayouts *layouts.ImageLayouts,
	logger params.Logger,
) error {
	logger.InfoF("Generating DeckhouseRelease manifests")
	deckhouseReleasesManifestFile := filepath.Join(pullParams.BundleDir, "deckhousereleases.yaml")
	if err := manifests.GenerateDeckhouseReleaseManifestsForVersions(
		tagsToMirror,
		deckhouseReleasesManifestFile,
		imageLayouts.ReleaseChannel,
	); err != nil {
		return fmt.Errorf("Generate DeckhouseRelease manifests: %w", err)
	}
	return nil
}
