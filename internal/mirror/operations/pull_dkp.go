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

package operations

import (
	"context"
	"errors"
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

	logger.Infof("Creating OCI Image Layouts")
	imageLayouts, err := layouts.CreateOCIImageLayoutsForDeckhouse(tmpDir, nil)
	if err != nil {
		return fmt.Errorf("Create OCI Image Layouts: %w", err)
	}

	layouts.FillLayoutsWithBasicDeckhouseImages(pullParams, imageLayouts, tagsToMirror)
	logger.Infof("Resolving tags")
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

	// We should not generate deckhousereleases.yaml manifest for tag-based pulls
	if pullParams.DeckhouseTag == "" {
		if err = generateDeckhouseReleaseManifests(pullParams, tagsToMirror, imageLayouts, logger); err != nil {
			logger.WarnLn(err.Error())
		}
	}

	logger.Infof("Searching for Deckhouse built-in modules digests")
	for imageTag := range imageLayouts.InstallImages {
		digests, err := images.ExtractImageDigestsFromDeckhouseInstaller(pullParams, imageTag, imageLayouts.Install)
		if err != nil {
			return fmt.Errorf("Extract images digests: %w", err)
		}
		maps.Copy(imageLayouts.DeckhouseImages, digests)
	}
	logger.Infof("Found %d images", len(imageLayouts.DeckhouseImages))

	if err = logger.Process("Pull Deckhouse images", func() error {
		return layouts.PullDeckhouseImages(pullParams, imageLayouts)
	}); err != nil {
		return fmt.Errorf("Pull Deckhouse images: %w", err)
	}

	err = logger.Process("Processing image indexes", func() error {
		if pullParams.DeckhouseTag != "" {
			// If we are pulling some build by tag, propagate release channel image of it to all channels if it exists.
			releaseChannel, err := layouts.FindImageDescriptorByTag(imageLayouts.ReleaseChannel, pullParams.DeckhouseTag)
			switch {
			case errors.Is(err, layouts.ErrImageNotFound):
				logger.WarnLn("Registry does not contain release channels, release channels images will not be added to bundle")
				goto sortManifests
			case err != nil:
				return fmt.Errorf("Find release-%s channel descriptor: %w", pullParams.DeckhouseTag, err)
			}

			for _, channel := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
				if err = layouts.TagImage(imageLayouts.ReleaseChannel, releaseChannel.Digest, channel); err != nil {
					return fmt.Errorf("tag release channel: %w", err)
				}
			}
		}

	sortManifests:
		for _, l := range imageLayouts.AsList() {
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
	logger.Infof("Generating DeckhouseRelease manifests")
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
