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
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
)

// pullDeckhousePlatformDryRun resolves and prints platform images without pulling
// any blobs to disk. It streams images_digests.json directly from the remote
// installer image using ExtractFileFromImage (layer-by-layer, no OCI layout needed).
func (svc *Service) pullDeckhousePlatformDryRun(ctx context.Context, tagsToMirror []string) error {
	logger := svc.userLogger

	logger.Infof("Searching for Deckhouse built-in modules digests")

	var prevDigests = make(map[string]struct{})
	for _, tag := range tagsToMirror {
		logger.Infof("[dry-run] Streaming installer metadata for %s from registry", tag)

		digests, err := svc.extractImageDigestsFromRemote(ctx, tag, prevDigests)
		if err != nil {
			logger.Warnf("[dry-run] Could not extract images from installer %q: %v", tag, err)
			continue
		}

		maps.Copy(svc.downloadList.Deckhouse, digests)
	}

	totalImages := len(svc.downloadList.Deckhouse) +
		len(svc.downloadList.DeckhouseReleaseChannel) +
		len(svc.downloadList.DeckhouseInstall) +
		len(svc.downloadList.DeckhouseInstallStandalone)

	svc.userLogger.InfoLn("[dry-run] Platform images that would be pulled:")

	svc.userLogger.Infof("  Deckhouse components: %d images", len(svc.downloadList.Deckhouse))
	for _, ref := range slices.Sorted(maps.Keys(svc.downloadList.Deckhouse)) {
		svc.userLogger.InfoLn("    " + ref)
	}

	svc.userLogger.Infof("  Release channels: %d", len(svc.downloadList.DeckhouseReleaseChannel))
	for _, ref := range slices.Sorted(maps.Keys(svc.downloadList.DeckhouseReleaseChannel)) {
		svc.userLogger.InfoLn("    " + ref)
	}

	svc.userLogger.Infof("  Installer: %d", len(svc.downloadList.DeckhouseInstall))
	for _, ref := range slices.Sorted(maps.Keys(svc.downloadList.DeckhouseInstall)) {
		svc.userLogger.InfoLn("    " + ref)
	}

	svc.userLogger.Infof("  Standalone installer: %d", len(svc.downloadList.DeckhouseInstallStandalone))
	for _, ref := range slices.Sorted(maps.Keys(svc.downloadList.DeckhouseInstallStandalone)) {
		svc.userLogger.InfoLn("    " + ref)
	}

	svc.userLogger.Infof("  Total: %d platform images", totalImages)
	return nil
}

// extractImageDigestsFromRemote streams images_digests.json (or images_tags.json)
// directly from the remote installer image without saving the image to disk.
// Uses ExtractFileFromImage which downloads only the layer containing the target file.
func (svc *Service) extractImageDigestsFromRemote(
	ctx context.Context,
	tag string,
	prevDigests map[string]struct{},
) (map[string]*puller.ImageMeta, error) {
	img, err := svc.deckhouseService.Installer().GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("get remote installer image %q: %w", tag, err)
	}

	rootURL := svc.deckhouseService.GetRoot()
	result := make(map[string]*puller.ImageMeta)

	// Try images_tags.json first (preferred)
	tagsFile, err := images.ExtractFileFromImage(img, imagesTagsFile)
	if err == nil && tagsFile.Len() > 0 {
		var tags map[string]map[string]string
		if err := json.NewDecoder(tagsFile).Decode(&tags); err != nil {
			return nil, fmt.Errorf("decode %s: %w", imagesTagsFile, err)
		}
		for _, nameTagTuple := range tags {
			for _, imageID := range nameTagTuple {
				ref := rootURL + ":" + imageID
				if _, ok := prevDigests[ref]; !ok {
					prevDigests[ref] = struct{}{}
					result[ref] = nil
				}
			}
		}
		svc.userLogger.Infof("Deckhouse digests found: %d", len(result))
		return result, nil
	}

	// Fallback: images_digests.json
	digestsFile, err := images.ExtractFileFromImage(img, imagesDigestsFile)
	if err != nil {
		return nil, fmt.Errorf("extract %s from installer %q: %w", imagesDigestsFile, tag, err)
	}
	var digests map[string]map[string]string
	if err := json.NewDecoder(digestsFile).Decode(&digests); err != nil {
		return nil, fmt.Errorf("decode %s: %w", imagesDigestsFile, err)
	}
	for _, nameDigestTuple := range digests {
		for _, imageID := range nameDigestTuple {
			ref := rootURL + "@" + imageID
			if _, ok := prevDigests[ref]; !ok {
				prevDigests[ref] = struct{}{}
				result[ref] = nil
			}
		}
	}

	svc.userLogger.Infof("Deckhouse digests found: %d", len(result))
	return result, nil
}
