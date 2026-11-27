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

package puller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// isContextErr reports whether err is one of the context cancellation errors,
// either directly or wrapped. These errors must never be silently swallowed by
// AllowMissingTags: doing so converts a Ctrl+C / timeout into a "tag not
// found" no-op for every subsequent image and module, which in turn causes the
// caller to produce stub artifacts (empty bundle tars) for downloads that
// never actually completed.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// PullerService handles the pulling of images from the registry
type PullerService struct {
	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewPullerService creates a new PullerService
func NewPullerService(
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PullerService {
	return &PullerService{
		logger:     logger,
		userLogger: userLogger,
	}
}

// PullImages pulls images according to the provided configuration
func (ps *PullerService) PullImages(ctx context.Context, config PullConfig) error {
	ps.userLogger.InfoLn("Beginning to pull " + config.Name)

	if err := ps.pullMeta(ctx, config); err != nil {
		return err
	}

	if err := ps.PullImageSet(ctx, config.ImageSet, config.Layout, config.GetterService.GetImage); err != nil {
		return err
	}

	ps.userLogger.InfoLn("All required " + config.Name + " are pulled!")

	return nil
}

// PullImagesWithOptionalVEX preserves the best-effort attestation policy for modules and packages.
func (ps *PullerService) PullImagesWithOptionalVEX(ctx context.Context, config PullConfig, includeVEX bool) error {
	if err := ps.PullImages(ctx, config); err != nil {
		return err
	}

	if includeVEX {
		return ps.pullVEX(ctx, config, true)
	}

	return nil
}

// PullVEX adds the attestations of the downloaded subjects to the same repository layout.
func (ps *PullerService) PullVEX(ctx context.Context, config PullConfig) error {
	return ps.pullVEX(ctx, config, false)
}

func (ps *PullerService) pullVEX(ctx context.Context, subjects PullConfig, bestEffort bool) error {
	images := make(map[string]*ImageMeta)
	indexes := make(map[string]*ImageMeta)

	total := 0

	for _, meta := range subjects.ImageSet {
		if meta != nil {
			total++
		}
	}

	if total == 0 {
		return ctx.Err()
	}

	ps.userLogger.InfoLn("Searching for VEX images")

	scanned := 0

	for imageRef, meta := range subjects.ImageSet {
		if meta == nil {
			continue
		}

		scanned++
		if scanned%vexLookupLogInterval == 0 {
			ps.userLogger.Infof("[%d / %d] Scanning images for VEX", scanned, total)
		}

		refs, err := ps.findVEX(ctx, subjects, imageRef, meta, bestEffort)
		if err != nil {
			return err
		}

		for _, ref := range refs {
			_, tag := SplitImageRefByRepoAndTag(ref.Name)

			found := NewImageMeta(tag, ref.Name, ref.Digest)
			if ref.Index {
				indexes[ref.Name] = found
			} else {
				images[ref.Name] = found
			}
		}
	}

	if total%vexLookupLogInterval != 0 {
		ps.userLogger.Infof("[%d / %d] Scanning images for VEX", total, total)
	}

	ps.userLogger.Infof("VEX images found: %d", len(images)+len(indexes))

	if len(images) > 0 {
		if err := ps.PullImageSet(ctx, images, subjects.Layout, subjects.GetterService.GetImage); err != nil {
			if !bestEffort || isContextErr(err) {
				return err
			}

			ps.userLogger.Debugf("Failed to pull VEX images for %s: %v", subjects.Name, err)
		}
	}

	if len(indexes) > 0 {
		config := subjects
		config.Name += " VEX indexes"

		config.ImageSet = indexes
		if err := ps.PullIndexes(ctx, config); err != nil {
			if !bestEffort || isContextErr(err) {
				return err
			}

			ps.userLogger.Debugf("Failed to pull VEX indexes for %s: %v", subjects.Name, err)
		}
	}

	return ctx.Err()
}

// The VEX lookup logs its progress once per this many images.
const vexLookupLogInterval = 20

// Retry policy for the VEX lookup, the same as for image downloads.
// Package vars so tests can shrink the delay.
var (
	vexLookupRetries    = uint(5)
	vexLookupRetryDelay = 10 * time.Second
)

// The source index and the selected platform can have independent attestations.
func (ps *PullerService) findVEX(
	ctx context.Context,
	subjects PullConfig,
	imageRef string,
	meta *ImageMeta,
	bestEffort bool,
) ([]vex.Ref, error) {
	digests := []v1.Hash{*meta.Digest}
	if child := meta.selectedDigest; child != nil && *child != *meta.Digest {
		digests = append(digests, *child)
	}

	var refs []vex.Ref

	for _, digest := range digests {
		err := retry.RunQuietTask(
			ctx,
			ps.userLogger,
			"Searching for VEX of "+imageRef+" ("+digest.String()+")",
			task.WithConstantRetries(vexLookupRetries, vexLookupRetryDelay, func(ctx context.Context) error {
				found, err := findSubjectVEX(ctx, subjects, imageRef, meta, digest)
				if err != nil {
					return err
				}

				refs = append(refs, found...)

				return nil
			}))
		if err != nil {
			if !bestEffort || isContextErr(err) {
				return nil, err
			}

			ps.userLogger.Debugf("Skipping VEX for %s (%s): %v", imageRef, digest, err)
		}
	}

	return refs, nil
}

func findSubjectVEX(ctx context.Context, subjects PullConfig, imageRef string, meta *ImageMeta, digest v1.Hash) ([]vex.Ref, error) {
	if digest != *meta.Digest {
		return vex.FindByDigest(ctx, subjects.GetterService, imageRef, digest)
	}

	found, err := vex.Find(ctx, subjects.GetterService, imageRef, digest)
	if err != nil || len(found) == 0 {
		return nil, err
	}

	return found, preserveVEXSubject(ctx, subjects, meta)
}

// An attestation may refer to the whole source index even when GetImage selected one platform.
func preserveVEXSubject(ctx context.Context, subjects PullConfig, meta *ImageMeta) error {
	stored, err := subjects.Layout.GetMeta(meta.ImageTag)
	if err != nil {
		return fmt.Errorf("get VEX subject %s: %w", meta.TagReference, err)
	}

	if *stored.Digest == *meta.Digest {
		return nil
	}

	index, err := subjects.GetterService.GetIndex(ctx, meta.Digest.String())
	if err != nil {
		return fmt.Errorf("get VEX subject index %s: %w", meta.Digest, err)
	}

	return subjects.Layout.PreserveSubjectIndex(index, meta.ImageTag, meta.TagReference)
}

// PullIndexes is PullImages for references that name image indexes, such as
// the referrers indexes of VEX attestations. Each index is stored whole and
// byte for byte, with all its children, instead of being resolved to the
// image of one platform: the children of a referrers index carry no platform,
// and consumers find them by the artifactType and annotations of their
// descriptors, which a rebuilt index would not keep.
func (ps *PullerService) PullIndexes(ctx context.Context, config PullConfig) error {
	ps.userLogger.InfoLn("Beginning to pull " + config.Name)

	if err := ps.pullMeta(ctx, config); err != nil {
		return err
	}

	if err := ps.PullIndexSet(ctx, config.ImageSet, config.Layout, config.GetterService.GetIndex); err != nil {
		return err
	}

	ps.userLogger.InfoLn("All required " + config.Name + " are pulled!")

	return nil
}

// pullMeta resolves the digest of every image of config.ImageSet that has no
// metadata yet. With AllowMissingTags, an image whose tag does not resolve
// keeps nil metadata, and the pull skips it.
func (ps *PullerService) pullMeta(ctx context.Context, config PullConfig) error {
	ps.userLogger.InfoLn("Pull " + config.Name + " meta")

	for image, meta := range config.ImageSet {
		// Bail out fast on cancellation. Without this check, a Ctrl+C in the
		// middle of a large pull would cause every subsequent GetDigest call
		// to fail with context.Canceled, which AllowMissingTags would then
		// swallow - turning real cancellation into "tag not found" for every
		// remaining image and module.
		if err := ctx.Err(); err != nil {
			return err
		}

		if meta != nil {
			continue
		}

		_, tag := SplitImageRefByRepoAndTag(image)

		// Check if this is a digest reference (repo@sha256:abc...)
		// For digest references, we already know the digest - it's in the reference itself
		if strings.Contains(image, "@sha256:") {
			// Extract digest from reference
			digestStr := image[strings.Index(image, "@sha256:")+1:] // "sha256:abc..."

			digest, err := v1.NewHash(digestStr)
			if err != nil {
				ps.userLogger.Debugf("failed to parse digest from %s: %v", image, err)

				if config.AllowMissingTags {
					continue
				}

				return fmt.Errorf("parse digest from reference %s: %w", image, err)
			}

			config.ImageSet[image] = NewImageMeta(tag, image, &digest)

			continue
		}

		digest, err := config.GetterService.GetDigest(ctx, tag)
		if err != nil {
			// AllowMissingTags should only mask "tag not found" style errors,
			// not cancellation: see comment on isContextErr.
			if isContextErr(err) {
				return err
			}

			if config.AllowMissingTags {
				continue
			}

			return fmt.Errorf("get digest for image %s: %w", tag, err)
		}

		config.ImageSet[image] = NewImageMeta(tag, image, digest)
	}

	ps.userLogger.InfoLn("All required " + config.Name + " meta are pulled!")

	return nil
}

// PullImageSet pulls a set of images using the provided image getter
func (ps *PullerService) PullImageSet(
	ctx context.Context,
	imageSet map[string]*ImageMeta,
	imageSetLayout *image.ImageLayout,
	imageGetter ImageGetter,
) error {
	logger := ps.userLogger

	return ps.pullSet(ctx, imageSet, imageSetLayout, func(ctx context.Context, imageMeta *ImageMeta) error {
		img, err := imageGetter(ctx, "@"+imageMeta.Digest.String())
		if err != nil {
			logger.Debugf("failed to pull image %s: %v", imageMeta.TagReference, err)

			return fmt.Errorf("pull image %s (digest %s): %w", imageMeta.TagReference, imageMeta.Digest.String(), err)
		}

		digest, err := img.Digest()
		if err != nil {
			return fmt.Errorf("get manifest digest for %s: %w", imageMeta.TagReference, err)
		}

		img.SetMetadata(&image.ImageMeta{
			TagReference:    imageMeta.TagReference,
			DigestReference: "@" + digest.String(),
			Digest:          &digest,
		})

		err = imageSetLayout.AddImage(img, imageMeta.ImageTag)
		if err != nil {
			logger.Debugf("failed to add image %s: %v", imageMeta.ImageTag, err)

			return fmt.Errorf("add image %s to layout: %w", imageMeta.ImageTag, err)
		}

		imageMeta.selectedDigest = &digest

		return nil
	})
}

// PullIndexSet is PullImageSet for image indexes, see PullIndexes.
func (ps *PullerService) PullIndexSet(
	ctx context.Context,
	imageSet map[string]*ImageMeta,
	imageSetLayout *image.ImageLayout,
	indexGetter IndexGetter,
) error {
	logger := ps.userLogger

	return ps.pullSet(ctx, imageSet, imageSetLayout, func(ctx context.Context, imageMeta *ImageMeta) error {
		// A bare digest, not "@"+digest as for images: the upstream fake
		// client resolves only this form in GetIndex, the real one both.
		idx, err := indexGetter(ctx, imageMeta.Digest.String())
		if err != nil {
			logger.Debugf("failed to pull index %s: %v", imageMeta.TagReference, err)

			return fmt.Errorf("pull index %s (digest %s): %w", imageMeta.TagReference, imageMeta.Digest.String(), err)
		}

		err = imageSetLayout.AddIndex(idx, imageMeta.ImageTag, imageMeta.TagReference)
		if err != nil {
			logger.Debugf("failed to add index %s: %v", imageMeta.ImageTag, err)

			return fmt.Errorf("add index %s to layout: %w", imageMeta.ImageTag, err)
		}

		return nil
	})
}

// pullSet stores every image of imageSet in imageSetLayout with store, which
// gets each image's metadata, and then checks that none went missing.
func (ps *PullerService) pullSet(
	ctx context.Context,
	imageSet map[string]*ImageMeta,
	imageSetLayout *image.ImageLayout,
	store func(ctx context.Context, imageMeta *ImageMeta) error,
) error {
	logger := ps.userLogger

	pullCount, totalCount := 1, len(imageSet)

	for imageReference, imageMeta := range imageSet {
		// Bail out on cancellation between images so we don't waste retry
		// budget on a doomed operation.
		if err := ctx.Err(); err != nil {
			return err
		}

		logger.Debugf("Preparing to pull image %s", imageReference)

		err := retry.RunTask(
			ctx,
			ps.userLogger,
			fmt.Sprintf("[%d / %d] Pulling %s ", pullCount, totalCount, imageReference),
			task.WithConstantRetries(5, 10*time.Second, func(ctx context.Context) error {
				if imageMeta == nil {
					logger.WarnLn("⚠️ Not found in registry, skipping pull")

					return nil
				}

				return store(ctx, imageMeta)
			}))
		if err != nil {
			ref := imageReference
			if imageMeta != nil {
				ref = imageMeta.TagReference
			}

			return fmt.Errorf("pull image %q: %w", ref, err)
		}

		pullCount++
	}

	return verifyPlannedImagesLanded(imageSet, imageSetLayout)
}

// verifyPlannedImagesLanded cross-checks the pull plan against the layout:
// every planned image must be present in index.json under its short tag.
// Defense in depth against silent-loss bugs - an incomplete bundle must fail
// the pull instead of surfacing later as ImagePullBackOff in the cluster.
// Nil metas are skipped: those images were never pulled (AllowMissingTags).
func verifyPlannedImagesLanded(imageSet map[string]*ImageMeta, imageSetLayout *image.ImageLayout) error {
	planned := make([]*ImageMeta, 0, len(imageSet))
	for _, meta := range imageSet {
		if meta != nil {
			planned = append(planned, meta)
		}
	}

	if len(planned) == 0 {
		return nil
	}

	index, err := imageSetLayout.Path().ImageIndex()
	if err != nil {
		return fmt.Errorf("verify pulled layout: read image index: %w", err)
	}

	indexManifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("verify pulled layout: parse index manifest: %w", err)
	}

	present := make(map[string]struct{}, len(indexManifest.Manifests))
	for _, desc := range indexManifest.Manifests {
		if tag, ok := desc.Annotations[image.AnnotationImageShortTag]; ok {
			present[tag] = struct{}{}
		}
	}

	var missing []string

	for _, meta := range planned {
		if _, found := present[meta.ImageTag]; !found {
			missing = append(missing, meta.TagReference)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	sort.Strings(missing)

	return fmt.Errorf("pulled layout is incomplete: %d of %d planned images missing: %s",
		len(missing), len(planned), strings.Join(missing, ", "))
}
