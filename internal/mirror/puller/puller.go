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
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

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

	if err := ps.PullImageSet(ctx, config.ImageSet, config.Layout, config.GetterService.GetImage); err != nil {
		return err
	}

	ps.userLogger.InfoLn("All required " + config.Name + " are pulled!")

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

				img, err := imageGetter(ctx, "@"+imageMeta.Digest.String())
				if err != nil {
					logger.Debugf("failed to pull image %s: %v", imageMeta.TagReference, err)

					return fmt.Errorf("pull image %s (digest %s): %w", imageMeta.TagReference, imageMeta.Digest.String(), err)
				}

				img.SetMetadata(&image.ImageMeta{
					TagReference:    imageMeta.TagReference,
					DigestReference: "@" + imageMeta.Digest.String(),
					Digest:          imageMeta.Digest,
				})

				err = imageSetLayout.AddImage(img, imageMeta.ImageTag)
				if err != nil {
					logger.Debugf("failed to add image %s: %v", imageMeta.ImageTag, err)

					return fmt.Errorf("add image %s to layout: %w", imageMeta.ImageTag, err)
				}

				return nil
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

	return nil
}
