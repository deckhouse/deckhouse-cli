package puller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

// PullerService handles the pulling of images from the registry
type PullerService struct {
	deckhouseService *service.DeckhouseService
	logger           *dkplog.Logger
	userLogger       *log.SLogger
}

// NewPullerService creates a new PullerService
func NewPullerService(
	deckhouseService *service.DeckhouseService,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PullerService {
	return &PullerService{
		deckhouseService: deckhouseService,
		logger:           logger,
		userLogger:       userLogger,
	}
}

// PullImages pulls images according to the provided configuration
func (ps *PullerService) PullImages(ctx context.Context, config PullConfig) error {
	ps.userLogger.InfoLn("Beginning to pull " + config.Name)

	ps.userLogger.InfoLn("Pull " + config.Name + " meta")
	for image, meta := range config.ImageSet {
		if meta != nil {
			continue
		}

		_, tag := SplitImageRefByRepoAndTag(image)

		digest, err := config.GetterService.GetDigest(ctx, tag)
		if err != nil {
			if config.AllowMissingTags {
				continue
			}

			return fmt.Errorf("get digest: %w", err)
		}

		config.ImageSet[image] = NewImageMeta(tag, image, digest)
	}
	ps.userLogger.InfoLn("All required " + config.Name + " meta are pulled!")

	if err := ps.PullImageSet(ctx, config.ImageSet, config.Layout, config.AllowMissingTags, config.GetterService.GetImage); err != nil {
		return err
	}

	ps.userLogger.InfoLn("All required " + config.Name + " are pulled!")

	return nil
}

// PullImageSet pulls a set of images using the provided image getter
func (ps *PullerService) PullImageSet(
	ctx context.Context,
	imageSet map[string]*ImageMeta,
	imageSetLayout *registry.ImageLayout,
	allowMissingTags bool,
	imageGetter ImageGetter,
) error {
	logger := ps.userLogger

	pullCount, totalCount := 1, len(imageSet)

	for _, imageMeta := range imageSet {
		logger.DebugF("Preparing to pull image %s", imageMeta.TagReference)

		logger.DebugF("Pulling image path %s: tag %s", imageMeta.ImageRepo, imageMeta.ImageTag)

		err := retry.RunTask(
			ctx,
			ps.userLogger,
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
