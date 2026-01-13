/*
Copyright 2024 Flant JSC

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

package layouts

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
	"github.com/samber/lo/parallel"

	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
)

func PushLayoutToRepo(
	client registry.Client,
	imagesLayout layout.Path,
	registryRepo string,
	authProvider authn.Authenticator,
	logger params.Logger,
	parallelismConfig params.ParallelismConfig,
	insecure, skipVerifyTLS bool,
) error {
	return PushLayoutToRepoContext(
		context.Background(),
		client,
		imagesLayout,
		registryRepo,
		authProvider,
		logger,
		parallelismConfig,
		insecure,
		skipVerifyTLS,
	)
}

func PushLayoutToRepoContext(
	ctx context.Context,
	client registry.Client,
	imagesLayout layout.Path,
	registryRepo string,
	authProvider authn.Authenticator,
	logger params.Logger,
	parallelismConfig params.ParallelismConfig,
	insecure, skipVerifyTLS bool,
) error {
	refOpts, _ := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)

	index, err := imagesLayout.ImageIndex()
	if err != nil {
		return fmt.Errorf("Read OCI Image Index: %w", err)
	}

	indexManifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("Parse OCI Image Index Manifest: %w", err)
	}

	if len(indexManifest.Manifests) == 0 {
		return nil
	}

	batches := lo.Chunk(indexManifest.Manifests, parallelismConfig.Images)
	batchesCount, imagesCount := 1, 1

	for _, manifestSet := range batches {
		if parallelismConfig.Images == 1 {
			cfg := &pushImageConfig{
				client:       client,
				registryRepo: registryRepo,
				index:        index,
				manifest:     manifestSet[0],
				refOpts:      refOpts,
				logger:       logger,
				imageNum:     imagesCount,
				totalImages:  len(indexManifest.Manifests),
			}
			if err = pushImage(ctx, cfg); err != nil {
				return fmt.Errorf("Push Image: %w", err)
			}
			imagesCount++
			continue
		}

		err = logger.Process(fmt.Sprintf("Pushing batch %d / %d", batchesCount, len(batches)), func() error {
			logger.InfoLn("Images in batch:")
			for _, manifest := range manifestSet {
				tag := manifest.Annotations["io.deckhouse.image.short_tag"]
				logger.Infof("- %s", registryRepo+":"+tag)
			}

			errMu := &sync.Mutex{}
			merr := &multierror.Error{}
			currentImagesCount := imagesCount
			parallel.ForEach(manifestSet, func(item v1.Descriptor, idx int) {
				imageNum := currentImagesCount + idx
				cfg := &pushImageConfig{
					client:       client,
					registryRepo: registryRepo,
					index:        index,
					manifest:     item,
					refOpts:      refOpts,
					logger:       logger,
					imageNum:     imageNum,
					totalImages:  len(indexManifest.Manifests),
				}
				if err = pushImage(ctx, cfg); err != nil {
					errMu.Lock()
					defer errMu.Unlock()
					merr = multierror.Append(merr, err)
				}
			})

			return merr.ErrorOrNil()
		})
		if err != nil {
			return fmt.Errorf("Push batch of images: %w", err)
		}
		batchesCount++
		imagesCount += len(manifestSet)
	}

	return nil
}

type pushImageConfig struct {
	client       registry.Client
	registryRepo string
	index        v1.ImageIndex
	manifest     v1.Descriptor
	refOpts      []name.Option
	logger       params.Logger
	imageNum     int
	totalImages  int
}

func pushImage(ctx context.Context, cfg *pushImageConfig) error {
	tag := cfg.manifest.Annotations["io.deckhouse.image.short_tag"]
	imageRef := cfg.registryRepo + ":" + tag
	img, err := cfg.index.Image(cfg.manifest.Digest)
	if err != nil {
		return fmt.Errorf("Read image: %v", err)
	}
	ref, err := name.ParseReference(imageRef, cfg.refOpts...)
	if err != nil {
		return fmt.Errorf("Parse image reference: %v", err)
	}

	err = retry.RunTask(
		ctx,
		cfg.logger,
		fmt.Sprintf("[%d / %d] Pushing %s", cfg.imageNum, cfg.totalImages, imageRef),
		task.WithConstantRetries(4, 3*time.Second, func(ctx context.Context) error {
			if err = cfg.client.PushImage(ctx, tag, img); err != nil {
				if errorutil.IsTrivyMediaTypeNotAllowedError(err) {
					return fmt.Errorf(errorutil.CustomTrivyMediaTypesWarning)
				}
				return fmt.Errorf("Write %s to registry: %w", ref.String(), err)
			}
			return nil
		}))
	if err != nil {
		return fmt.Errorf("Run push task: %v", err)
	}
	return nil
}
