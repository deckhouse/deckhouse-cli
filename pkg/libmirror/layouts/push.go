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
	"errors"
	"fmt"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/samber/lo"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/parallel"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
)

var ErrEmptyLayout = errors.New("No images in layout")

func PushLayoutToRepo(
	ctx context.Context,
	imagesLayout layout.Path,
	registryRepo string,
	authProvider authn.Authenticator,
	logger contexts.Logger,
	parallelismConfig contexts.ParallelismConfig,
	insecure, skipVerifyTLS bool,
) error {
	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)
	if parallelismConfig.Blobs != 0 {
		remoteOpts = append(remoteOpts, remote.WithJobs(parallelismConfig.Blobs))
	}

	index, err := imagesLayout.ImageIndex()
	if err != nil {
		return fmt.Errorf("Read OCI Image Index: %w", err)
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("Parse OCI Image Index Manifest: %w", err)
	}

	if len(indexManifest.Manifests) == 0 {
		return fmt.Errorf("%s: %w", registryRepo, ErrEmptyLayout)
	}

	batches := lo.Chunk(indexManifest.Manifests, parallelismConfig.Images)
	batchesCount, imagesCount := 1, 1

	for _, manifestSet := range batches {
		if parallelismConfig.Images == 1 {
			tag := manifestSet[0].Annotations["io.deckhouse.image.short_tag"]
			imageRef := registryRepo + ":" + tag
			logger.InfoF("[%d / %d] Pushing image %s", imagesCount, len(indexManifest.Manifests), imageRef)

			ctx, ctxCancel := context.WithCancel(ctx)
			if err := pushImage(ctx, ctxCancel, logger, registryRepo, index, imagesCount, refOpts, remoteOpts)(manifestSet[0], 0); err != nil {
				return err
			}
			imagesCount += 1
			continue
		}

		err = logger.Process(fmt.Sprintf("Pushing batch %d / %d", batchesCount, len(batches)), func() error {
			logger.InfoLn("Images in batch:")
			for _, manifest := range manifestSet {
				logger.InfoF("- %s", registryRepo+":"+manifest.Annotations["io.deckhouse.image.short_tag"])
			}

			ctx, ctxCancel := context.WithCancel(ctx)
			return parallel.ForEachWithErrors(manifestSet, pushImage(ctx, ctxCancel, logger, registryRepo, index, imagesCount, refOpts, remoteOpts))
		})
		if err != nil {
			return fmt.Errorf("Push batch of images: %w", err)
		}
		batchesCount += 1
		imagesCount += len(manifestSet)
	}

	return nil
}

func pushImage(
	ctx context.Context,
	ctxCancel context.CancelFunc,
	logger contexts.Logger,
	registryRepo string,
	index v1.ImageIndex,
	imagesCount int,
	refOpts []name.Option,
	remoteOpts []remote.Option,
) func(v1.Descriptor, int) error {
	return func(manifest v1.Descriptor, _ int) error {
		tag := manifest.Annotations["io.deckhouse.image.short_tag"]
		imageRef := registryRepo + ":" + tag
		img, err := index.Image(manifest.Digest)
		if err != nil {
			logger.WarnF("Read image: %v", err)
			ctxCancel()
			return err
		}
		ref, err := name.ParseReference(imageRef, refOpts...)
		if err != nil {
			logger.WarnF("Parse image reference: %v", err)
			ctxCancel()
			return err
		}

		err = retry.RunTask(ctx, silentLogger{}, "", task.WithConstantRetries(19, 3*time.Second, func(ctx context.Context) error {
			remoteOpts := remoteOpts
			remoteOpts = append(remoteOpts, remote.WithContext(ctx))
			if err = remote.Write(ref, img, remoteOpts...); err != nil {
				if errorutil.IsTrivyMediaTypeNotAllowedError(err) {
					logger.WarnLn(errorutil.CustomTrivyMediaTypesWarning)
					ctxCancel()
					return err
				}
				return fmt.Errorf("Write %s to registry: %w", ref.String(), err)
			}
			return nil
		}))
		if err != nil {
			logger.WarnF("Push image: %v", err)
			ctxCancel()
			return err
		}

		imagesCount += 1
		return nil
	}
}

type silentLogger struct{}

var _ contexts.Logger = silentLogger{}

func (silentLogger) DebugF(_ string, _ ...interface{})      {}
func (silentLogger) DebugLn(_ ...interface{})               {}
func (silentLogger) InfoF(_ string, _ ...interface{})       {}
func (silentLogger) InfoLn(_ ...interface{})                {}
func (silentLogger) WarnF(_ string, _ ...interface{})       {}
func (silentLogger) WarnLn(_ ...interface{})                {}
func (silentLogger) Process(_ string, _ func() error) error { return nil }
