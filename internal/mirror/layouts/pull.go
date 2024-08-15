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
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/contexts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/retry"
)

func PullInstallers(mirrorCtx *contexts.PullContext, layouts *ImageLayouts) error {
	log.InfoLn("Beginning to pull installers")
	if err := PullImageSet(
		mirrorCtx,
		layouts.Install,
		layouts.InstallImages,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
	); err != nil {
		return err
	}
	log.InfoLn("✅ All required installers are pulled!")
	return nil
}

func PullDeckhouseReleaseChannels(mirrorCtx *contexts.PullContext, layouts *ImageLayouts) error {
	log.InfoLn("Beginning to pull Deckhouse release channels information")
	if err := PullImageSet(
		mirrorCtx,
		layouts.ReleaseChannel,
		layouts.ReleaseChannelImages,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
		WithAllowMissingTags(mirrorCtx.SpecificVersion != nil),
	); err != nil {
		return err
	}
	log.InfoLn("✅ Deckhouse release channels are pulled!")
	return nil
}

func PullDeckhouseImages(mirrorCtx *contexts.PullContext, layouts *ImageLayouts) error {
	log.InfoLn("Beginning to pull Deckhouse, this may take a while")
	if err := PullImageSet(
		mirrorCtx,
		layouts.Deckhouse,
		layouts.DeckhouseImages,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
	); err != nil {
		return err
	}
	log.InfoLn("✅ All required Deckhouse images are pulled!")
	return nil
}

func PullModules(mirrorCtx *contexts.PullContext, layouts *ImageLayouts) error {
	log.InfoLn("Beginning to pull Deckhouse modules")
	for moduleName, moduleData := range layouts.Modules {
		if err := PullImageSet(
			mirrorCtx,
			moduleData.ModuleLayout,
			moduleData.ModuleImages,
			WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
		); err != nil {
			return fmt.Errorf("pull %q module: %w", moduleName, err)
		}
		if err := PullImageSet(
			mirrorCtx,
			moduleData.ReleasesLayout,
			moduleData.ReleaseImages,
			WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
			WithAllowMissingTags(true),
		); err != nil {
			return fmt.Errorf("pull %q module release information: %w", moduleName, err)
		}
	}
	log.InfoLn("✅ Deckhouse modules pulled!")
	return nil
}

func PullTrivyVulnerabilityDatabasesImages(
	pullCtx *contexts.PullContext,
	layouts *ImageLayouts,
) error {
	nameOpts, _ := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(&pullCtx.BaseContext)

	dbImages := map[layout.Path]string{
		layouts.TrivyDB:     path.Join(pullCtx.DeckhouseRegistryRepo, "security", "trivy-db:2"),
		layouts.TrivyBDU:    path.Join(pullCtx.DeckhouseRegistryRepo, "security", "trivy-bdu:1"),
		layouts.TrivyJavaDB: path.Join(pullCtx.DeckhouseRegistryRepo, "security", "trivy-java-db:1"),
	}

	for dbImageLayout, imageRef := range dbImages {
		ref, err := name.ParseReference(imageRef, nameOpts...)
		if err != nil {
			return fmt.Errorf("parse trivy-db reference %q: %w", imageRef, err)
		}

		if err = PullImageSet(
			pullCtx,
			dbImageLayout,
			map[string]struct{}{ref.String(): {}},
			WithTagToDigestMapper(NopTagToDigestMappingFunc),
			WithAllowMissingTags(true), // SE edition does not contain images for trivy
		); err != nil {
			return fmt.Errorf("pull vulnerability database: %w", err)
		}
	}

	return nil
}

func PullImageSet(
	pullCtx *contexts.PullContext,
	targetLayout layout.Path,
	imageSet map[string]struct{},
	opts ...func(opts *pullImageSetOptions),
) error {
	pullOpts := &pullImageSetOptions{}
	for _, o := range opts {
		o(pullOpts)
	}

	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(pullCtx.RegistryAuth, pullCtx.Insecure, pullCtx.SkipTLSVerification)

	pullCount, totalCount := 1, len(imageSet)
	for imageReferenceString := range imageSet {
		imageRepo, imageTag := splitImageRefByRepoAndTag(imageReferenceString)

		// If we already know the digest of the tagged image, we should pull it by this digest instead of pulling by tag
		// to avoid race-conditions between mirroring and releasing new builds on release channels.
		pullReference := imageReferenceString
		if pullOpts.tagToDigestMapper != nil {
			if mapping := pullOpts.tagToDigestMapper(imageReferenceString); mapping != nil {
				pullReference = imageRepo + "@" + mapping.String()
			}
		}

		ref, err := name.ParseReference(pullReference, nameOpts...)
		if err != nil {
			return fmt.Errorf("parse image reference %q: %w", pullReference, err)
		}

		err = retry.NewLoop(
			fmt.Sprintf("[%d / %d] Pulling %s...", pullCount, totalCount, imageReferenceString),
			6, 10*time.Second,
		).Run(func() error {
			img, err := remote.Image(ref, remoteOpts...)
			if err != nil {
				if errorutil.IsImageNotFoundError(err) && pullOpts.allowMissingTags {
					log.WarnLn("⚠️ Not found in registry, skipping pull")
					return nil
				}

				return fmt.Errorf("pull image metadata: %w", err)
			}

			err = targetLayout.AppendImage(img,
				layout.WithPlatform(v1.Platform{Architecture: "amd64", OS: "linux"}),
				layout.WithAnnotations(map[string]string{
					"org.opencontainers.image.ref.name": imageReferenceString,
					"io.deckhouse.image.short_tag":      imageTag,
				}),
			)
			if err != nil {
				return fmt.Errorf("write image to index: %w", err)
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("pull image %q: %w", imageReferenceString, err)
		}
		pullCount++
	}
	return nil
}

func splitImageRefByRepoAndTag(imageReferenceString string) (repo, tag string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo = imageReferenceString[:splitIndex]
	tag = imageReferenceString[splitIndex+1:]
	return
}

type pullImageSetOptions struct {
	tagToDigestMapper TagToDigestMappingFunc
	allowMissingTags  bool
}

func WithAllowMissingTags(allow bool) func(opts *pullImageSetOptions) {
	return func(opts *pullImageSetOptions) {
		opts.allowMissingTags = allow
	}
}

type TagToDigestMappingFunc func(imageRef string) *v1.Hash

func WithTagToDigestMapper(fn TagToDigestMappingFunc) func(opts *pullImageSetOptions) {
	return func(opts *pullImageSetOptions) {
		opts.tagToDigestMapper = fn
	}
}
