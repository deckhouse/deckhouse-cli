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
	"path"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse/pkg/registry"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
)

func PullInstallers(pullParams *params.PullParams, layouts *ImageLayouts, client registry.Client) error {
	pullParams.Logger.InfoLn("Beginning to pull installers")
	if err := PullImageSet(
		pullParams,
		layouts.Install,
		layouts.InstallImages,
		client,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
	); err != nil {
		return err
	}
	pullParams.Logger.InfoLn("All required installers are pulled!")
	return nil
}

func PullStandaloneInstallers(pullParams *params.PullParams, layouts *ImageLayouts, client registry.Client) error {
	pullParams.Logger.InfoLn("Beginning to pull standalone installers")
	if err := PullImageSet(
		pullParams,
		layouts.InstallStandalone,
		layouts.InstallStandaloneImages,
		client,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
		WithAllowMissingTags(true),
	); err != nil {
		return err
	}
	pullParams.Logger.InfoLn("All required standalone installers are pulled!")
	return nil
}

func PullDeckhouseReleaseChannels(pullParams *params.PullParams, layouts *ImageLayouts, client registry.Client) error {
	pullParams.Logger.InfoLn("Beginning to pull Deckhouse release channels information")
	if err := PullImageSet(
		pullParams,
		layouts.ReleaseChannel,
		layouts.ReleaseChannelImages,
		client,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
		WithAllowMissingTags(pullParams.DeckhouseTag != ""),
	); err != nil {
		return err
	}
	pullParams.Logger.InfoLn("Deckhouse release channels are pulled!")
	return nil
}

func PullDeckhouseImages(pullParams *params.PullParams, layouts *ImageLayouts, client registry.Client) error {
	pullParams.Logger.InfoLn("Beginning to pull Deckhouse, this may take a while")
	if err := PullImageSet(
		pullParams,
		layouts.Deckhouse,
		layouts.DeckhouseImages,
		client,
		WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
	); err != nil {
		return err
	}
	pullParams.Logger.InfoLn("All required Deckhouse images are pulled!")
	return nil
}

func PullModules(pullParams *params.PullParams, layouts *ImageLayouts, client registry.Client) error {
	for moduleName, moduleData := range layouts.Modules {
		// Skip main module images if only pulling extra images
		if !pullParams.OnlyExtraImages {
			pullParams.Logger.InfoLn(moduleName, "images")

			if err := PullImageSet(
				pullParams,
				moduleData.ModuleLayout,
				moduleData.ModuleImages,
				client,
				WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
			); err != nil {
				return fmt.Errorf("pull %q module: %w", moduleName, err)
			}

			pullParams.Logger.InfoLn(moduleName, "release channels")

			if err := PullImageSet(
				pullParams,
				moduleData.ReleasesLayout,
				moduleData.ReleaseImages,
				client,
				WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
				WithAllowMissingTags(true),
			); err != nil {
				return fmt.Errorf("pull %q module release information: %w", moduleName, err)
			}
		}

		// Always pull extra images if they exist
		if len(moduleData.ExtraNamedImages) > 0 {
			for extraName, imageSet := range moduleData.ExtraNamedImages {
				l := moduleData.ExtraNamedLayouts[extraName]

				pullParams.Logger.InfoLn(moduleName, "extra images")

				if err := PullImageSet(
					pullParams,
					l,
					imageSet,
					client,
					WithTagToDigestMapper(layouts.TagsResolver.GetTagDigest),
					WithAllowMissingTags(true),
				); err != nil {
					return fmt.Errorf("pull %q module extra images: %w", moduleName, err)
				}
			}
		}
	}

	message := "Deckhouse modules pulled!"
	if pullParams.OnlyExtraImages {
		message = "Extra images pulled!"
	}
	pullParams.Logger.InfoLn(message)
	return nil
}

func PullTrivyVulnerabilityDatabasesImages(
	pullParams *params.PullParams,
	layouts *ImageLayouts,
	client registry.Client,
) error {
	nameOpts, _ := auth.MakeRemoteRegistryRequestOptionsFromMirrorParams(&pullParams.BaseParams)

	dbImages := map[layout.Path]string{
		layouts.TrivyDB:     path.Join(pullParams.DeckhouseRegistryRepo, "security", "trivy-db:2"),
		layouts.TrivyBDU:    path.Join(pullParams.DeckhouseRegistryRepo, "security", "trivy-bdu:1"),
		layouts.TrivyJavaDB: path.Join(pullParams.DeckhouseRegistryRepo, "security", "trivy-java-db:1"),
		layouts.TrivyChecks: path.Join(pullParams.DeckhouseRegistryRepo, "security", "trivy-checks:0"),
	}

	for dbImageLayout, imageRef := range dbImages {
		ref, err := name.ParseReference(imageRef, nameOpts...)
		if err != nil {
			return fmt.Errorf("parse trivy-db reference %q: %w", imageRef, err)
		}

		if err = PullImageSet(
			pullParams,
			dbImageLayout,
			map[string]struct{}{ref.String(): {}},
			client,
			WithTagToDigestMapper(NopTagToDigestMappingFunc),
			WithAllowMissingTags(true), // SE edition does not contain images for trivy
		); err != nil {
			return fmt.Errorf("pull vulnerability database: %w", err)
		}
	}

	return nil
}

func PullImageSet(
	pullParams *params.PullParams,
	targetLayout layout.Path,
	imageSet map[string]struct{},
	client registry.Client,
	opts ...func(opts *pullImageSetOptions),
) error {
	logger := pullParams.Logger

	pullOpts := &pullImageSetOptions{}
	for _, o := range opts {
		o(pullOpts)
	}

	nameOpts, _ := auth.MakeRemoteRegistryRequestOptions(
		pullParams.RegistryAuth,
		pullParams.Insecure,
		pullParams.SkipTLSVerification,
	)

	pullCount, totalCount := 1, len(imageSet)
	for imageReferenceString := range imageSet {
		imageRepo, originalTag := splitImageRefByRepoAndTag(imageReferenceString)

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

		logger.Debugf("reference here: %s", ref.String())

		// tag here can be ussually tag or digest
		// thats why we need original tag calculated upper
		imagePath, tag := splitImageRefByRepoAndTag(pullReference)

		scopedClient := client
		imageSegmentsRaw := strings.TrimPrefix(imagePath, scopedClient.GetRegistry())
		imageSegments := strings.Split(imageSegmentsRaw, "/")

		for i, segment := range imageSegments {
			scopedClient = scopedClient.WithSegment(segment)
			logger.Debugf("Segment %d: %s", i, segment)
		}

		err = retry.RunTask(
			context.TODO(),
			pullParams.Logger,
			fmt.Sprintf("[%d / %d] Pulling %s ", pullCount, totalCount, imageReferenceString),
			task.WithConstantRetries(5, 10*time.Second, func(_ context.Context) error {
				img, err := scopedClient.GetImage(context.TODO(), tag)
				if err != nil {
					if errors.Is(err, regclient.ErrImageNotFound) && pullOpts.allowMissingTags {
						logger.WarnLn("⚠️ Not found in registry, skipping pull")
						return nil
					}

					logger.Debugf("failed to pull image %s:%s: %v", imageReferenceString, tag, err)

					return fmt.Errorf("pull image metadata: %w", err)
				}

				err = targetLayout.AppendImage(img,
					layout.WithPlatform(v1.Platform{Architecture: "amd64", OS: "linux"}),
					layout.WithAnnotations(map[string]string{
						"org.opencontainers.image.ref.name": imageReferenceString,
						// original tag here to have tagged releases as an alias
						"io.deckhouse.image.short_tag": originalTag,
					}),
				)
				if err != nil {
					return fmt.Errorf("write image to index: %w", err)
				}

				return nil
			}))
		if err != nil {
			return fmt.Errorf("pull image %q: %w", imageReferenceString, err)
		}
		pullCount++
	}
	return nil
}

func splitImageRefByRepoAndTag(imageReferenceString string) (string, string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo := imageReferenceString[:splitIndex]
	tag := imageReferenceString[splitIndex+1:]

	return repo, tag
}

func splitExtraName(imageReferenceStringWOTag string) (string, string) {
	splitIndex := strings.LastIndex(imageReferenceStringWOTag, "/")
	repo := imageReferenceStringWOTag[:splitIndex]
	extra := imageReferenceStringWOTag[splitIndex+1:]

	return repo, extra
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
