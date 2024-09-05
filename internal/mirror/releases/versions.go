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

package releases

import (
	"encoding/json"
	"fmt"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"golang.org/x/exp/maps"

	mirror "github.com/deckhouse/deckhouse-cli/internal/mirror/contexts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/images"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/errorutil"
)

func VersionsToMirror(mirrorCtx *mirror.PullContext) ([]semver.Version, error) {
	//releaseChannelsToCopy := []string{"alpha", "beta", "early-access", "stable", "rock-solid"}
	releaseChannelsToCopy := []string{"main"}
	releaseChannelsVersions := make([]*semver.Version, len(releaseChannelsToCopy))
	for i, channel := range releaseChannelsToCopy {
		v, err := getReleaseChannelVersionFromRegistry(mirrorCtx, channel)
		if err != nil {
			return nil, fmt.Errorf("get %s release version from registry: %w", channel, err)
		}
		releaseChannelsVersions[i] = v
	}

	rockSolidVersion := releaseChannelsVersions[len(releaseChannelsToCopy)-1]
	mirrorFromVersion := *rockSolidVersion
	if mirrorCtx.MinVersion != nil {
		mirrorFromVersion = *mirrorCtx.MinVersion
		if rockSolidVersion.LessThan(mirrorCtx.MinVersion) {
			mirrorFromVersion = *rockSolidVersion
		}
	}

	tags, err := getReleasedTagsFromRegistry(mirrorCtx)
	if err != nil {
		return nil, fmt.Errorf("get releases from github: %w", err)
	}

	alphaChannelVersion := releaseChannelsVersions[0]
	for i := range releaseChannelsToCopy {
		if releaseChannelsToCopy[i] == "alpha" {
			alphaChannelVersion = releaseChannelsVersions[i]
			break
		}
	}
	versionsAboveMinimal := parseAndFilterVersionsAboveMinimalAnbBelowAlpha(&mirrorFromVersion, tags, alphaChannelVersion)
	versionsAboveMinimal = filterOnlyLatestPatches(versionsAboveMinimal)

	return deduplicateVersions(append(releaseChannelsVersions, versionsAboveMinimal...)), nil
}

func getReleasedTagsFromRegistry(mirrorCtx *mirror.PullContext) ([]string, error) {
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(&mirrorCtx.BaseContext)
	repo, err := name.NewRepository(mirrorCtx.DeckhouseRegistryRepo+"/install-standalone", nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("parsing repo: %v", err)
	}
	tags, err := remote.List(repo, remoteOpts...)
	if err != nil {
		return nil, fmt.Errorf("get tags from Deckhouse registry: %w", err)
	}

	return tags, nil
}

func parseAndFilterVersionsAboveMinimalAnbBelowAlpha(
	minVersion *semver.Version,
	tags []string,
	alphaChannelVersion *semver.Version,
) []*semver.Version {
	versionsAboveMinimal := make([]*semver.Version, 0)
	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil || minVersion.GreaterThan(version) || version.GreaterThan(alphaChannelVersion) {
			continue
		}
		versionsAboveMinimal = append(versionsAboveMinimal, version)
	}
	return versionsAboveMinimal
}

func filterOnlyLatestPatches(versions []*semver.Version) []*semver.Version {
	type majorMinor [2]uint64
	patches := map[majorMinor]uint64{}
	for _, version := range versions {
		release := majorMinor{version.Major(), version.Minor()}
		if patch := patches[release]; patch <= version.Patch() {
			patches[release] = version.Patch()
		}
	}

	topPatches := make([]*semver.Version, 0, len(patches))
	for majMin, patch := range patches {
		topPatches = append(topPatches, semver.MustParse(fmt.Sprintf("v%d.%d.%d", majMin[0], majMin[1], patch)))
	}
	return topPatches
}

func getReleaseChannelVersionFromRegistry(mirrorCtx *mirror.PullContext, releaseChannel string) (*semver.Version, error) {
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(&mirrorCtx.BaseContext)
	nameOpts = append(nameOpts, name.StrictValidation)

	ref, err := name.ParseReference(mirrorCtx.DeckhouseRegistryRepo+"/install-standalone:"+releaseChannel, nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("parse rock solid release ref: %w", err)
	}

	rockSolidReleaseImage, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		return nil, fmt.Errorf("get %s release channel data: %w", releaseChannel, err)
	}

	versionJSON, err := images.ExtractFileFromImage(rockSolidReleaseImage, "version.json")
	if err != nil {
		return nil, fmt.Errorf("cannot get %s release channel version: %w", releaseChannel, err)
	}

	releaseInfo := &struct {
		Version   string `json:"version"`
		Suspended bool   `json:"suspend"`
	}{}
	if err = json.Unmarshal(versionJSON.Bytes(), releaseInfo); err != nil {
		return nil, fmt.Errorf("cannot find release channel version: %w", err)
	}

	if releaseInfo.Suspended {
		return nil, fmt.Errorf("Cannot mirror Deckhouse: source registry contains suspended release channel %q, try again later", releaseChannel)
	}

	ver, err := semver.NewVersion(releaseInfo.Version)
	if err != nil {
		return nil, fmt.Errorf("cannot find release channel version: %w", err)
	}
	return ver, nil
}

func deduplicateVersions(versions []*semver.Version) []semver.Version {
	m := map[semver.Version]struct{}{}
	for _, v := range versions {
		m[*v] = struct{}{}
	}

	return maps.Keys(m)
}

func FetchVersionsFromModuleReleaseChannels(
	releaseChannelImages map[string]struct{},
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
) (map[string]string, error) {
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)
	channelVersions := map[string]string{}
	for imageTag := range releaseChannelImages {

		ref, err := name.ParseReference(imageTag, nameOpts...)
		if err != nil {
			return nil, fmt.Errorf("pull %q release channel: %w", imageTag, err)
		}

		img, err := remote.Image(ref, remoteOpts...)
		if err != nil {
			if errorutil.IsImageNotFoundError(err) {
				continue
			}
			return nil, fmt.Errorf("pull %q release channel: %w", imageTag, err)
		}

		versionJSON, err := images.ExtractFileFromImage(img, "version.json")
		if err != nil {
			return nil, fmt.Errorf("read version.json from %q: %w", imageTag, err)
		}

		tmp := &struct {
			Version string `json:"version"`
		}{}
		if err = json.Unmarshal(versionJSON.Bytes(), tmp); err != nil {
			return nil, fmt.Errorf("parse version.json: %w", err)
		}

		channelVersions[imageTag] = tmp.Version
	}

	return channelVersions, nil
}
