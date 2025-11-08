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

package modules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/releases"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/errorutil"
)

type Module struct {
	Name         string
	RegistryPath string
	Releases     []string
}

func (m *Module) Versions() []*semver.Version {
	versions := make([]*semver.Version, 0)
	for _, release := range m.Releases {
		v, err := semver.NewVersion(release)
		if err == nil {
			versions = append(versions, v)
		}
	}
	return versions
}

func ForRepo(repo string, registryAuth authn.Authenticator, insecure, skipVerifyTLS bool, client pkg.RegistryClient) ([]Module, error) {
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(registryAuth, insecure, skipVerifyTLS)
	result, err := getModulesForRepo(repo, nameOpts, remoteOpts, client)
	if err != nil {
		return nil, fmt.Errorf("Get external modules: %w", err)
	}

	return result, nil
}

func getModulesForRepo(
	repo string,
	nameOpts []name.Option,
	remoteOpts []remote.Option,
	client pkg.RegistryClient,
) ([]Module, error) {
	modules, err := client.ListTags(context.TODO())
	if err != nil {
		if errorutil.IsRepoNotFoundError(err) {
			return []Module{}, nil
		}
		return nil, fmt.Errorf("Get Deckhouse modules list from %s: %w", repo, err)
	}

	result := make([]Module, 0, len(modules))
	for _, module := range modules {
		m := Module{
			Name:         module,
			RegistryPath: path.Join(repo, module),
			Releases:     []string{},
		}

		repo, err := name.NewRepository(path.Join(m.RegistryPath, "release"), nameOpts...)
		if err != nil {
			return nil, fmt.Errorf("Parsing repo: %v", err)
		}
		m.Releases, err = remote.List(repo, remoteOpts...)
		if err != nil {
			return nil, fmt.Errorf("Get releases for module %q: %w", m.RegistryPath, err)
		}
		result = append(result, m)
	}
	return result, nil
}

func FindExternalModuleImages(
	params *params.PullParams,
	mod *Module,
	filter *Filter,
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
	client pkg.RegistryClient,
) (moduleImages []string, moduleImagesWithExternal, releaseImages map[string]struct{}, err error) {
	logger := params.Logger

	moduleImagesWithExternal, releaseImages = map[string]struct{}{}, map[string]struct{}{}
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)

	// Check if specific versions are requested (explicit tags)
	versionsToMirror := filter.VersionsToMirror(mod)

	// Check if this is the default ">=0.0.0" constraint (no version specified)
	isDefaultConstraint := false
	if constraint, found := filter.GetConstraint(mod.Name); found {
		if sc, ok := constraint.(*SemanticVersionConstraint); ok {
			// Detect the default >=0.0.0 constraint
			constraintStr := sc.constraint.String()
			if constraintStr == ">= 0.0.0" || constraintStr == ">=0.0.0" {
				isDefaultConstraint = true
			}
		}
	}

	if len(versionsToMirror) > 0 && !isDefaultConstraint {
		// Explicit versions specified (e.g., neuvector@=v1.2.3 or neuvector@~1.2.0)
		for _, tag := range versionsToMirror {
			moduleImages = append(moduleImages, mod.RegistryPath+":"+tag)
			moduleImagesWithExternal[mod.RegistryPath+":"+tag] = struct{}{}
			releaseImages[path.Join(mod.RegistryPath, "release")+":"+tag] = struct{}{}
		}
	}

	if filter.ShouldMirrorReleaseChannels(mod.Name) {
		// No explicit versions - use release channels
		channelImgs, err := getAvailableReleaseChannelsImagesForModule(mod, nameOpts, remoteOpts)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("get release channels: %w", err)
		}

		for img := range channelImgs {
			releaseImages[img] = struct{}{}
		}

		channelVers, err := releases.FetchVersionsFromModuleReleaseChannels(channelImgs, authProvider, insecure, skipVerifyTLS, client.WithSegment("release"))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("fetch channel versions: %w", err)
		}

		for _, version := range channelVers {
			moduleImages = append(moduleImages, mod.RegistryPath+":"+version)
			moduleImagesWithExternal[mod.RegistryPath+":"+version] = struct{}{}
			releaseImages[path.Join(mod.RegistryPath, "release")+":"+version] = struct{}{}
		}

		versionsToMirror := make([]string, 0)

		for moduleTag := range moduleImagesWithExternal {
			tag := strings.SplitN(moduleTag, ":", 2)[1]

			semverTag, err := semver.NewVersion(tag)
			if err == nil {
				versionsToMirror = append(versionsToMirror, semverTag.Original())
			}
		}
	}

	// remove duplicate from versionsToMirror
	seen := make(map[string]struct{})
	uniqueTags := make([]string, 0, len(versionsToMirror))
	for _, tag := range versionsToMirror {
		if _, ok := seen[tag]; !ok {
			seen[tag] = struct{}{}
			uniqueTags = append(uniqueTags, tag)
		}
	}

	versionsToMirror = uniqueTags

	logger.Debugf("Finding module extra images for %s", mod.Name)

	for _, tag := range versionsToMirror {
		logger.Debugf("Checking module image %s for extra images", tag)

		img, err := client.GetImage(context.TODO(), tag)
		if err != nil {
			if errorutil.IsImageNotFoundError(err) {
				continue
			}
			return nil, nil, nil, fmt.Errorf("Get digests for %q version: %w", tag, err)
		}

		logger.Debugf("Extracting images_digests.json from %s", tag)

		imagesDigestsJSON, err := images.ExtractFileFromImage(img, "images_digests.json")
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case err != nil:
			return nil, nil, nil, fmt.Errorf("Extract digests for %q version: %w", tag, err)
		}

		logger.Debugf("Parsing images_digests.json from %s", tag)

		digests := images.ExtractDigestsFromJSONFile(imagesDigestsJSON.Bytes())
		for _, digest := range digests {
			extraImageName := mod.RegistryPath + "@" + digest
			moduleImagesWithExternal[extraImageName] = struct{}{}
		}
	}

	return moduleImages, moduleImagesWithExternal, releaseImages, nil
}

func getAvailableReleaseChannelsImagesForModule(mod *Module, refOpts []name.Option, remoteOpts []remote.Option) (map[string]struct{}, error) {
	releasesRegistryPath := path.Join(mod.RegistryPath, "release")
	result := make(map[string]struct{})
	for _, imageTag := range []string{
		releasesRegistryPath + ":" + internal.AlphaChannel,
		releasesRegistryPath + ":" + internal.BetaChannel,
		releasesRegistryPath + ":" + internal.EarlyAccessChannel,
		releasesRegistryPath + ":" + internal.StableChannel,
		releasesRegistryPath + ":" + internal.RockSolidChannel,
		releasesRegistryPath + ":" + internal.LTSChannel,
	} {
		imageRef, err := name.ParseReference(imageTag, refOpts...)
		if err != nil {
			return nil, fmt.Errorf("Parse release channel reference: %w", err)
		}

		_, err = remote.Head(imageRef, remoteOpts...)
		if err != nil {
			if errorutil.IsImageNotFoundError(err) {
				continue
			}
			return nil, fmt.Errorf("Check if release channel is present: %w", err)
		}
		result[imageTag] = struct{}{}
	}

	return result, nil
}

// FindModuleExtraImages extracts extra_images.json from module images and returns extra images map
func FindModuleExtraImages(
	params *params.PullParams,
	mod *Module,
	moduleImages []string,
	_ authn.Authenticator,
	_, _ bool,
	client pkg.RegistryClient,
) (extraImages map[string]struct{}, err error) {
	logger := params.Logger

	extraImages = map[string]struct{}{}

	// Try to extract extra_images.json from any available module version
	for _, imageTag := range moduleImages {
		if strings.Contains(imageTag, "@sha256:") {
			logger.Debugf("Skipping digest reference %s for extra_images.json extraction", imageTag)
			continue // Skip digest references
		}

		logger.Debugf("Checking module image %s for extra_images.json", imageTag)

		img, err := client.GetImage(context.TODO(), strings.Split(imageTag, ":")[1])
		if err != nil {
			continue
		}

		logger.Debugf("Extracting extra_images.json from %s", imageTag)

		extraImagesJSON, err := images.ExtractFileFromImage(img, "extra_images.json")
		if errors.Is(err, fs.ErrNotExist) {
			continue // No extra_images.json in this version, try next
		}
		if err != nil {
			return nil, fmt.Errorf("Extract extra_images.json from %q: %w", imageTag, err)
		}

		// Parse extra_images.json - it should contain image_name:tag mappings
		// Support numeric tag values like {"scanner": 3}
		var extraImagesRaw map[string]interface{}
		if err := json.Unmarshal(extraImagesJSON.Bytes(), &extraImagesRaw); err != nil {
			return nil, fmt.Errorf("Parse extra_images.json from %q: %w", imageTag, err)
		}

		// Convert to full registry paths with tags
		for imageName, tagValue := range extraImagesRaw {
			var imageTag string

			switch v := tagValue.(type) {
			case float64:
				imageTag = fmt.Sprintf("%.0f", v)
			case int:
				imageTag = fmt.Sprintf("%d", v)
			default:
				return nil, fmt.Errorf("Invalid tag type for %q in extra_images.json: %T", imageName, tagValue)
			}

			fullImagePath := path.Join(mod.RegistryPath, "extra", imageName) + ":" + imageTag
			extraImages[fullImagePath] = struct{}{}
		}

		// Continue checking other versions to collect all possible extra images
	}

	return extraImages, nil
}
