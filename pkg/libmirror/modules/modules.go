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
	"errors"
	"fmt"
	"io/fs"
	"path"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/releases"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
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

func ForRepo(repo string, registryAuth authn.Authenticator, insecure, skipVerifyTLS bool) ([]Module, error) {
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(registryAuth, insecure, skipVerifyTLS)
	result, err := getModulesForRepo(repo, nameOpts, remoteOpts)
	if err != nil {
		return nil, fmt.Errorf("Get external modules: %w", err)
	}

	return result, nil
}

func getModulesForRepo(
	repo string,
	nameOpts []name.Option,
	remoteOpts []remote.Option,
) ([]Module, error) {
	modulesRepo, err := name.NewRepository(repo, nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("Parsing modules repo: %v", err)
	}

	modules, err := remote.List(modulesRepo, remoteOpts...)
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
	mod *Module,
	filter *Filter,
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
) (moduleImages, releaseImages map[string]struct{}, err error) {
	moduleImages, releaseImages = map[string]struct{}{}, map[string]struct{}{}
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)

	if filter.ShouldMirrorReleaseChannels(mod.Name) {
		channelImgs, err := getAvailableReleaseChannelsImagesForModule(mod, nameOpts, remoteOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("get release channels: %w", err)
		}
		for img := range channelImgs {
			releaseImages[img] = struct{}{}
		}

		channelVers, err := releases.FetchVersionsFromModuleReleaseChannels(channelImgs, authProvider, insecure, skipVerifyTLS)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch channel versions: %w", err)
		}
		for _, v := range channelVers {
			moduleImages[mod.RegistryPath+":"+v] = struct{}{}
			releaseImages[path.Join(mod.RegistryPath, "release")+":"+v] = struct{}{}
		}
	}

	for _, tag := range filter.VersionsToMirror(mod) {
		moduleImages[mod.RegistryPath+":"+tag] = struct{}{}
		releaseImages[path.Join(mod.RegistryPath, "release")+":"+tag] = struct{}{}
	}

	for imageTag := range (moduleImages) {
		ref, err := name.ParseReference(imageTag, nameOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("Get digests for %q version: %w", imageTag, err)
		}

		img, err := remote.Image(ref, remoteOpts...)
		if err != nil {
			if errorutil.IsImageNotFoundError(err) {
				continue
			}
			return nil, nil, fmt.Errorf("Get digests for %q version: %w", imageTag, err)
		}

		imagesDigestsJSON, err := images.ExtractFileFromImage(img, "images_digests.json")
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case err != nil:
			return nil, nil, fmt.Errorf("Extract digests for %q version: %w", imageTag, err)
		}

		digests := images.ExtractDigestsFromJSONFile(imagesDigestsJSON.Bytes())
		for _, digest := range digests {
			moduleImages[mod.RegistryPath+"@"+digest] = struct{}{}
		}
	}

	return moduleImages, releaseImages, nil
}

func getAvailableReleaseChannelsImagesForModule(mod *Module, refOpts []name.Option, remoteOpts []remote.Option) (map[string]struct{}, error) {
	releasesRegistryPath := path.Join(mod.RegistryPath, "release")
	result := make(map[string]struct{})
	for _, imageTag := range []string{
		releasesRegistryPath + ":alpha",
		releasesRegistryPath + ":beta",
		releasesRegistryPath + ":early-access",
		releasesRegistryPath + ":stable",
		releasesRegistryPath + ":rock-solid",
		releasesRegistryPath + ":lts",
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
