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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"golang.org/x/exp/maps"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/releases"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

type ImageLayouts struct {
	Deckhouse       layout.Path
	DeckhouseImages map[string]struct{}

	Install       layout.Path
	InstallImages map[string]struct{}

	InstallStandalone       layout.Path
	InstallStandaloneImages map[string]struct{}

	ReleaseChannel       layout.Path
	ReleaseChannelImages map[string]struct{}

	TrivyDB           layout.Path
	TrivyDBImages     map[string]struct{}
	TrivyBDU          layout.Path
	TrivyBDUImages    map[string]struct{}
	TrivyJavaDB       layout.Path
	TrivyJavaDBImages map[string]struct{}

	Modules map[string]ModuleImageLayout

	TagsResolver *TagsResolver
}

type ModuleImageLayout struct {
	ModuleLayout layout.Path
	ModuleImages map[string]struct{}

	ReleasesLayout layout.Path
	ReleaseImages  map[string]struct{}
}

func CreateOCIImageLayoutsForDeckhouse(
	rootFolder string,
	modules []modules.Module,
) (*ImageLayouts, error) {
	var err error
	layouts := &ImageLayouts{
		TagsResolver: NewTagsResolver(),
		Modules:      map[string]ModuleImageLayout{},
	}

	fsPaths := map[*layout.Path]string{
		&layouts.Deckhouse:         rootFolder,
		&layouts.Install:           filepath.Join(rootFolder, "install"),
		&layouts.InstallStandalone: filepath.Join(rootFolder, "install-standalone"),
		&layouts.ReleaseChannel:    filepath.Join(rootFolder, "release-channel"),
		&layouts.TrivyDB:           filepath.Join(rootFolder, "security", "trivy-db"),
		&layouts.TrivyBDU:          filepath.Join(rootFolder, "security", "trivy-bdu"),
		&layouts.TrivyJavaDB:       filepath.Join(rootFolder, "security", "trivy-java-db"),
	}
	for layoutPtr, fsPath := range fsPaths {
		*layoutPtr, err = CreateEmptyImageLayoutAtPath(fsPath)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", fsPath, err)
		}
	}

	for _, module := range modules {
		path := filepath.Join(rootFolder, "modules", module.Name)
		moduleLayout, err := CreateEmptyImageLayoutAtPath(path)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", path, err)
		}

		path = filepath.Join(rootFolder, "modules", module.Name, "release")
		moduleReleasesLayout, err := CreateEmptyImageLayoutAtPath(path)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", path, err)
		}

		layouts.Modules[module.Name] = ModuleImageLayout{
			ModuleLayout:   moduleLayout,
			ModuleImages:   map[string]struct{}{},
			ReleasesLayout: moduleReleasesLayout,
			ReleaseImages:  map[string]struct{}{},
		}
	}

	return layouts, nil
}

func CreateEmptyImageLayoutAtPath(path string) (layout.Path, error) {
	layoutFilePath := filepath.Join(path, "oci-layout")
	indexFilePath := filepath.Join(path, "index.json")
	blobsPath := filepath.Join(path, "blobs")

	if err := os.MkdirAll(blobsPath, 0o755); err != nil {
		return "", fmt.Errorf("mkdir for blobs: %w", err)
	}

	layoutContents := ociLayout{ImageLayoutVersion: "1.0.0"}
	indexContents := indexSchema{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.index.v1+json",
	}

	rawJSON, err := json.MarshalIndent(indexContents, "", "    ")
	if err != nil {
		return "", fmt.Errorf("create index.json: %w", err)
	}
	if err = os.WriteFile(indexFilePath, rawJSON, 0o644); err != nil {
		return "", fmt.Errorf("create index.json: %w", err)
	}

	rawJSON, err = json.MarshalIndent(layoutContents, "", "    ")
	if err != nil {
		return "", fmt.Errorf("create oci-layout: %w", err)
	}
	if err = os.WriteFile(layoutFilePath, rawJSON, 0o644); err != nil {
		return "", fmt.Errorf("create oci-layout: %w", err)
	}

	return layout.Path(path), nil
}

type indexSchema struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	Manifests     []struct {
		MediaType string `json:"mediaType,omitempty"`
		Size      int    `json:"size,omitempty"`
		Digest    string `json:"digest,omitempty"`
	} `json:"manifests"`
}

type ociLayout struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

func FillLayoutsWithBasicDeckhouseImages(
	mirrorCtx *contexts.PullContext,
	layouts *ImageLayouts,
	deckhouseVersions []semver.Version,
) {
	layouts.DeckhouseImages = map[string]struct{}{}
	layouts.InstallImages = map[string]struct{}{}
	layouts.InstallStandaloneImages = map[string]struct{}{}
	layouts.ReleaseChannelImages = map[string]struct{}{}
	layouts.TrivyDBImages = map[string]struct{}{
		mirrorCtx.DeckhouseRegistryRepo + "/security/trivy-db:2":      {},
		mirrorCtx.DeckhouseRegistryRepo + "/security/trivy-bdu:1":     {},
		mirrorCtx.DeckhouseRegistryRepo + "/security/trivy-java-db:1": {},
	}

	for _, version := range deckhouseVersions {
		layouts.DeckhouseImages[fmt.Sprintf("%s:v%s", mirrorCtx.DeckhouseRegistryRepo, version.String())] = struct{}{}
		layouts.InstallImages[fmt.Sprintf("%s/install:v%s", mirrorCtx.DeckhouseRegistryRepo, version.String())] = struct{}{}
		layouts.InstallStandaloneImages[fmt.Sprintf("%s/install-standalone:v%s", mirrorCtx.DeckhouseRegistryRepo, version.String())] = struct{}{}
		layouts.ReleaseChannelImages[fmt.Sprintf("%s/release-channel:v%s", mirrorCtx.DeckhouseRegistryRepo, version.String())] = struct{}{}
	}

	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if mirrorCtx.SpecificVersion != nil {
		return
	}

	layouts.DeckhouseImages[mirrorCtx.DeckhouseRegistryRepo+":alpha"] = struct{}{}
	layouts.DeckhouseImages[mirrorCtx.DeckhouseRegistryRepo+":beta"] = struct{}{}
	layouts.DeckhouseImages[mirrorCtx.DeckhouseRegistryRepo+":early-access"] = struct{}{}
	layouts.DeckhouseImages[mirrorCtx.DeckhouseRegistryRepo+":stable"] = struct{}{}
	layouts.DeckhouseImages[mirrorCtx.DeckhouseRegistryRepo+":rock-solid"] = struct{}{}

	layouts.InstallImages[mirrorCtx.DeckhouseRegistryRepo+"/install:alpha"] = struct{}{}
	layouts.InstallImages[mirrorCtx.DeckhouseRegistryRepo+"/install:beta"] = struct{}{}
	layouts.InstallImages[mirrorCtx.DeckhouseRegistryRepo+"/install:early-access"] = struct{}{}
	layouts.InstallImages[mirrorCtx.DeckhouseRegistryRepo+"/install:stable"] = struct{}{}
	layouts.InstallImages[mirrorCtx.DeckhouseRegistryRepo+"/install:rock-solid"] = struct{}{}

	layouts.InstallStandaloneImages[mirrorCtx.DeckhouseRegistryRepo+"/install-standalone:alpha"] = struct{}{}
	layouts.InstallStandaloneImages[mirrorCtx.DeckhouseRegistryRepo+"/install-standalone:beta"] = struct{}{}
	layouts.InstallStandaloneImages[mirrorCtx.DeckhouseRegistryRepo+"/install-standalone:early-access"] = struct{}{}
	layouts.InstallStandaloneImages[mirrorCtx.DeckhouseRegistryRepo+"/install-standalone:stable"] = struct{}{}
	layouts.InstallStandaloneImages[mirrorCtx.DeckhouseRegistryRepo+"/install-standalone:rock-solid"] = struct{}{}

	layouts.ReleaseChannelImages[mirrorCtx.DeckhouseRegistryRepo+"/release-channel:alpha"] = struct{}{}
	layouts.ReleaseChannelImages[mirrorCtx.DeckhouseRegistryRepo+"/release-channel:beta"] = struct{}{}
	layouts.ReleaseChannelImages[mirrorCtx.DeckhouseRegistryRepo+"/release-channel:early-access"] = struct{}{}
	layouts.ReleaseChannelImages[mirrorCtx.DeckhouseRegistryRepo+"/release-channel:stable"] = struct{}{}
	layouts.ReleaseChannelImages[mirrorCtx.DeckhouseRegistryRepo+"/release-channel:rock-solid"] = struct{}{}
}

func FindDeckhouseModulesImages(mirrorCtx *contexts.PullContext, layouts *ImageLayouts) error {
	modulesNames := maps.Keys(layouts.Modules)
	for _, moduleName := range modulesNames {
		moduleData := layouts.Modules[moduleName]
		moduleData.ReleaseImages = map[string]struct{}{
			mirrorCtx.DeckhouseRegistryRepo + "/modules/" + moduleName + "/release:alpha":        {},
			mirrorCtx.DeckhouseRegistryRepo + "/modules/" + moduleName + "/release:beta":         {},
			mirrorCtx.DeckhouseRegistryRepo + "/modules/" + moduleName + "/release:early-access": {},
			mirrorCtx.DeckhouseRegistryRepo + "/modules/" + moduleName + "/release:stable":       {},
			mirrorCtx.DeckhouseRegistryRepo + "/modules/" + moduleName + "/release:rock-solid":   {},
		}

		channelVersions, err := releases.FetchVersionsFromModuleReleaseChannels(
			moduleData.ReleaseImages,
			mirrorCtx.RegistryAuth,
			mirrorCtx.Insecure,
			mirrorCtx.SkipTLSVerification,
		)
		if err != nil {
			return fmt.Errorf("fetch versions from %q release channels: %w", moduleName, err)
		}

		for _, moduleVersion := range channelVersions {
			moduleData.ModuleImages[mirrorCtx.DeckhouseRegistryRepo+"/modules/"+moduleName+":"+moduleVersion] = struct{}{}
			moduleData.ReleaseImages[mirrorCtx.DeckhouseRegistryRepo+"/modules/"+moduleName+"/release:"+moduleVersion] = struct{}{}
		}

		nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(&mirrorCtx.BaseContext)
		fetchDigestsFrom := maps.Clone(moduleData.ModuleImages)
		for imageTag := range fetchDigestsFrom {
			ref, err := name.ParseReference(imageTag, nameOpts...)
			if err != nil {
				return fmt.Errorf("get digests for %q version: %w", imageTag, err)
			}

			img, err := remote.Image(ref, remoteOpts...)
			if err != nil {
				return fmt.Errorf("get digests for %q version: %w", imageTag, err)
			}

			imagesDigestsJSON, err := images.ExtractFileFromImage(img, "images_digests.json")
			switch {
			case errors.Is(err, fs.ErrNotExist):
				continue
			case err != nil:
				return fmt.Errorf("extract digests for %q version: %w", imageTag, err)
			}

			digests := images.ExtractDigestsFromJSONFile(imagesDigestsJSON.Bytes())
			for _, digest := range digests {
				moduleData.ModuleImages[mirrorCtx.DeckhouseRegistryRepo+"/modules/"+moduleName+"@"+digest] = struct{}{}
			}
		}

		layouts.Modules[moduleName] = moduleData
	}

	return nil
}

func FindImageByTag(l layout.Path, tag string) (v1.Image, error) {
	index, err := l.ImageIndex()
	if err != nil {
		return nil, err
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return nil, err
	}

	for _, imageManifest := range indexManifest.Manifests {
		for key, value := range imageManifest.Annotations {
			if key == "org.opencontainers.image.ref.name" && strings.HasSuffix(value, ":"+tag) {
				return index.Image(imageManifest.Digest)
			}
		}
	}

	return nil, nil
}
