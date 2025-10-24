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
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"golang.org/x/exp/maps"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

type ModuleImageLayout struct {
	ModuleLayout layout.Path
	ModuleImages map[string]struct{}

	ReleasesLayout layout.Path
	ReleaseImages  map[string]struct{}

	ExtraLayout layout.Path
	ExtraImages map[string]struct{}
}

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
	TrivyChecks       layout.Path
	TrivyChecksImages map[string]struct{}

	Modules map[string]ModuleImageLayout

	TagsResolver *TagsResolver
}

func NewImageLayouts() *ImageLayouts {
	return &ImageLayouts{
		TagsResolver: NewTagsResolver(),
		Modules:      make(map[string]ModuleImageLayout),
	}
}

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	layoutsValue := reflect.ValueOf(l).Elem()
	layoutPathType := reflect.TypeOf(layout.Path(""))

	paths := make([]layout.Path, 0)
	for i := 0; i < layoutsValue.NumField(); i++ {
		if layoutsValue.Field(i).Type() != layoutPathType {
			continue
		}

		if pathValue := layoutsValue.Field(i).String(); pathValue != "" {
			paths = append(paths, layout.Path(pathValue))
		}
	}

	for _, moduleImageLayout := range l.Modules {
		if moduleImageLayout.ModuleLayout != "" {
			paths = append(paths, moduleImageLayout.ModuleLayout)
		}
		if moduleImageLayout.ReleasesLayout != "" {
			paths = append(paths, moduleImageLayout.ReleasesLayout)
		}
		if moduleImageLayout.ExtraLayout != "" {
			paths = append(paths, moduleImageLayout.ExtraLayout)
		}
	}

	return paths
}

func CreateOCIImageLayoutsForDeckhouse(
	rootFolder string,
	modules []modules.Module,
) (*ImageLayouts, error) {
	var err error
	layouts := NewImageLayouts()

	fsPaths := map[*layout.Path]string{
		&layouts.Deckhouse:         rootFolder,
		&layouts.Install:           filepath.Join(rootFolder, "install"),
		&layouts.InstallStandalone: filepath.Join(rootFolder, "install-standalone"),
		&layouts.ReleaseChannel:    filepath.Join(rootFolder, "release-channel"),
	}
	for layoutPtr, fsPath := range fsPaths {
		*layoutPtr, err = CreateEmptyImageLayout(fsPath)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", fsPath, err)
		}
	}

	for _, module := range modules {
		path := filepath.Join(rootFolder, "modules", module.Name)
		moduleLayout, err := CreateEmptyImageLayout(path)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", path, err)
		}

		path = filepath.Join(rootFolder, "modules", module.Name, "release")
		moduleReleasesLayout, err := CreateEmptyImageLayout(path)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", path, err)
		}

		path = filepath.Join(rootFolder, "modules", module.Name, "extra")
		moduleExtraLayout, err := CreateEmptyImageLayout(path)
		if err != nil {
			return nil, fmt.Errorf("create OCI Image Layout at %s: %w", path, err)
		}

		layouts.Modules[module.Name] = ModuleImageLayout{
			ModuleLayout:   moduleLayout,
			ModuleImages:   map[string]struct{}{},
			ReleasesLayout: moduleReleasesLayout,
			ReleaseImages:  map[string]struct{}{},
			ExtraLayout:    moduleExtraLayout,
			ExtraImages:    map[string]struct{}{},
		}
	}

	return layouts, nil
}

func CreateEmptyImageLayout(path string) (layout.Path, error) {
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
	pullParams *params.PullParams,
	layouts *ImageLayouts,
	deckhouseVersions []string,
) {
	layouts.DeckhouseImages = map[string]struct{}{}
	layouts.InstallImages = map[string]struct{}{}
	layouts.InstallStandaloneImages = map[string]struct{}{}
	layouts.ReleaseChannelImages = map[string]struct{}{}
	// todo(mvasl) need to check if trivy must be here anymore
	layouts.TrivyDBImages = map[string]struct{}{
		pullParams.DeckhouseRegistryRepo + "/security/trivy-db:2":      {},
		pullParams.DeckhouseRegistryRepo + "/security/trivy-bdu:1":     {},
		pullParams.DeckhouseRegistryRepo + "/security/trivy-java-db:1": {},
		pullParams.DeckhouseRegistryRepo + "/security/trivy-checks:0":  {},
	}

	for _, version := range deckhouseVersions {
		layouts.DeckhouseImages[fmt.Sprintf("%s:%s", pullParams.DeckhouseRegistryRepo, version)] = struct{}{}
		layouts.InstallImages[fmt.Sprintf("%s/install:%s", pullParams.DeckhouseRegistryRepo, version)] = struct{}{}
		layouts.InstallStandaloneImages[fmt.Sprintf("%s/install-standalone:%s", pullParams.DeckhouseRegistryRepo, version)] = struct{}{}
		layouts.ReleaseChannelImages[fmt.Sprintf("%s/release-channel:%s", pullParams.DeckhouseRegistryRepo, version)] = struct{}{}
	}

	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if pullParams.DeckhouseTag != "" {
		return
	}

	layouts.DeckhouseImages[pullParams.DeckhouseRegistryRepo+":alpha"] = struct{}{}
	layouts.DeckhouseImages[pullParams.DeckhouseRegistryRepo+":beta"] = struct{}{}
	layouts.DeckhouseImages[pullParams.DeckhouseRegistryRepo+":early-access"] = struct{}{}
	layouts.DeckhouseImages[pullParams.DeckhouseRegistryRepo+":stable"] = struct{}{}
	layouts.DeckhouseImages[pullParams.DeckhouseRegistryRepo+":rock-solid"] = struct{}{}

	layouts.InstallImages[pullParams.DeckhouseRegistryRepo+"/install:alpha"] = struct{}{}
	layouts.InstallImages[pullParams.DeckhouseRegistryRepo+"/install:beta"] = struct{}{}
	layouts.InstallImages[pullParams.DeckhouseRegistryRepo+"/install:early-access"] = struct{}{}
	layouts.InstallImages[pullParams.DeckhouseRegistryRepo+"/install:stable"] = struct{}{}
	layouts.InstallImages[pullParams.DeckhouseRegistryRepo+"/install:rock-solid"] = struct{}{}

	layouts.InstallStandaloneImages[pullParams.DeckhouseRegistryRepo+"/install-standalone:alpha"] = struct{}{}
	layouts.InstallStandaloneImages[pullParams.DeckhouseRegistryRepo+"/install-standalone:beta"] = struct{}{}
	layouts.InstallStandaloneImages[pullParams.DeckhouseRegistryRepo+"/install-standalone:early-access"] = struct{}{}
	layouts.InstallStandaloneImages[pullParams.DeckhouseRegistryRepo+"/install-standalone:stable"] = struct{}{}
	layouts.InstallStandaloneImages[pullParams.DeckhouseRegistryRepo+"/install-standalone:rock-solid"] = struct{}{}

	layouts.ReleaseChannelImages[pullParams.DeckhouseRegistryRepo+"/release-channel:alpha"] = struct{}{}
	layouts.ReleaseChannelImages[pullParams.DeckhouseRegistryRepo+"/release-channel:beta"] = struct{}{}
	layouts.ReleaseChannelImages[pullParams.DeckhouseRegistryRepo+"/release-channel:early-access"] = struct{}{}
	layouts.ReleaseChannelImages[pullParams.DeckhouseRegistryRepo+"/release-channel:stable"] = struct{}{}
	layouts.ReleaseChannelImages[pullParams.DeckhouseRegistryRepo+"/release-channel:rock-solid"] = struct{}{}
}

func FindDeckhouseModulesImages(
	params *params.PullParams,
	layouts *ImageLayouts,
	modulesData []modules.Module,
	filter *modules.Filter,
) error {
	logger := params.Logger

	counter := 0
	for _, module := range modulesData {
		if !filter.Match(&module) {
			continue
		}

		counter++

		moduleImageLayouts := layouts.Modules[module.Name]
		moduleImageLayouts.ReleaseImages = map[string]struct{}{}
		if filter.ShouldMirrorReleaseChannels(module.Name) {
			moduleImageLayouts.ReleaseImages = map[string]struct{}{
				path.Join(params.DeckhouseRegistryRepo, params.ModulesPathSuffix, module.Name, "release") + ":alpha":        {},
				path.Join(params.DeckhouseRegistryRepo, params.ModulesPathSuffix, module.Name, "release") + ":beta":         {},
				path.Join(params.DeckhouseRegistryRepo, params.ModulesPathSuffix, module.Name, "release") + ":early-access": {},
				path.Join(params.DeckhouseRegistryRepo, params.ModulesPathSuffix, module.Name, "release") + ":stable":       {},
				path.Join(params.DeckhouseRegistryRepo, params.ModulesPathSuffix, module.Name, "release") + ":rock-solid":   {},
			}
		}

		logger.InfoF("%d:\t%s - find external module images", counter, module.Name)

		moduleImages, moduleImagesWithExternal, releaseImages, err := modules.FindExternalModuleImages(
			params,
			&module,
			filter,
			params.RegistryAuth,
			params.Insecure,
			params.SkipTLSVerification,
		)
		if err != nil {
			return fmt.Errorf("Find images of %s: %w", module.Name, err)
		}

		moduleImageLayouts.ModuleImages = moduleImagesWithExternal
		maps.Copy(moduleImageLayouts.ReleaseImages, releaseImages)

		logger.InfoF("%d:\t%s - find module extra images", counter, module.Name)

		// Find extra images if any exist
		extraImages, err := modules.FindModuleExtraImages(
			params,
			&module,
			moduleImages,
			params.RegistryAuth,
			params.Insecure,
			params.SkipTLSVerification,
		)
		if err != nil {
			return fmt.Errorf("Find extra images of %s: %w", module.Name, err)
		}

		nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorParams(&params.BaseParams)

		logger.InfoF("Searching for VEX images")

		for digest := range moduleImagesWithExternal {
			vexImageName, err := FindVexImage(
				params,
				module.RegistryPath,
				nameOpts,
				remoteOpts,
				digest,
			)

			if err != nil {
				return fmt.Errorf("Find VEX image for digest %q: %w", digest, err)
			}

			if vexImageName != "" {
				logger.DebugF("Vex image found %s", vexImageName)
				moduleImagesWithExternal[vexImageName] = struct{}{}
			}
		}

		for digest := range extraImages {
			vexImageName, err := FindVexImage(
				params,
				module.RegistryPath,
				nameOpts,
				remoteOpts,
				digest,
			)

			if err != nil {
				return fmt.Errorf("Find VEX image for digest %q: %w", digest, err)
			}

			if vexImageName != "" {
				logger.DebugF("Vex image found %s", vexImageName)
				extraImages[vexImageName] = struct{}{}
			}
		}

		moduleImageLayouts.ExtraImages = extraImages

		if len(moduleImageLayouts.ModuleImages) == 0 {
			return fmt.Errorf("found no releases for %s", module.Name)
		}

		layouts.Modules[module.Name] = moduleImageLayouts

		logger.InfoF("%d:\t%s", counter, module.Name)
	}

	return nil
}

var ErrImageNotFound = errors.New("image not found")

func FindImageByTag(l layout.Path, tag string) (v1.Image, error) {
	index, err := l.ImageIndex()
	if err != nil {
		return nil, err
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return nil, err
	}

	for _, imageDescriptor := range indexManifest.Manifests {
		for key, value := range imageDescriptor.Annotations {
			if key == "org.opencontainers.image.ref.name" && strings.HasSuffix(value, ":"+tag) {
				return index.Image(imageDescriptor.Digest)
			}
		}
	}

	return nil, ErrImageNotFound
}

func FindImageDescriptorByTag(l layout.Path, tag string) (v1.Descriptor, error) {
	index, err := l.ImageIndex()
	if err != nil {
		return v1.Descriptor{}, err
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return v1.Descriptor{}, err
	}

	for _, imageDescriptor := range indexManifest.Manifests {
		for key, value := range imageDescriptor.Annotations {
			if key == "org.opencontainers.image.ref.name" && strings.HasSuffix(value, ":"+tag) {
				return imageDescriptor, nil
			}
		}
	}

	return v1.Descriptor{}, ErrImageNotFound
}

func TagImage(l layout.Path, imageDigest v1.Hash, tag string) error {
	index, err := l.ImageIndex()
	if err != nil {
		return err
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return err
	}

	for _, imageDescriptor := range indexManifest.Manifests {
		if imageDescriptor.Digest == imageDigest {
			imageRepo, _, found := strings.Cut(imageDescriptor.Annotations["org.opencontainers.image.ref.name"], ":")
			// If there is no ":" symbol in the image reference, then it must be a reference by digest and those are fine as is
			if found {
				imageDescriptor.Annotations["org.opencontainers.image.ref.name"] = imageRepo + ":" + tag
			}
			imageDescriptor.Annotations["io.deckhouse.image.short_tag"] = tag
			if err = l.AppendDescriptor(imageDescriptor); err != nil {
				return fmt.Errorf("append descriptor %s: %w", tag, err)
			}
			return nil
		}
	}

	return ErrImageNotFound
}

func FindVexImage(
	params *params.PullParams,
	registryPath string,
	nameOpts []name.Option,
	remoteOpts []remote.Option,
	digest string,
) (string, error) {
	logger := params.Logger

	// vex image reference check
	vexImageName := strings.Replace(strings.Replace(digest, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"

	logger.DebugF("Checking vex image from %s", vexImageName)

	vexref, err := name.ParseReference(vexImageName, nameOpts...)
	if err != nil {
		return "", fmt.Errorf("parse reference: %w", err)
	}

	var vexErr error
	_, vexErr = remote.Head(vexref, remoteOpts...)
	if vexErr != nil {
		var transportErr *transport.Error
		if errors.As(vexErr, &transportErr) && transportErr.StatusCode == 404 {
			// Image not found, which is expected for non-vulnerable images
			return "", nil
		}

		logger.WarnF("get Head error: %w", vexErr)
	}

	if vexErr != nil {
		_, vexErr = remote.Get(vexref, remoteOpts...)
	}

	if vexErr != nil {
		var transportErr *transport.Error
		if errors.As(vexErr, &transportErr) && transportErr.StatusCode == 404 {
			// Image not found, which is expected for non-vulnerable images
			return "", nil
		}

		logger.WarnF("get Get error: %w", vexErr)
	}

	return vexImageName, nil
}
