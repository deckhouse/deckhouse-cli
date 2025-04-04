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

package manifests

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Masterminds/semver/v3"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/images"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
)

func GenerateDeckhouseReleaseManifestsForVersions(
	versionTagsToMirror []string,
	pathToManifestYAML string,
	releaseChannelsImagesLayout layout.Path,
) error {
	manifests := &bytes.Buffer{}
	for _, version := range versionTagsToMirror {
		versionReleaseImage, err := layouts.FindImageByTag(releaseChannelsImagesLayout, version)
		releaseData, err := extractReleaseInfoForDeckhouseRelease(versionReleaseImage)
		if err != nil {
			return fmt.Errorf("Build manifest for version %q: %w", version, err)
		}

		releaseManifest, err := generateDeckhouseRelease(version, releaseData)
		if err != nil {
			return fmt.Errorf("Build manifest for version %q: %w", version, err)
		}

		manifests.Write(releaseManifest)
	}

	if err := os.MkdirAll(filepath.Dir(pathToManifestYAML), 0o775); err != nil {
		return fmt.Errorf("Create DeckhouseReleases manifest file: %w", err)
	}
	manifestFile, err := os.Create(pathToManifestYAML)
	if err != nil {
		return fmt.Errorf("Create DeckhouseReleases manifest file: %w", err)
	}

	if _, err = io.Copy(manifestFile, manifests); err != nil {
		return fmt.Errorf("Write DeckhouseReleases manifest file: %w", err)
	}

	if err = manifestFile.Sync(); err != nil {
		return fmt.Errorf("Write DeckhouseReleases manifest file: %w", err)
	}
	if err = manifestFile.Close(); err != nil {
		return fmt.Errorf("Write DeckhouseReleases manifest file: %w", err)
	}

	return nil
}

func generateDeckhouseRelease(versionTag string, releaseInfo *releaseInfo) ([]byte, error) {
	const githubReleaseChangelogLinkBase = "https://github.com/deckhouse/deckhouse/releases/tag"
	version, err := semver.NewVersion(versionTag)
	if err != nil {
		return nil, fmt.Errorf("Parse version tag %q: %w", versionTag, err)
	}

	var disruptions []string
	if len(releaseInfo.Disruptions) > 0 {
		disruptionsVersion := fmt.Sprintf("%d.%d", version.Major(), version.Minor())
		disruptions = releaseInfo.Disruptions[disruptionsVersion]
	}

	manifest, err := yaml.Marshal(&v1alpha1.DeckhouseRelease{
		TypeMeta: metav1.TypeMeta{
			Kind:       "DeckhouseRelease",
			APIVersion: "deckhouse.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: versionTag,
		},
		Spec: v1alpha1.DeckhouseReleaseSpec{
			Version:       versionTag,
			Requirements:  releaseInfo.Requirements,
			Disruptions:   disruptions,
			Changelog:     releaseInfo.Changelog,
			ChangelogLink: fmt.Sprintf("%s/%s", githubReleaseChangelogLinkBase, versionTag),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("Marshal DeckhouseRelease: %w", err)
	}

	return append([]byte("---\n"), manifest...), nil
}

type releaseInfo struct {
	Changelog    map[string]any      `yaml:"-"`
	Disruptions  map[string][]string `yaml:"disruptions"`
	Requirements map[string]string   `yaml:"requirements"`
}

func extractReleaseInfoForDeckhouseRelease(versionReleaseImage v1.Image) (*releaseInfo, error) {
	rawChangelog, err := images.ExtractFileFromImage(versionReleaseImage, "changelog.yaml")
	if err != nil {
		return nil, fmt.Errorf("Extract changelog from release image: %w", err)
	}
	rawReleaseData, err := images.ExtractFileFromImage(versionReleaseImage, "version.json")
	if err != nil {
		return nil, fmt.Errorf("Extract release data from release image: %w", err)
	}

	release := &releaseInfo{
		Changelog: make(map[string]any),
	}
	if err = yaml.Unmarshal(rawReleaseData.Bytes(), release); err != nil {
		return nil, fmt.Errorf("Extract release data from release image: %w", err)
	}
	if err = yaml.Unmarshal(rawChangelog.Bytes(), &release.Changelog); err != nil {
		return nil, fmt.Errorf("Extract release data from release image: %w", err)
	}

	return release, nil
}
