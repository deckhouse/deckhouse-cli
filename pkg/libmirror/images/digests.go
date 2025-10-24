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

package images

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

var digestRegex = regexp.MustCompile(`sha256:([a-f0-9]{64})`)

func ExtractDigestsFromJSONFile(digestsFile []byte) []string {
	return digestRegex.FindAllString(string(digestsFile), -1)
}

func IsValidImageDigestString(digest string) bool {
	return digestRegex.MatchString(digest)
}

func ExtractImageDigestsFromDeckhouseInstaller(
	mirrorCtx *params.PullParams,
	installerTag string,
	installersLayout layout.Path,
) (map[string]struct{}, error) {
	logger := mirrorCtx.Logger

	index, err := installersLayout.ImageIndex()
	if err != nil {
		return nil, fmt.Errorf("read installer images index: %w", err)
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return nil, fmt.Errorf("read installers index manifest: %w", err)
	}

	installerHash := findDigestForInstallerTag(installerTag, indexManifest)
	if installerHash == nil {
		return nil, fmt.Errorf("no image tagged as %q found in index", installerTag)
	}

	img, err := index.Image(*installerHash)
	if err != nil {
		return nil, fmt.Errorf("cannot read image from index: %w", err)
	}

	tagsCompatMode := false
	imagesJSON, err := ExtractFileFromImage(img, "deckhouse/candi/images_digests.json")
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Older images had lists of deckhouse images tags instead of digests
		tagsCompatMode = true
		imagesJSON, err = ExtractFileFromImage(img, "deckhouse/candi/images_tags.json")
		if err != nil {
			return nil, fmt.Errorf("read tags from %q: %w", installerTag, err)
		}
	case err != nil:
		return nil, fmt.Errorf("read digests from %q: %w", installerTag, err)
	}

	images := map[string]struct{}{}
	if err = parseImagesFromJSON(mirrorCtx.DeckhouseRegistryRepo, imagesJSON, images, tagsCompatMode); err != nil {
		return nil, fmt.Errorf("cannot parse images list from json: %w", err)
	}

	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorParams(&mirrorCtx.BaseParams)

	logger.InfoF("Deckhouse digests found: %d", len(images))

	logger.InfoF("Searching for VEX images")

	vex := make([]string, 0)
	const scanPrintInterval = 20
	counter := 0
	for image := range images {
		vexImageName, err := FindVexImage(mirrorCtx, mirrorCtx.DeckhouseRegistryRepo, nameOpts, remoteOpts, image)
		if err != nil {
			return nil, fmt.Errorf("find VEX image for digest %q: %w", image, err)
		}

		if vexImageName != "" {
			logger.DebugF("Vex image found %s", vexImageName)
			vex = append(vex, vexImageName)
		}

		counter++
		if counter%scanPrintInterval == 0 {
			logger.InfoF("[%d / %d] Scanning images for VEX", counter, len(images))
		}
	}

	logger.InfoF("[%d / %d] Scanning images for VEX", counter, len(images))

	logger.InfoF("Deckhouse digests found: %d", len(images))
	logger.InfoF("VEX images found: %d", len(vex))

	for _, v := range vex {
		images[v] = struct{}{}
	}

	return images, nil
}

func findDigestForInstallerTag(installerTag string, indexManifest *v1.IndexManifest) *v1.Hash {
	for _, imageManifest := range indexManifest.Manifests {
		if imageRef, found := imageManifest.Annotations["org.opencontainers.image.ref.name"]; found && imageRef == installerTag {
			tag := imageManifest.Digest
			return &tag
		}
	}
	return nil
}

func parseImagesFromJSON(registryRepo string, jsonDigests io.Reader, dst map[string]struct{}, tagsCompatMode bool) error {
	digestsByModule := map[string]map[string]string{}
	if err := json.NewDecoder(jsonDigests).Decode(&digestsByModule); err != nil {
		return fmt.Errorf("parse images from json: %w", err)
	}

	for _, nameDigestTuple := range digestsByModule {
		for _, imageID := range nameDigestTuple {
			if tagsCompatMode {
				dst[registryRepo+":"+imageID] = struct{}{}
				continue
			}

			dst[registryRepo+"@"+imageID] = struct{}{}
		}
	}

	return nil
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
