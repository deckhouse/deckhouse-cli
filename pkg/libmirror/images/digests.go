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
	"context"
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

	"github.com/deckhouse/deckhouse/pkg/registry"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

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
	prevDigests map[string]struct{},
	client registry.Client,
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

	logger.Infof("Deckhouse digests found: %d", len(images))

	logger.Infof("Searching for VEX images")

	vex := make([]string, 0)
	const scanPrintInterval = 20
	counter := 0
	for image := range images {
		counter++
		if counter%scanPrintInterval == 0 {
			logger.Infof("[%d / %d] Scanning images for VEX", counter, len(images))
		}

		if _, ok := prevDigests[image]; ok {
			continue
		}

		vexImageName := strings.Replace(strings.Replace(image, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"
		if _, ok := prevDigests[vexImageName]; ok {
			continue
		}

		vexImageName, err := FindVexImage(mirrorCtx, mirrorCtx.DeckhouseRegistryRepo, nameOpts, remoteOpts, image, client)
		if err != nil {
			return nil, fmt.Errorf("find VEX image for digest %q: %w", image, err)
		}

		if vexImageName != "" {
			logger.Debugf("Vex image found %s", vexImageName)
			vex = append(vex, vexImageName)
		}

		prevDigests[image] = struct{}{}
		prevDigests[vexImageName] = struct{}{}
	}

	logger.Infof("[%d / %d] Scanning images for VEX", counter, len(images))

	logger.Infof("Deckhouse digests found: %d", len(images))
	logger.Infof("VEX images found: %d", len(vex))

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
	_ string,
	nameOpts []name.Option,
	_ []remote.Option,
	digest string,
	client registry.Client,
) (string, error) {
	logger := params.Logger

	// vex image reference check
	vexImageName := strings.Replace(strings.Replace(digest, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"

	logger.Debugf("Checking vex image from %s", vexImageName)

	_, err := name.ParseReference(vexImageName, nameOpts...)
	if err != nil {
		return "", fmt.Errorf("parse reference: %w", err)
	}

	// Use LastIndex to correctly handle URLs with port (e.g., localhost:443/repo:tag)
	splitIndex := strings.LastIndex(vexImageName, ":")
	if splitIndex == -1 {
		return "", fmt.Errorf("invalid vex image name format: %s", vexImageName)
	}
	imagePath := vexImageName[:splitIndex]
	tag := vexImageName[splitIndex+1:]

	// Add missing path segments to client if VEX image is in a subpath
	imageSegmentsRaw := strings.TrimPrefix(imagePath, client.GetRegistry())
	imageSegmentsRaw = strings.TrimPrefix(imageSegmentsRaw, "/")
	if imageSegmentsRaw != "" {
		for _, segment := range strings.Split(imageSegmentsRaw, "/") {
			client = client.WithSegment(segment)
			logger.Debugf("Segment: %s", segment)
		}
	}

	err = client.CheckImageExists(context.TODO(), tag)
	if errors.Is(err, regclient.ErrImageNotFound) {
		// Image not found, which is expected for non-vulnerable images
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("check VEX image exists: %w", err)
	}

	return vexImageName, nil
}
