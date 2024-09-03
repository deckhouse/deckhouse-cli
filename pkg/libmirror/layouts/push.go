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

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

func PushLayoutToRepo(
	imagesLayout layout.Path,
	registryRepo string,
	authProvider authn.Authenticator,
	logger contexts.Logger,
	insecure, skipVerifyTLS bool,
) error {
	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)

	index, err := imagesLayout.ImageIndex()
	if err != nil {
		return fmt.Errorf("Read OCI Image Index: %w", err)
	}
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("Parse OCI Image Index Manifest: %w", err)
	}

	pushCount := 1
	for _, imageDesc := range indexManifest.Manifests {
		tag := imageDesc.Annotations["io.deckhouse.image.short_tag"]
		imageRef := registryRepo + ":" + tag

		logger.InfoF("[%d / %d] Pushing image %s ", pushCount, len(indexManifest.Manifests), imageRef)
		img, err := index.Image(imageDesc.Digest)
		if err != nil {
			return fmt.Errorf("Read image: %w", err)
		}

		ref, err := name.ParseReference(imageRef, refOpts...)
		if err != nil {
			return fmt.Errorf("Parse image reference: %w", err)
		}
		if err = remote.Write(ref, img, remoteOpts...); err != nil {
			return fmt.Errorf("Write %s to registry: %w", ref.String(), err)
		}
		logger.InfoLn("✅")
		pushCount += 1
	}

	return nil
}
