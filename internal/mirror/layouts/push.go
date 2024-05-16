package layouts

import (
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	mirror "github.com/deckhouse/deckhouse-cli/internal/mirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
)

func PushLayoutToRepo(
	imagesLayout layout.Path,
	registryRepo string,
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
) error {
	refOpts, remoteOpts := mirror.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)

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

		log.InfoF("[%d / %d] Pushing image %s...\t", pushCount, len(indexManifest.Manifests), imageRef)
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
		log.InfoLn("✅")
		pushCount += 1
	}

	return nil
}
