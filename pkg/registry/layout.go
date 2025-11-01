package registry

import (
	"fmt"
	"strings"

	"github.com/deckhouse/deckhouse-cli/pkg"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

type ImageMeta struct {
	TagReference    string
	DigestReference string
}

func NewImageMeta(tagReference string, digest *v1.Hash) *ImageMeta {
	imageRepo, _ := splitImageRefByRepoAndTag(tagReference)

	return &ImageMeta{
		TagReference:    tagReference,
		DigestReference: imageRepo + "@" + digest.String(),
	}
}

type ImageLayout struct {
	wrapped layout.Path
	mapping map[string]*ImageMeta
}

func NewImageLayout(path layout.Path) *ImageLayout {
	return &ImageLayout{wrapped: path}
}

func (l *ImageLayout) Path() layout.Path {
	return l.wrapped
}

func (l *ImageLayout) GetImage(imageReference string) (pkg.RegistryImage, error) {
	index, err := l.wrapped.ImageIndex()
	if err != nil {
		return nil, fmt.Errorf("images index: %w", err)
	}

	indexManifest, err := index.IndexManifest()
	if err != nil {
		return nil, fmt.Errorf("index manifest: %w", err)
	}

	installerHash := findDigestByImageReference(imageReference, indexManifest)
	if installerHash == nil {
		return nil, fmt.Errorf("no image tagged as %q found in index", imageReference)
	}

	img, err := index.Image(*installerHash)
	if err != nil {
		return nil, fmt.Errorf("cannot read image from index: %w", err)
	}

	return NewImage(img, WithTagReference(imageReference)), nil
}

func findDigestByImageReference(imageReference string, indexManifest *v1.IndexManifest) *v1.Hash {
	for _, imageManifest := range indexManifest.Manifests {
		imageRef, found := imageManifest.Annotations["org.opencontainers.image.ref.name"]
		if found && imageRef == imageReference {
			tag := imageManifest.Digest

			return &tag
		}
	}

	return nil
}

func splitImageRefByRepoAndTag(imageReferenceString string) (repo, tag string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo = imageReferenceString[:splitIndex]
	tag = imageReferenceString[splitIndex+1:]

	if strings.HasSuffix(repo, "@sha256") {
		repo = strings.TrimSuffix(repo, "@sha256")
		tag = "@sha256:" + tag
	}

	return repo, tag
}
