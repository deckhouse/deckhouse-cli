package image

import (
	"fmt"
	"strings"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

const (
	AnnotationImageReferenceName = "org.opencontainers.image.ref.name"
	AnnotationImageShortTag      = "io.deckhouse.image.short_tag"
)

type ImageLayout struct {
	wrapped         layout.Path
	defaultPlatform v1.Platform

	metaByTag map[string]*ImageMeta
}

func NewImageLayout(path layout.Path) *ImageLayout {
	return &ImageLayout{
		wrapped:   path,
		metaByTag: make(map[string]*ImageMeta),
	}
}

func (l *ImageLayout) Path() layout.Path {
	return l.wrapped
}

func (l *ImageLayout) AddImage(img pkg.RegistryImage, tag string) error {
	meta, err := img.GetMetadata()
	if err != nil {
		return fmt.Errorf("get image tag reference: %w", err)
	}

	// TODO: support nesting tags in image
	l.metaByTag[tag] = meta.(*ImageMeta)

	err = l.wrapped.AppendImage(img,
		layout.WithPlatform(l.defaultPlatform),
		layout.WithAnnotations(map[string]string{
			AnnotationImageReferenceName: meta.GetTagReference(),
			AnnotationImageShortTag:      extractExtraImageShortTag(meta.GetTagReference()),
		}),
	)
	if err != nil {
		return fmt.Errorf("append image: %w", err)
	}

	return nil
}

func (l *ImageLayout) GetImage(tag string) (pkg.RegistryImage, error) {
	index, err := l.wrapped.ImageIndex()
	if err != nil {
		return nil, fmt.Errorf("images index: %w", err)
	}

	imageMeta, err := l.GetMeta(tag)
	if err != nil {
		return nil, fmt.Errorf("get image metadata for %q: %w", tag, err)
	}

	img, err := index.Image(*imageMeta.GetDigest())
	if err != nil {
		return nil, fmt.Errorf("cannot read image from index: %w", err)
	}

	newImage, err := NewImage(img, WithMetadata(imageMeta))
	if err != nil {
		return nil, fmt.Errorf("create new image: %w", err)
	}

	return newImage, nil
}

func (l *ImageLayout) TagImage(imageDigest v1.Hash, tag string) error {
	index, err := l.wrapped.ImageIndex()
	if err != nil {
		return err
	}

	indexManifest, err := index.IndexManifest()
	if err != nil {
		return err
	}

	for _, imageDescriptor := range indexManifest.Manifests {
		if imageDescriptor.Digest == imageDigest {
			imageRepo, _, found := strings.Cut(imageDescriptor.Annotations[AnnotationImageReferenceName], ":")
			// If there is no ":" symbol in the image reference, then it must be a reference by digest and those are fine as is
			if found {
				imageDescriptor.Annotations[AnnotationImageReferenceName] = imageRepo + ":" + tag
			}

			imageDescriptor.Annotations[AnnotationImageShortTag] = tag

			if err = l.wrapped.AppendDescriptor(imageDescriptor); err != nil {
				return fmt.Errorf("append descriptor %s: %w", tag, err)
			}

			return nil
		}
	}

	return client.ErrImageNotFound
}

var ErrImageMetaNotFound = fmt.Errorf("image metadata not found")

func (l *ImageLayout) GetMeta(tag string) (*ImageMeta, error) {
	// Extract tag part from full reference or use as is
	meta, found := l.metaByTag[tag]
	if !found {
		return nil, fmt.Errorf("no metadata found for tag %q: %w", tag, ErrImageMetaNotFound)
	}

	return meta, nil
}

func SplitImageRefByRepoAndTag(imageReferenceString string) (repo, tag string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo = imageReferenceString[:splitIndex]
	tag = imageReferenceString[splitIndex+1:]

	if strings.HasSuffix(repo, "@sha256") {
		repo = strings.TrimSuffix(repo, "@sha256")
		tag = "@sha256:" + tag
	}

	return repo, tag
}

func extractExtraImageShortTag(imageReferenceString string) string {
	const extraPrefix = "/extra/"

	if extraIndex := strings.LastIndex(imageReferenceString, extraPrefix); extraIndex != -1 {
		// Extra image: return "imageName:tag" part after "/extra/"
		return imageReferenceString[extraIndex+len(extraPrefix):]
	}

	// Regular image: return just the tag
	_, tag := SplitImageRefByRepoAndTag(imageReferenceString)

	return tag
}
