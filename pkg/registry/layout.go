package registry

import (
	"fmt"
	"strings"

	"github.com/deckhouse/deckhouse-cli/pkg"
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

	metaByTag map[string]pkg.ImageMeta
}

func NewImageLayout(path layout.Path) *ImageLayout {
	return &ImageLayout{
		wrapped:   path,
		metaByTag: make(map[string]pkg.ImageMeta),
	}
}

func (l *ImageLayout) Path() layout.Path {
	return l.wrapped
}

func (l *ImageLayout) AddImage(img pkg.RegistryImage) error {
	meta, err := img.GetMetadata()
	if err != nil {
		return fmt.Errorf("get image tag reference: %w", err)
	}

	// TODO: support nesting tags in image
	repoTags := strings.Split(meta.GetTagReference(), ":")
	if len(repoTags) == 2 {
		l.metaByTag[repoTags[1]] = meta
	}

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

	newImage, err := NewImage(img, WithFetchingMetadata(imageMeta.GetTagReference()))
	if err != nil {
		return nil, fmt.Errorf("create new image: %w", err)
	}

	return newImage, nil
}

var ErrImageMetaNotFound = fmt.Errorf("image metadata not found")

func (l *ImageLayout) GetMeta(tag string) (pkg.ImageMeta, error) {
	meta, found := l.metaByTag[tag]
	if !found {
		return nil, fmt.Errorf("no metadata found for tag %q: %w", tag, ErrImageMetaNotFound)
	}

	return meta, nil
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

func extractExtraImageShortTag(imageReferenceString string) string {
	const extraPrefix = "/extra/"

	if extraIndex := strings.LastIndex(imageReferenceString, extraPrefix); extraIndex != -1 {
		// Extra image: return "imageName:tag" part after "/extra/"
		return imageReferenceString[extraIndex+len(extraPrefix):]
	}

	// Regular image: return just the tag
	_, tag := splitImageRefByRepoAndTag(imageReferenceString)

	return tag
}
