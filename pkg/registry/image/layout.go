package image

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

const (
	AnnotationImageReferenceName = "org.opencontainers.image.ref.name"
	AnnotationImageShortTag      = "io.deckhouse.image.short_tag"
	layoutFileName               = "oci-layout"
	indexFileName                = "index.json"
	blobsDirName                 = "blobs"
	layoutMediaType              = "application/vnd.oci.image.layout.v1+json"
)

type ImageLayout struct {
	wrapped         layout.Path
	defaultPlatform v1.Platform

	metaByTag map[string]*ImageMeta
}

func NewImageLayout(path string) (*ImageLayout, error) {
	l, err := createEmptyImageLayout(path)
	if err != nil {
		return nil, err
	}

	return &ImageLayout{
		wrapped:   l,
		metaByTag: make(map[string]*ImageMeta),
	}, nil
}

func createEmptyImageLayout(path string) (layout.Path, error) {
	layoutFilePath := filepath.Join(path, layoutFileName)
	indexFilePath := filepath.Join(path, indexFileName)
	blobsPath := filepath.Join(path, blobsDirName)

	if err := os.MkdirAll(blobsPath, 0o755); err != nil {
		return "", fmt.Errorf("mkdir for blobs: %w", err)
	}

	layoutContents := ociLayout{ImageLayoutVersion: "1.0.0"}
	indexContents := indexSchema{
		SchemaVersion: 2,
		MediaType:     layoutMediaType,
	}

	rawJSON, err := json.MarshalIndent(indexContents, "", "    ")
	if err != nil {
		return "", fmt.Errorf("marshal index.json content: %w", err)
	}

	if err = os.WriteFile(indexFilePath, rawJSON, 0o644); err != nil {
		return "", fmt.Errorf("create index.json: %w", err)
	}

	rawJSON, err = json.MarshalIndent(layoutContents, "", "    ")
	if err != nil {
		return "", fmt.Errorf("marshal oci-layout content: %w", err)
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

func (l *ImageLayout) Path() layout.Path {
	return l.wrapped
}

// AddImage stores img in the layout under tag.
//
// Idempotent for the (tag, digest) pair: a repeat call with the same tag and
// manifest digest is a no-op. AppendImage always appends a new descriptor to
// index.json, so without this guard repeated pulls of the same image set
// would create duplicate descriptors (and duplicate pushes later).
//
// - same tag, different digest: falls through to AppendImage (re-tag);
//   the push pipeline dedupes such cases with last-wins semantics.
// - metaByTag is recorded only after AppendImage succeeds. Recording earlier
//   would poison the guard on a failed write: a retry would see the pair as
//   done and silently skip the image.
func (l *ImageLayout) AddImage(img pkg.RegistryImage, tag string) error {
	meta, err := img.GetMetadata()
	if err != nil {
		return fmt.Errorf("get image tag reference: %w", err)
	}

	typedMeta, ok := meta.(*ImageMeta)
	if !ok {
		return fmt.Errorf("unexpected image metadata type %T, want *ImageMeta", meta)
	}

	newDigest := meta.GetDigest()
	if existing, ok := l.metaByTag[tag]; ok {
		if existingDigest := existing.GetDigest(); existingDigest != nil &&
			newDigest != nil &&
			*existingDigest == *newDigest {
			return nil
		}
	}

	err = l.wrapped.AppendImage(img,
		layout.WithPlatform(l.defaultPlatform),
		layout.WithAnnotations(map[string]string{
			AnnotationImageReferenceName: meta.GetTagReference(),
			AnnotationImageShortTag:      tag,
		}),
	)
	if err != nil {
		return fmt.Errorf("append image: %w", err)
	}

	// TODO: support nesting tags in image
	l.metaByTag[tag] = typedMeta

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

// CountManifests returns the total image-manifest count across the given layout
// paths. It feeds the pull summary's per-phase image counts, read from the OCI
// layouts before packing deletes them (bundle.Pack) - no extra registry call.
// Unreadable paths (e.g. layouts absent in a dry-run) are skipped.
func CountManifests(paths []layout.Path) int {
	return CountManifestsMatching(paths, func(map[string]string) bool { return true })
}

// CountManifestsMatching counts manifests whose descriptor annotations satisfy
// match. It feeds summary subsets in the same single pass - notably the VEX
// tally (".att" short-tag). Unreadable paths are skipped, as in CountManifests.
func CountManifestsMatching(paths []layout.Path, match func(annotations map[string]string) bool) int {
	total := 0

	for _, lp := range paths {
		index, err := lp.ImageIndex()
		if err != nil {
			continue
		}

		manifest, err := index.IndexManifest()
		if err != nil {
			continue
		}

		for _, desc := range manifest.Manifests {
			if match(desc.Annotations) {
				total++
			}
		}
	}

	return total
}

func SplitImageRefByRepoAndTag(imageReferenceString string) (string, string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo := imageReferenceString[:splitIndex]
	tag := imageReferenceString[splitIndex+1:]

	if strings.HasSuffix(repo, "@sha256") {
		repo = strings.TrimSuffix(repo, "@sha256")
		tag = "@sha256:" + tag
	}

	return repo, tag
}
