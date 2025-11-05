package platform

import (
	"path"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

type ImageMeta struct {
	ImageRepo       string
	ImageTag        string
	Digest          string
	Version         string
	TagReference    string
	DigestReference string
}

func NewImageMeta(version string, tagReference string, digest *v1.Hash) *ImageMeta {
	imageRepo, tag := splitImageRefByRepoAndTag(tagReference)

	return &ImageMeta{
		ImageRepo:       imageRepo,
		ImageTag:        tag,
		Digest:          digest.String(),
		Version:         version,
		TagReference:    tagReference,
		DigestReference: imageRepo + "@" + digest.String(),
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string
	rootUrl    string

	Deckhouse       *registry.ImageLayout
	DeckhouseImages map[string]*ImageMeta

	Install       *registry.ImageLayout
	InstallImages map[string]*ImageMeta

	InstallStandalone       layout.Path
	InstallStandaloneImages map[string]*ImageMeta

	ReleaseChannel       layout.Path
	ReleaseChannelImages map[string]*ImageMeta
}

func NewImageLayouts(rootFolder, rootUrl string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		rootUrl:    rootUrl,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},

		DeckhouseImages:         map[string]*ImageMeta{},
		InstallImages:           map[string]*ImageMeta{},
		InstallStandaloneImages: map[string]*ImageMeta{},
		ReleaseChannelImages:    map[string]*ImageMeta{},
	}

	return l
}

func (l *ImageLayouts) FillDeckhouseImages(deckhouseVersions []string) {
	for _, version := range deckhouseVersions {
		l.DeckhouseImages[l.rootUrl+":"+version] = nil
		l.InstallImages[path.Join(l.rootUrl, internal.InstallSegment)+":"+version] = nil
		l.InstallStandaloneImages[path.Join(l.rootUrl, internal.InstallStandaloneSegment)+":"+version] = nil
	}
}

func (l *ImageLayouts) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		l.DeckhouseImages[l.rootUrl+":"+channel] = nil
		l.InstallImages[path.Join(l.rootUrl, internal.InstallSegment)+":"+channel] = nil
		l.InstallStandaloneImages[path.Join(l.rootUrl, internal.InstallStandaloneSegment)+":"+channel] = nil
		l.ReleaseChannelImages[path.Join(l.rootUrl, internal.ReleaseChannelSegment)+":"+channel] = nil
	}
}

// extractExtraImageShortTag extracts the image name and tag for extra images
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
