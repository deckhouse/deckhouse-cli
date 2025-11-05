package platform

import (
	"fmt"
	"path"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
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

	DeckhouseInstall *registry.ImageLayout
	InstallImages    map[string]*ImageMeta

	DeckhouseInstallStandalone *registry.ImageLayout
	InstallStandaloneImages    map[string]*ImageMeta

	DeckhouseReleaseChannel *registry.ImageLayout
	ReleaseChannelImages    map[string]*ImageMeta
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

func (l *ImageLayouts) setLayoutByMirrorType(mirrorType internal.MirrorType, layout *registry.ImageLayout) {
	switch mirrorType {
	case internal.MirrorTypeDeckhouse:
		l.Deckhouse = layout
	case internal.MirrorTypeDeckhouseReleaseChannels:
		l.DeckhouseReleaseChannel = layout
	case internal.MirrorTypeDeckhouseInstall:
		l.DeckhouseInstall = layout
	case internal.MirrorTypeDeckhouseInstallStandalone:
		l.DeckhouseInstallStandalone = layout
	default:
		panic(fmt.Sprintf("wrong mirror type in platform image layout: %v", mirrorType))
	}
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
