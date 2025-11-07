package platform

import (
	"fmt"
	"path"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type ImageDownloadRequest struct {
	DeckhouseImages         map[string]*puller.ImageMeta
	InstallImages           map[string]*puller.ImageMeta
	InstallStandaloneImages map[string]*puller.ImageMeta
	ReleaseChannelImages    map[string]*puller.ImageMeta
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string
	rootUrl    string

	Deckhouse       *registry.ImageLayout
	DeckhouseImages map[string]*puller.ImageMeta

	DeckhouseInstall *registry.ImageLayout
	InstallImages    map[string]*puller.ImageMeta

	DeckhouseInstallStandalone *registry.ImageLayout
	InstallStandaloneImages    map[string]*puller.ImageMeta

	DeckhouseReleaseChannel *registry.ImageLayout
	ReleaseChannelImages    map[string]*puller.ImageMeta
}

func NewImageLayouts(rootFolder, rootUrl string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		rootUrl:    rootUrl,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},

		DeckhouseImages:         map[string]*puller.ImageMeta{},
		InstallImages:           map[string]*puller.ImageMeta{},
		InstallStandaloneImages: map[string]*puller.ImageMeta{},
		ReleaseChannelImages:    map[string]*puller.ImageMeta{},
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
		key := path.Join(l.rootUrl, internal.ReleaseChannelSegment) + ":" + channel
		if _, exists := l.ReleaseChannelImages[key]; !exists {
			l.ReleaseChannelImages[key] = nil
		}
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
