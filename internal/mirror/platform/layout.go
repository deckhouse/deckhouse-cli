package platform

import (
	"fmt"
	"path"
	"reflect"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
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
	rootURL    string

	Deckhouse       *regimage.ImageLayout
	DeckhouseImages map[string]*puller.ImageMeta

	DeckhouseInstall *regimage.ImageLayout
	InstallImages    map[string]*puller.ImageMeta

	DeckhouseInstallStandalone *regimage.ImageLayout
	InstallStandaloneImages    map[string]*puller.ImageMeta

	DeckhouseReleaseChannel *regimage.ImageLayout
	ReleaseChannelImages    map[string]*puller.ImageMeta
}

func NewImageLayouts(rootFolder, rootURL string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		rootURL:    rootURL,
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
		l.DeckhouseImages[l.rootURL+":"+version] = nil
		l.InstallImages[path.Join(l.rootURL, internal.InstallSegment)+":"+version] = nil
		l.InstallStandaloneImages[path.Join(l.rootURL, internal.InstallStandaloneSegment)+":"+version] = nil
	}
}

func (l *ImageLayouts) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		l.DeckhouseImages[l.rootURL+":"+channel] = nil
		l.InstallImages[path.Join(l.rootURL, internal.InstallSegment)+":"+channel] = nil
		l.InstallStandaloneImages[path.Join(l.rootURL, internal.InstallStandaloneSegment)+":"+channel] = nil
		key := path.Join(l.rootURL, internal.ReleaseChannelSegment) + ":" + channel
		if _, exists := l.ReleaseChannelImages[key]; !exists {
			l.ReleaseChannelImages[key] = nil
		}
	}
}

func (l *ImageLayouts) setLayoutByMirrorType(mirrorType internal.MirrorType, layout *regimage.ImageLayout) {
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

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	layoutsValue := reflect.ValueOf(l).Elem()
	layoutPathType := reflect.TypeOf(layout.Path(""))

	paths := make([]layout.Path, 0)
	for i := 0; i < layoutsValue.NumField(); i++ {
		if layoutsValue.Field(i).Type() != layoutPathType {
			continue
		}

		if pathValue := layoutsValue.Field(i).String(); pathValue != "" {
			paths = append(paths, layout.Path(pathValue))
		}
	}

	return paths
}
