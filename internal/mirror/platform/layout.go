package platform

import (
	"fmt"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

type DeckhouseReleaseMeta struct {
	Version string
	Digest  v1.Hash
}

func NewDeckhouseReleaseMeta(version string, digest v1.Hash) *DeckhouseReleaseMeta {
	return &DeckhouseReleaseMeta{
		Version: version,
		Digest:  digest,
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string
	rootUrl    string

	Deckhouse       layout.Path
	DeckhouseImages map[string]struct{}

	Install       layout.Path
	InstallImages map[string]struct{}

	InstallStandalone       layout.Path
	InstallStandaloneImages map[string]struct{}

	ReleaseChannel       layout.Path
	ReleaseChannelImages map[string]*DeckhouseReleaseMeta
}

func NewImageLayouts(rootFolder, rootUrl string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		rootUrl:    rootUrl,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},

		DeckhouseImages:         map[string]struct{}{},
		InstallImages:           map[string]struct{}{},
		InstallStandaloneImages: map[string]struct{}{},
		ReleaseChannelImages:    map[string]*DeckhouseReleaseMeta{},
	}

	return l
}

func (l *ImageLayouts) FillDeckhouseImages(deckhouseVersions []string) {
	for _, version := range deckhouseVersions {
		l.DeckhouseImages[fmt.Sprintf("%s:%s", l.rootUrl, version)] = struct{}{}
		l.InstallImages[fmt.Sprintf("%s/install:%s", l.rootUrl, version)] = struct{}{}
		l.InstallStandaloneImages[fmt.Sprintf("%s/install-standalone:%s", l.rootUrl, version)] = struct{}{}
	}
}

func (l *ImageLayouts) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	l.DeckhouseImages[l.rootUrl+":alpha"] = struct{}{}
	l.DeckhouseImages[l.rootUrl+":beta"] = struct{}{}
	l.DeckhouseImages[l.rootUrl+":early-access"] = struct{}{}
	l.DeckhouseImages[l.rootUrl+":stable"] = struct{}{}
	l.DeckhouseImages[l.rootUrl+":rock-solid"] = struct{}{}

	l.InstallImages[l.rootUrl+"/install:alpha"] = struct{}{}
	l.InstallImages[l.rootUrl+"/install:beta"] = struct{}{}
	l.InstallImages[l.rootUrl+"/install:early-access"] = struct{}{}
	l.InstallImages[l.rootUrl+"/install:stable"] = struct{}{}
	l.InstallImages[l.rootUrl+"/install:rock-solid"] = struct{}{}

	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:alpha"] = struct{}{}
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:beta"] = struct{}{}
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:early-access"] = struct{}{}
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:stable"] = struct{}{}
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:rock-solid"] = struct{}{}
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
