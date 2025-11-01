package platform

import (
	"fmt"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

type ImageMeta struct {
	Version         string
	TagReference    string
	DigestReference string
}

func NewImageMeta(version string, tagReference string, digest *v1.Hash) *ImageMeta {
	imageRepo, _ := splitImageRefByRepoAndTag(tagReference)

	return &ImageMeta{
		Version:         version,
		TagReference:    tagReference,
		DigestReference: imageRepo + "@" + digest.String(),
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string
	rootUrl    string

	Deckhouse       layout.Path
	DeckhouseImages map[string]*ImageMeta

	Install       layout.Path
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
		l.DeckhouseImages[fmt.Sprintf("%s:%s", l.rootUrl, version)] = nil
		l.InstallImages[fmt.Sprintf("%s/install:%s", l.rootUrl, version)] = nil
		l.InstallStandaloneImages[fmt.Sprintf("%s/install-standalone:%s", l.rootUrl, version)] = nil
	}
}

func (l *ImageLayouts) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	l.DeckhouseImages[l.rootUrl+":alpha"] = nil
	l.DeckhouseImages[l.rootUrl+":beta"] = nil
	l.DeckhouseImages[l.rootUrl+":early-access"] = nil
	l.DeckhouseImages[l.rootUrl+":stable"] = nil
	l.DeckhouseImages[l.rootUrl+":rock-solid"] = nil

	l.InstallImages[l.rootUrl+"/install:alpha"] = nil
	l.InstallImages[l.rootUrl+"/install:beta"] = nil
	l.InstallImages[l.rootUrl+"/install:early-access"] = nil
	l.InstallImages[l.rootUrl+"/install:stable"] = nil
	l.InstallImages[l.rootUrl+"/install:rock-solid"] = nil

	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:alpha"] = nil
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:beta"] = nil
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:early-access"] = nil
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:stable"] = nil
	l.InstallStandaloneImages[l.rootUrl+"/install-standalone:rock-solid"] = nil
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
