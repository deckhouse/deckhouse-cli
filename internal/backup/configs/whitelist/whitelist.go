package whitelist

import (
	_ "embed"
	"log"
	"regexp"
	"strings"
	"sync"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/tarball"
)

// map[Namespace]map[APIVersion]map[Kind] = []Objects names
type whitelistManifest map[string]map[string]map[string][]string

//go:embed whitelist.yml
var rawBuiltInWhitelist []byte

var _ tarball.BackupResourcesFilter = &BakedInFilter{}

type BakedInFilter struct {
	whitelist whitelistManifest
	initOnce  sync.Once
}

func (f *BakedInFilter) Matches(obj runtime.Object) bool {
	metadataAccessor, err := meta.Accessor(obj)
	if err != nil {
		log.Printf("%s does not contain metadata to filter with", obj.GetObjectKind().GroupVersionKind().String())
		return false
	}
	apiVersion, kind := obj.GetObjectKind().GroupVersionKind().ToAPIVersionAndKind()
	name, namespace := metadataAccessor.GetName(), metadataAccessor.GetNamespace()

	f.initOnce.Do(func() {
		if err := yaml.Unmarshal(rawBuiltInWhitelist, &f.whitelist); err != nil {
			panic(err)
		}
	})

	if !lo.HasKey(f.whitelist, namespace) ||
		!lo.HasKey(f.whitelist[namespace], apiVersion) ||
		!lo.HasKey(f.whitelist[namespace][apiVersion], kind) {
		return false
	}

	_, foundInWhitelist := lo.Find(f.whitelist[namespace][apiVersion][kind], func(item string) bool {
		if strings.HasPrefix(item, "$regexp:") {
			_, pattern, _ := strings.Cut(item, ":")
			return matchNameWithRegex(name, pattern)
		}

		return item == name
	})

	return foundInWhitelist
}

func matchNameWithRegex(objectName, pattern string) bool {
	matched, err := regexp.MatchString(pattern, objectName)
	if err != nil {
		log.Panicln("Invalid regexp:", pattern, err)
	}

	return matched
}
