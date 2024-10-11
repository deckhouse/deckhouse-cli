package blacklist

import (
	_ "embed"

	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

type Item struct {
	metav1.TypeMeta
	metav1.ObjectMeta // Only Name and Namespace are taken into account
}

//go:embed blacklist.yml
var rawBlacklist []byte
var list []Item

func init() {
	if err := yaml.Unmarshal(rawBlacklist, &list); err != nil {
		panic(err)
	}
}

func List() []Item {
	return list
}

func Matches(obj unstructured.Unstructured) bool {
	_, foundInBlacklist := lo.Find(list, func(item Item) bool {
		return obj.GetName() == item.Name &&
			obj.GetKind() == item.Kind &&
			obj.GetAPIVersion() == item.APIVersion &&
			obj.GetNamespace() == item.Namespace
	})
	return foundInBlacklist
}
