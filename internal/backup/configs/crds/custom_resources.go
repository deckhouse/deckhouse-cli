package crds

import (
	"context"
	"fmt"
	"log"

	"github.com/samber/lo"
	"github.com/samber/lo/parallel"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiext "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const configResourcesLabelSelector = "backup.deckhouse.io/cluster-config=true"

func BackupCustomResources(
	restConfig *rest.Config,
	_ kubernetes.Interface,
	dynamicCl dynamic.Interface,
	namespaces []string,
) ([]runtime.Object, error) {
	apiExtensionClient, err := apiext.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to create api extension clientset: %w", err)
	}

	crdList, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().List(context.TODO(), metav1.ListOptions{
		LabelSelector: configResourcesLabelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to list CustomResourceDefinitions: %w", err)
	}

	resourcesToBackup := lo.Compact(
		lo.Map(crdList.Items, func(crd v1.CustomResourceDefinition, _ int) schema.GroupVersionResource {
			if crd.Spec.Scope != v1.NamespaceScoped {
				return schema.GroupVersionResource{}
			}

			version, validVersionFound := lo.Find(crd.Spec.Versions, func(item v1.CustomResourceDefinitionVersion) bool {
				return item.Storage && item.Served
			})
			if !validVersionFound {
				return schema.GroupVersionResource{} // Empty GVR's will be filtered out
			}

			return schema.GroupVersionResource{
				Group:    crd.Spec.Group,
				Version:  version.Name,
				Resource: crd.Spec.Names.Plural,
			}
		}))

	resources := lo.Map(resourcesToBackup, func(resource schema.GroupVersionResource, _ int) []runtime.Object {
		return lo.Flatten(parallel.Map(namespaces, func(namespace string, _ int) []runtime.Object {
			list, err := dynamicCl.Resource(resource).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				log.Fatalf("Failed to list %s: %v", resource, err)
			}

			return lo.Map(list.Items, func(object unstructured.Unstructured, _ int) runtime.Object {
				return &object
			})
		}))
	})

	return lo.Flatten(resources), nil
}
