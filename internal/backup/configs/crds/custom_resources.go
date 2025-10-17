package crds

import (
	"context"
	"fmt"
	"log"

	"github.com/samber/lo"
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

type customResourceDescription struct {
	gvr schema.GroupVersionResource
	crd v1.CustomResourceDefinition
}

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
		lo.Map(crdList.Items, func(crd v1.CustomResourceDefinition, _ int) *customResourceDescription {
			version, validVersionFound := lo.Find(crd.Spec.Versions, func(item v1.CustomResourceDefinitionVersion) bool {
				return item.Storage && item.Served
			})
			if !validVersionFound {
				return nil // Empty GVR's will be filtered out
			}

			return &customResourceDescription{
				gvr: schema.GroupVersionResource{
					Group:    crd.Spec.Group,
					Version:  version.Name,
					Resource: crd.Spec.Names.Plural,
				},
				crd: crd,
			}
		}))

	namespacedResourcesToBackup, clusterwideResourcesToBackup := lo.FilterReject(
		resourcesToBackup,
		func(r *customResourceDescription, _ int) bool { return r.crd.Spec.Scope == v1.NamespaceScoped },
	)

	nsResources := lo.Map(namespacedResourcesToBackup, func(resource *customResourceDescription, _ int) []runtime.Object {
		return lo.Flatten(lo.Map(namespaces, func(namespace string, _ int) []runtime.Object {
			query := dynamic.ResourceInterface(dynamicCl.Resource(resource.gvr))
			query = query.(dynamic.NamespaceableResourceInterface).Namespace(namespace)

			list, err := query.List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				log.Fatalf("Failed to list %s: %v", resource.gvr, err)
			}

			return lo.Map(list.Items, func(object unstructured.Unstructured, _ int) runtime.Object {
				return &object
			})
		}))
	})

	cwResources := lo.Map(clusterwideResourcesToBackup, func(resource *customResourceDescription, _ int) []runtime.Object {
		query := dynamic.ResourceInterface(dynamicCl.Resource(resource.gvr))
		list, err := query.List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Fatalf("Failed to list %s: %v", resource.gvr, err)
		}

		return lo.Map(list.Items, func(object unstructured.Unstructured, _ int) runtime.Object {
			return &object
		})
	})

	var result [][]runtime.Object
	result = append(result, nsResources...)
	result = append(result, cwResources...)
	return lo.Flatten(result), nil
}
