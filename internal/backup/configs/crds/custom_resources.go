package crds

import (
	"context"
	"fmt"
	"log"
	"slices"

	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiext "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	var namespacedResourcesToBackup, clusterwideResourcesToBackup []*customResourceDescription

	for _, crd := range crdList.Items {
		versionIdx := slices.IndexFunc(crd.Spec.Versions, func(item v1.CustomResourceDefinitionVersion) bool {
			return item.Storage && item.Served
		})
		if versionIdx < 0 {
			continue // No served storage version, nothing to back up
		}

		resource := &customResourceDescription{
			gvr: schema.GroupVersionResource{
				Group:    crd.Spec.Group,
				Version:  crd.Spec.Versions[versionIdx].Name,
				Resource: crd.Spec.Names.Plural,
			},
			crd: crd,
		}

		if crd.Spec.Scope == v1.NamespaceScoped {
			namespacedResourcesToBackup = append(namespacedResourcesToBackup, resource)
		} else {
			clusterwideResourcesToBackup = append(clusterwideResourcesToBackup, resource)
		}
	}

	var objects []runtime.Object

	for _, resource := range namespacedResourcesToBackup {
		for _, namespace := range namespaces {
			query := dynamic.ResourceInterface(dynamicCl.Resource(resource.gvr))
			query = query.(dynamic.NamespaceableResourceInterface).Namespace(namespace)

			list, err := query.List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				log.Fatalf("Failed to list %s: %v", resource.gvr, err)
			}

			for i := range list.Items {
				objects = append(objects, &list.Items[i])
			}
		}
	}

	for _, resource := range clusterwideResourcesToBackup {
		query := dynamic.ResourceInterface(dynamicCl.Resource(resource.gvr))

		list, err := query.List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Fatalf("Failed to list %s: %v", resource.gvr, err)
		}

		for i := range list.Items {
			objects = append(objects, &list.Items[i])
		}
	}

	return objects, nil
}
