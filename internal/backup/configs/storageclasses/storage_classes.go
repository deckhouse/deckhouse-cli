package storageclasses

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	v1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func BackupStorageClasses(
	_ *rest.Config,
	kubeCl kubernetes.Interface,
	_ dynamic.Interface,
	_ []string,
) ([]runtime.Object, error) {
	list, err := kubeCl.StorageV1().StorageClasses().List(context.TODO(), metav1.ListOptions{
		LabelSelector: "heritage=deckhouse",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list StorageClasses: %w", err)
	}

	return lo.Map(list.Items, func(item v1.StorageClass, _ int) runtime.Object {
		// Some shit-for-brains kubernetes/client-go developer decided that it is fun to remove GVK from responses for no reason.
		// Have to add it back so that meta.Accessor can do its job
		// https://github.com/kubernetes/client-go/issues/1328
		item.TypeMeta = metav1.TypeMeta{
			Kind:       "StorageClass",
			APIVersion: v1.SchemeGroupVersion.String(),
		}
		return &item
	}), nil
}
