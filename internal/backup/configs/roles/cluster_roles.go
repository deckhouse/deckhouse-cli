package roles

import (
	"context"
	"log"

	"github.com/samber/lo"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const notDeckhouseHeritage = "heritage!=deckhouse"

func BackupClusterRoles(
	_ *rest.Config,
	kubeCl kubernetes.Interface,
	_ dynamic.Interface,
	_ []string,
) ([]runtime.Object, error) {
	list, err := kubeCl.RbacV1().ClusterRoles().List(context.TODO(), metav1.ListOptions{
		LabelSelector: notDeckhouseHeritage,
	})
	if err != nil {
		log.Fatalf("Failed to list ClusterRoles from: %v", err)
	}

	return lo.Map(list.Items, func(item rbacv1.ClusterRole, _ int) runtime.Object {
		// Some shit-for-brains kubernetes/client-go developer decided that it is fun to remove GVK from responses for no reason.
		// Have to add it back so that meta.Accessor can do its job
		// https://github.com/kubernetes/client-go/issues/1328
		item.TypeMeta = metav1.TypeMeta{
			Kind:       "ClusterRole",
			APIVersion: rbacv1.SchemeGroupVersion.String(),
		}
		return &item
	}), nil
}

func BackupClusterRoleBindings(
	_ *rest.Config,
	kubeCl kubernetes.Interface,
	_ dynamic.Interface,
	_ []string,
) ([]runtime.Object, error) {
	list, err := kubeCl.RbacV1().ClusterRoleBindings().List(context.TODO(), metav1.ListOptions{
		LabelSelector: notDeckhouseHeritage,
	})
	if err != nil {
		log.Fatalf("Failed to list ClusterRoleBindings from: %v", err)
	}

	return lo.Map(list.Items, func(item rbacv1.ClusterRoleBinding, _ int) runtime.Object {
		// Some shit-for-brains kubernetes/client-go developer decided that it is fun to remove GVK from responses for no reason.
		// Have to add it back so that meta.Accessor can do its job
		// https://github.com/kubernetes/client-go/issues/1328
		item.TypeMeta = metav1.TypeMeta{
			Kind:       "ClusterRoleBinding",
			APIVersion: rbacv1.SchemeGroupVersion.String(),
		}
		return &item
	}), nil
}
