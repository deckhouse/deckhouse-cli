package configmaps

import (
	"context"
	"log"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func BackupConfigMaps(
	_ *rest.Config,
	kubeCl kubernetes.Interface,
	_ dynamic.Interface,
	namespaces []string,
) ([]runtime.Object, error) {
	var configmaps []runtime.Object

	for _, namespace := range namespaces {
		if !strings.HasPrefix(namespace, "d8-") && !strings.HasPrefix(namespace, "kube-") {
			continue
		}

		list, err := kubeCl.CoreV1().ConfigMaps(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Fatalf("Failed to list configmaps from : %v", err)
		}

		for i := range list.Items {
			item := &list.Items[i]
			// Some shit-for-brains kubernetes/client-go developer decided that it is fun to remove GVK from responses for no reason.
			// Have to add it back so that meta.Accessor can do its job
			// https://github.com/kubernetes/client-go/issues/1328
			item.TypeMeta = metav1.TypeMeta{
				Kind:       "ConfigMap",
				APIVersion: corev1.SchemeGroupVersion.String(),
			}

			configmaps = append(configmaps, item)
		}
	}

	return configmaps, nil
}
