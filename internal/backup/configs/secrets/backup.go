package secrets

import (
	"context"
	"log"
	"strings"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func BackupSecrets(
	_ *rest.Config,
	kubeCl kubernetes.Interface,
	_ dynamic.Interface,
	namespaces []string,
) ([]runtime.Object, error) {
	namespaces = lo.Filter(namespaces, func(item string, _ int) bool {
		return strings.HasPrefix(item, "d8-") || strings.HasPrefix(item, "kube-")
	})

	secrets := lo.Map(namespaces, func(namespace string, _ int) []runtime.Object {
		list, err := kubeCl.CoreV1().Secrets(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Fatalf("Failed to list secrets from : %v", err)
		}

		return lo.Map(list.Items, func(secret corev1.Secret, _ int) runtime.Object {
			// Some shit-for-brains kubernetes/client-go developer decided that it is fun to remove GVK from responses for no reason.
			// Have to add it back so that meta.Accessor can do its job
			// https://github.com/kubernetes/client-go/issues/1328
			secret.TypeMeta = metav1.TypeMeta{
				Kind:       "Secret",
				APIVersion: corev1.SchemeGroupVersion.String(),
			}
			return &secret
		})
	})

	return lo.Flatten(secrets), nil
}
