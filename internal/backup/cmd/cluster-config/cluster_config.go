package cluster_config

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"strings"

	"github.com/samber/lo"
	"github.com/samber/lo/parallel"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/backup/k8s"
	"github.com/deckhouse/deckhouse-cli/internal/backup/tarball"
)

// TODO texts
var clusterConfigLong = templates.LongDesc(`
Take a snapshot of cluster configuration.
		
This command creates a snapshot various resources .

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	etcdCmd := &cobra.Command{
		Use:           "cluster-config <backup-tarball-path>",
		Short:         "Take a snapshot of cluster configuration",
		Long:          clusterConfigLong,
		ValidArgs:     []string{"backup-tarball-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		// PreRunE: func(cmd *cobra.Command, args []string) error {
		// 	return validateFlags()
		// },
		RunE: backupConfig,
	}

	addFlags(etcdCmd.Flags())
	return etcdCmd
}

type BackupFunc func(
	kubeCl kubernetes.Interface,
	dynamicCl dynamic.Interface,
	namespaces []string,
) ([]unstructured.Unstructured, error)

func backupConfig(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("This command requires exactly 1 argument")
	}

	// TODO move this to real file when done
	tarFile, err := os.CreateTemp(".", ".*.d8bkp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer func(fileName string) {
		_ = os.Remove(fileName)
	}(tarFile.Name())

	backup := tarball.NewBackup(tarFile)
	kubeCl, dynamicCl, err := setupK8sClients(cmd)
	if err != nil {
		return fmt.Errorf("setup k8s clients: %w", err)
	}

	namespaceList, err := kubeCl.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("Failed to list namespaces: %w", err)
	}
	namespaces := lo.Map(namespaceList.Items, func(ns corev1.Namespace, _ int) string {
		return ns.Name
	})

	// TODO move this to separate packages
	backups := []BackupFunc{
		backupSecrets,
		backupConfigMaps,
	}

	parallel.ForEach(backups, func(bf BackupFunc, _ int) {
		thisFuncName := runtime.FuncForPC(reflect.ValueOf(bf).Pointer()).Name()
		resources, err := bf(kubeCl, dynamicCl, namespaces)
		if err != nil {
			log.Fatalf("%s failed: %v", thisFuncName, err)
		}

		if err = backup.PutResources(resources); err != nil {
			log.Fatalf("%s failed: %v", thisFuncName, err)
		}
	})

	if err = backup.Close(); err != nil {
		return fmt.Errorf("close tarball failed: %w", err)
	}
	if err = tarFile.Sync(); err != nil {
		return fmt.Errorf("tarball flush failed: %w", err)
	}
	if err = tarFile.Close(); err != nil {
		return fmt.Errorf("tarball close failed: %w", err)
	}

	if err = os.Rename(tarFile.Name(), args[0]); err != nil {
		return fmt.Errorf("write tarball failed: %w", err)
	}

	return nil
}

func setupK8sClients(cmd *cobra.Command) (*kubernetes.Clientset, *dynamic.DynamicClient, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	_, kubeCl, err := k8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	dynamicCl := k8s.SetupDynamicClientFromK8sClientset(kubeCl.RESTClient())
	return kubeCl, dynamicCl, nil
}

func backupSecrets(
	_ kubernetes.Interface,
	dynamicCl dynamic.Interface,
	namespaces []string,
) ([]unstructured.Unstructured, error) {
	namespaces = lo.Filter(namespaces, func(item string, _ int) bool {
		return strings.HasPrefix(item, "d8-") || strings.HasPrefix(item, "kube-")
	})

	secrets := parallel.Map(namespaces, func(namespace string, index int) []unstructured.Unstructured {
		gvr := schema.GroupVersionResource{
			Group:    corev1.SchemeGroupVersion.Group,
			Version:  corev1.SchemeGroupVersion.Version,
			Resource: "secrets",
		}

		list, err := dynamicCl.Resource(gvr).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Fatalf("Failed to list secrets from : %v", err)
		}

		return list.Items
	})

	return lo.Flatten(secrets), nil
}

func backupConfigMaps(
	_ kubernetes.Interface,
	dynamicCl dynamic.Interface,
	namespaces []string,
) ([]unstructured.Unstructured, error) {
	namespaces = lo.Filter(namespaces, func(item string, _ int) bool {
		return strings.HasPrefix(item, "d8-") || strings.HasPrefix(item, "kube-")
	})

	configmaps := parallel.Map(namespaces, func(namespace string, _ int) []unstructured.Unstructured {
		gvr := schema.GroupVersionResource{
			Group:    corev1.SchemeGroupVersion.Group,
			Version:  corev1.SchemeGroupVersion.Version,
			Resource: "configmaps",
		}

		list, err := dynamicCl.Resource(gvr).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Fatalf("Failed to list configmaps from : %v", err)
		}

		return list.Items
	})

	return lo.Flatten(configmaps), nil
}
