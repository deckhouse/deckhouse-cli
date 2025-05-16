package cluster_config

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"

	"github.com/samber/lo"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/configmaps"
	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/crds"
	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/roles"
	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/secrets"
	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/storageclasses"
	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/tarball"
	"github.com/deckhouse/deckhouse-cli/internal/backup/configs/whitelist"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var clusterConfigLong = templates.LongDesc(`
Take a snapshot of cluster configuration.
		
This command creates a snapshot various kubernetes resources.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	etcdCmd := &cobra.Command{
		Use:           "cluster-config <backup-tarball-path>",
		Short:         "Take a snapshot of cluster configuration",
		Long:          clusterConfigLong,
		ValidArgs:     []string{"backup-tarball-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          backupConfigs,
	}

	return etcdCmd
}

type BackupStage struct {
	payload BackupFunc
	filter  tarball.BackupResourcesFilter
}

type BackupFunc func(
	restConfig *rest.Config,
	kubeCl kubernetes.Interface,
	dynamicCl dynamic.Interface,
	namespaces []string,
) ([]k8sruntime.Object, error)

func backupConfigs(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("This command requires exactly 1 argument")
	}

	restConfig, kubeCl, dynamicCl, err := setupK8sClients(cmd)
	if err != nil {
		return err
	}
	namespaces, err := getNamespacesFromCluster(kubeCl)
	if err != nil {
		return err
	}

	tarFile, err := os.CreateTemp(".", ".*.d8tmp")
	if err != nil {
		return fmt.Errorf("Failed to create temp file: %v", err)
	}
	defer func() {
		os.Remove(tarFile.Name())
	}()
	backup := tarball.NewBackup(tarFile)

	backupStages := []*BackupStage{
		{payload: secrets.BackupSecrets, filter: &whitelist.BakedInFilter{}},
		{payload: configmaps.BackupConfigMaps, filter: &whitelist.BakedInFilter{}},
		{payload: crds.BackupCustomResources},
		{payload: roles.BackupClusterRoles},
		{payload: roles.BackupClusterRoleBindings},
		{payload: storageclasses.BackupStorageClasses},
	}

	errs := lo.Map(backupStages, func(stage *BackupStage, _ int) error {
		stagePayloadFuncName := runtime.FuncForPC(reflect.ValueOf(stage.payload).Pointer()).Name()

		objects, err := stage.payload(restConfig, kubeCl, dynamicCl, namespaces)
		if err != nil {
			return fmt.Errorf("%s failed: %v", stagePayloadFuncName, err)
		}

		for _, object := range objects {
			if stage.filter != nil && !stage.filter.Matches(object) {
				continue
			}

			if err = backup.PutObject(object); err != nil {
				return fmt.Errorf("%s failed: %v", stagePayloadFuncName, err)
			}
		}

		return nil
	})
	if err = errors.Join(errs...); err != nil {
		log.Printf("WARN: Some backup procedures failed, only successfully backed-up resources will be available:\n%v", err)
	}

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

func getNamespacesFromCluster(kubeCl *kubernetes.Clientset) ([]string, error) {
	namespaceList, err := kubeCl.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("Failed to list namespaces: %w", err)
	}
	namespaces := lo.Map(namespaceList.Items, func(ns corev1.Namespace, _ int) string {
		return ns.Name
	})
	return namespaces, nil
}

func setupK8sClients(cmd *cobra.Command) (*rest.Config, *kubernetes.Clientset, *dynamic.DynamicClient, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	dynamicCl := dynamic.New(kubeCl.RESTClient())
	return restConfig, kubeCl, dynamicCl, nil
}
