/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package loki

import (
	"context"
	"fmt"
	"k8s.io/client-go/rest"

	//"github.com/deckhouse/deckhouse-cli/internal/platform/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/kubectl/pkg/util/templates"
)

var lokiLong = templates.LongDesc(`
Take a snapshot of ETCD state.
		
This command creates a snapshot of the Kubernetes underlying key-value database ETCD.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	lokiCmd := &cobra.Command{
		Use:           "loki <snapshot-path>",
		Short:         "Take a snapshot of ETCD state",
		Long:          lokiLong,
		ValidArgs:     []string{"snapshot-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		//PreRunE:       flags.ValidateParameters,
		RunE: backupLoki,
	}

	//addFlags(lokiCmd.Flags())
	return lokiCmd
}

//const (
//	etcdPodNamespace      = "kube-system"
//	etcdPodsLabelSelector = "component=etcd"
//
//	bufferSize16MB = 16 * 1024 * 1024
//)
//
//var (
//	requestedEtcdPodName string
//
//	verboseLog bool
//)

func backupLoki(cmd *cobra.Command, _ []string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	client, err := rest.RESTClientFor(config)
	//req := client.Get().RequestURI("")

	//err = createtarball.Tarball(config, kubeCl)
	//if err != nil {
	//	return fmt.Errorf("Error collecting debug info: %w", err)
	//}
	const (
		namespace   = "d8-monitoring" // Change to your service namespace
		serviceName = "loki"          // Change to your service name
		portName    = "http"
		servicePort = "3100" // Change to the service port name
	)
	//loki.d8-monitoring.svc.cluster.local:3100

	apiURL := fmt.Sprintf("/api/v1/namespaces/%s/services/%s:%d/proxy/", namespace, serviceName, servicePort)
	req := client.Get().RequestURI(apiURL)
	resp, err := req.DoRaw(context.TODO())
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}

	fmt.Println("Response from service:\n", resp)

	return err
}
