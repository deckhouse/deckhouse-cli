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

	//req := client.Get().RequestURI("")

	//err = createtarball.Tarball(config, kubeCl)
	//if err != nil {
	//	return fmt.Errorf("Error collecting debug info: %w", err)
	//}
	const (
		namespace   = "d8-monitoring" // Change to your service namespace
		serviceName = "loki:"         // Change to your service name
		portScheme  = "https:"
		servicePort = "3100" // Change to the service port name
		//namespace   = "default"      // Change to your service namespace
		//serviceName = "log-service:" // Change to your service name
		//portScheme  = "http:"
		//servicePort = "80" // Change to the service port name

	)
	//loki.d8-monitoring.svc.cluster.local:3100
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	_, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	token := "eyJhbGciOiJSUzI1NiIsImtpZCI6IkFnbVRCVndWRm43dy04Qmg1cENqcXFQMVFhOEhuLXF0dUpFSTdWQXBYYUkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkOC1tb25pdG9yaW5nIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6InByb21ldGhldXMtdG9rZW4iLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoicHJvbWV0aGV1cyIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6ImY0ZmFiMmY0LTNhNDYtNDkyOS1hMGY5LTFhODZkYjBiNDg4NCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpkOC1tb25pdG9yaW5nOnByb21ldGhldXMifQ.PvoNrjPncvdmlqmkUPuBsKxPb8wvz0IYwt2UhMDGlkFateXcQiWFDUs82bnCG6FeRi_dR2UL5ODcnb8HN6WvlKM_vUDKx6jB1pZ93ejBb8_GFri1kvguYdeldoJ6WzX1BPXWLz8iUX6RkkWjzpTLSefy7GIeRyffnhuZfjMPySKGuK3aPwejfgnJ63duqJIpQO8DIa2NolUYupTw3G0_G5p1ad_Fj6kLdtecWXnZ89sEOyugJMlNLaE198paqXl2ijLz11u5Mb8BBQtGTk8cmqmfYroL2aocTV3fZunqW2W4jx24BwFD9276fOb2a4kq7yPHoyexX8PxGz7FpC6SYA"
	//config.BearerTokenFile = ""

	// Create Kubernetes clientset
	//clientsetRbac, err := kubernetes.NewForConfig(config)
	//if err != nil {
	//	return fmt.Errorf("failed to create clientset: %w", err)
	//}

	//apiProxyURL := fmt.Sprintf(
	//	"%s/api/v1/namespaces/%s/services/%s:%s/proxy/",
	//	config.Host, namespace, serviceName, servicePort,
	//)

	//fmt.Println("Response from service:\n", apiProxyURL)
	apiLokiUrl := "loki/api/v1/status/buildinfo"
	//apiLokiUrl := ""

	request := kubeCl.CoreV1().RESTClient().
		Get().
		Namespace(namespace).
		Resource("services").
		Name(portScheme+serviceName+servicePort). // Port is required here
		SubResource("proxy").
		Suffix(apiLokiUrl).
		SetHeader("Authorization", "Bearer "+token).
		Do(context.TODO())

	//request.Header().Set("Authorization", "Bearer "+token)
	//request.ContentType()

	rawData, err := request.Raw()
	if err != nil {
		return fmt.Errorf("Failed to query Loki API: %v", err)
	}

	fmt.Println("Loki API Response:", string(rawData))

	return err
}
