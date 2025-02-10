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
	"k8s.io/apimachinery/pkg/api/errors"
	"time"

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

	token := "eyJhbGciOiJSUzI1NiIsImtpZCI6IkFnbVRCVndWRm43dy04Qmg1cENqcXFQMVFhOEhuLXF0dUpFSTdWQXBYYUkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkOC1sb2ctc2hpcHBlciIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJsb2ctc2hpcHBlci10b2tlbiIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJsb2ctc2hpcHBlciIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6Ijk5MDEyMzgyLTc3ODEtNGI3NS04ZDgzLTZiNGRjYjhkOGY1ZCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpkOC1sb2ctc2hpcHBlcjpsb2ctc2hpcHBlciJ9.CdYOo_jrEy2k3pKu9jEnkRiDdC5D8NAmYcQIbLE5X6uN78xqhmrzSzIoKi_5D9ZSbc69mpDLmbhlfH9i4v9wFMiLi4NzrXWBOrQxWToB3W5lXDUPdpfIIgyBh1_EhTYPoYRZI_YYan2ToZ4l-RJ4T7jbgtkQmdJotBEHk38VCmtvILXYzYIkGcv302LhiZY8Ia8G_3fnjxNLnuHSyOcv19c9CwAQ6EPI4CqmvPxIJMQfjxUKEZqN217ek0kFx_W3FTY00arkU1IZxpsG6idLcfenDftvXclaulqcxlr4P9je6ghO2hUij4AzQOe7PFJyadH7ZVGAqdHY8n8ofY8X5w"
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
	apiLokiUrl := "ready"
	//apiLokiUrl := ""

	// Create a context with timeout (avoid hanging requests)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	request := kubeCl.CoreV1().RESTClient().
		Get().
		SetHeader("Authorization", "Bearer "+token).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/json").
		SetHeader("User-Agent", "kubernetes-client-go").
		SetHeader("Connection", "keep-alive").
		SetHeader("User-Agent", "kube-rbac-proxy-debug").
		SetHeader("X-Debug", "true"). // Custom debug header
		Namespace(namespace).
		Resource("services").
		Name(portScheme + serviceName + servicePort).
		SubResource("proxy").
		Suffix(apiLokiUrl).
		Do(ctx)

	//rawData, err := request.DoRaw(context.Background())
	//if err != nil {
	//	return fmt.Errorf("Failed to query Loki API: %v", err)
	//}

	// Handle response and print detailed errors
	rawResponse, err := request.Raw()
	if err != nil {
		if errors.IsUnauthorized(err) {
			fmt.Println("❌ Authentication error: Check RBAC permissions.")
		} else if errors.IsForbidden(err) {
			fmt.Println("❌ Forbidden: ServiceAccount lacks required permissions.")
		} else if errors.IsNotFound(err) {
			fmt.Println("❌ Service or API path not found.")
		} else {
			fmt.Printf("❌ Unexpected error: %v\n", err)
		}
	}

	fmt.Println("Loki API Response:", string(rawResponse))

	return err
}
