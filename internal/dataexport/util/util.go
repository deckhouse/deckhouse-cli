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

package util

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	neturl "net/url"
	"os"
	"slices"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	defaultTTL = "10m"
)

func GetDataExport(deName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataExport, error) {
	ctx := context.Background()

	deObj := &v1alpha1.DataExport{}
	err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
	if err != nil {
		return nil, fmt.Errorf("kube Get dataexport: %s", err.Error())
	}

	// check DataExport is Ready. No status in new version of dataexport
	for _, condition := range deObj.Status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status != "True" {
				return nil, fmt.Errorf("DataExport %s/%s is not Ready", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
			}
			break
		}
	}

	return deObj, nil
}

func GetDataExportWithRestart(deName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataExport, error) {
	ctx := context.Background()

	deObj := &v1alpha1.DataExport{}

	for i := 0; ; i++ {
		var returnErr error = nil

		// get DataExport from k8s by name
		err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
		if err != nil {
			return nil, fmt.Errorf("kube Get dataexport with restart: %s", err.Error())
		}

		for _, condition := range deObj.Status.Conditions {
			// restart DataExport if Expired
			if condition.Type == "Expired" {
				if condition.Status == "True" {
					if err := DeleteDataExport(deName, namespace, rtClient); err != nil {
						return nil, err
					}
					if err := CreateDataExport(
						deName, namespace, "",
						deObj.Spec.TargetRef.Kind,
						deObj.Spec.TargetRef.Name,
						deObj.Spec.Publish, rtClient,
					); err != nil {
						return nil, err
					}
				}
			}
			// check DataExport is Ready
			if condition.Type == "Ready" {
				if condition.Status != "True" {
					returnErr = fmt.Errorf("DataExport %s/%s is not Ready", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
				}
			}
		}
		// check DataExport Url
		if returnErr == nil && deObj.Status.Url == "" {
			returnErr = fmt.Errorf("DataExport %s/%s has no URL", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
		} else if deObj.Spec.Publish && deObj.Status.PublicURL == "" {
			returnErr = fmt.Errorf("DataExport %s/%s has empty PublicURL", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
		}

		if returnErr == nil {
			break
		}
		if i > 40 {
			return nil, returnErr
		}
		time.Sleep(time.Second * 3)
	}

	return deObj, nil
}

func CreateDataExport(deName, namespace, ttl, volumeKind, volumeName string, publish bool, rtClient ctrlrtclient.Client) error {
	ctx := context.Background()
	if ttl == "" {
		ttl = defaultTTL
	}

	// Create dataexport object
	deCfg := &v1alpha1.DataExport{
		TypeMeta: metav1.TypeMeta{
			//APIVersion: "deckhouse.io/v1alpha1",
			Kind: "DataExport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Spec: v1alpha1.DataexportSpec{
			Ttl: ttl,
			TargetRef: v1alpha1.TargetRefSpec{
				Kind: volumeKind,
				Name: volumeName,
			},
			Publish: publish,
		},
	}
	err := rtClient.Create(ctx, deCfg)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("DataExporter create error: %s", err.Error())
	}

	return nil
}

func DeleteDataExport(deName, namespace string, rtClient ctrlrtclient.Client) error {
	ctx := context.Background()

	deObj := &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
	}
	err := rtClient.Delete(ctx, deObj)
	if err != nil {
		return err
	}

	return nil
}

func CreateToken(rtClient ctrlrtclient.Client) (string, error) {
	ctx := context.Background()
	// Authorization token
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "d8-data-exporter", Name: "data-exporter"}}

	token := &authenticationv1.TokenRequest{}
	err := rtClient.SubResource("token").Create(ctx, sa, token)
	if err != nil {
		return "", fmt.Errorf("kube Create token: %s", err.Error())
	}
	fmt.Printf("Token %#v\n\n", token.Status.Token)

	// Authentication access
	crBinding := &rbacv1.ClusterRoleBinding{}
	err = rtClient.Get(ctx, client.ObjectKey{Name: "data-exporter-binding"}, crBinding)
	if err != nil && apierrors.IsNotFound(err) {
		crBinding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data-exporter-binding",
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "cluster-admin", //TODO create specific role
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      "data-exporter",
					Namespace: "d8-data-exporter",
				},
			},
		}
		err = rtClient.Create(ctx, crBinding)
	}
	if err != nil {
		return "", err
	}

	return token.Status.Token, nil
}

func AskYesNoWithTimeout(prompt string, timeout time.Duration) bool {
	inputChan := make(chan string)
	defer close(inputChan)

	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("%s: ", prompt)
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading input, please try again.")
				continue
			}

			input = strings.ToLower(strings.TrimSpace(input))
			if slices.Contains([]string{"y", "n"}, input) {
				inputChan <- strings.TrimSpace(input)
				return
			} else {
				fmt.Println("Invalid input. Please press 'y' or 'n'.")
			}
		}
	}()

	select {
	case input := <-inputChan:
		if input == "n" || input == "no" {
			return false
		}
		return true
	case <-time.After(timeout):
		fmt.Printf("\n")
		return true
	}
}

func getExportStatus(deName, namespace string, public bool, rtClient ctrlrtclient.Client) (podUrl, volumeMode, internalCAData string, err error) {
	fmt.Printf("Waiting for DataExport %s/%s to be ready ...\n", namespace, deName)
	deObj, err := GetDataExportWithRestart(deName, namespace, rtClient)
	if err != nil {
		return
	}

	//TODO can get from URL param, Cluster flag, KubeConfig
	if public {
		if deObj.Status.PublicURL == "" {
			err = fmt.Errorf("empty PublicURL")
			return
		}
		podUrl = deObj.Status.PublicURL
		if !strings.HasPrefix(podUrl, "http") {
			podUrl += "https://"
		}
	} else if deObj.Status.Url != "" {
		podUrl = deObj.Status.Url
		internalCAData = deObj.Status.CA
	} else {
		err = fmt.Errorf("invalid URL")
		return
	}

	volumeKind := deObj.Spec.TargetRef.Kind
	if !slices.Contains([]string{"PersistentVolumeClaim", "VolumeSnapshot"}, volumeKind) {
		err = fmt.Errorf("invalid volume kind: %s", volumeKind)
		return
	}

	volumeMode = deObj.Status.VolumeMode
	fmt.Printf("DataExport %s/%s is ready\n", namespace, deName)
	return
}

func PrepareDownload(deName, namespace string, publish bool, sClient *safeClient.SafeClient) (url, volumeMode string, subClient *safeClient.SafeClient, finErr error) {
	rtClient, err := sClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		finErr = err
		return
	}

	podUrl, volumeMode, intrenalCAData, err := getExportStatus(deName, namespace, publish, rtClient)
	if err != nil {
		finErr = err
		return
	}

	// Validate srcPath, dstPath params
	switch volumeMode {
	case "Filesystem":
		url, err = neturl.JoinPath(podUrl, "api/v1/files")
		if err != nil {
			finErr = err
			return
		}
	case "Block":
		url, err = neturl.JoinPath(podUrl, "api/v1/block")
		if err != nil {
			finErr = err
			return
		}
	default:
		finErr = fmt.Errorf("invalid volume mode: '%s'", volumeMode)
		return
	}

	subClient = sClient.Copy()

	if publish {
		subClient.SetTLSCAData([]byte{})

	} else if len(intrenalCAData) > 0 {
		decodedBytes, err := base64.StdEncoding.DecodeString(intrenalCAData)
		if err != nil {
			finErr = fmt.Errorf("CA decoding error: %s", err.Error())
			return
		}
		subClient.SetTLSCAData(decodedBytes)
	}

	return
}
