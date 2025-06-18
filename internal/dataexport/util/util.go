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
	"context"
	"fmt"

	coreapi "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/api/v1alpha1"
)

func NewKubeRTClient(clientCmdConfig clientcmd.ClientConfig) (ctrlrtclient.Client, error) {
	restCfg, err := clientCmdConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("client config init error: %s", err.Error())
	}

	var resourcesSchemeFuncs = []func(*apiruntime.Scheme) error{
		kubescheme.AddToScheme,
		v1alpha1.AddToScheme,
	}

	scheme := apiruntime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			return nil, err
		}
	}
	clientOpts := ctrlrtclient.Options{
		Scheme: scheme,
	}

	cl, err := ctrlrtclient.New(restCfg, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("kubernetes runtime client error: %s", err.Error())
	}

	return cl, nil
}

func GetDataExportWithRestart(deName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataExport, error) {
	ctx := context.Background()

	// Get DataExport from k8s by name
	deObj := &v1alpha1.DataExport{}
	err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
	if err != nil {
		return nil, fmt.Errorf("kube Get dataexport: %s", err.Error())
	}

	// Restart DataExport if Expired
	for _, condition := range deObj.Status.Conditions {
		if condition.Type == "Expired" {
			if condition.Status == "True" {
				if err := DeleteDataExport(deName, namespace, rtClient); err != nil {
					return nil, err
				}
				if err := rtClient.Create(ctx, deObj); err != nil {
					return nil, err
				}
			}
			break
		}
	}

	// Check DataExport is Ready
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

func CreateDataExport(deName, namespace, ttl, volumeKind, volumeName string, publish bool, rtClient ctrlrtclient.Client) error {
	ctx := context.Background()

	// Create namespace if not exist
	nsObject := &coreapi.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	err := rtClient.Create(ctx, nsObject)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	// Create dataexport object
	deCfg := &v1alpha1.DataExport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "deckhouse.io/v1alpha1",
			Kind:       "DataExport",
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
	err = rtClient.Create(ctx, deCfg)
	if err != nil {
		return err
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
