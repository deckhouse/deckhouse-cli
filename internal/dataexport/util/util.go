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

package datautil

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	neturl "net/url"
	"os"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	defaultTTL                = "2m"
	PersistentVolumeClaimKind = "PersistentVolumeClaim"
	VolumeSnapshotKind        = "VolumeSnapshot"
	VirtualDiskKind           = "VirtualDisk"
	VirtualDiskSnapshotKind   = "VirtualDiskSnapshot"
)

var (
	ErrUnsupportedVolumeMode = errors.New("invalid volume mode")
)

// Function pointers for test stubbing
var (
	PrepareDownloadFunc            = PrepareDownload
	CreateDataExporterIfNeededFunc = CreateDataExporterIfNeeded
)

func GetDataExport(ctx context.Context, deName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataExport, error) {
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

func GetDataExportWithRestart(ctx context.Context, deName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataExport, error) {
	deObj := &v1alpha1.DataExport{}

	for i := 0; ; i++ {
		var returnErr error

		// get DataExport from k8s by name
		err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
		if err != nil {
			return nil, fmt.Errorf("kube Get dataexport with restart: %s", err.Error())
		}

		for _, condition := range deObj.Status.Conditions {
			// restart DataExport if Expired
			if condition.Type == "Expired" {
				if condition.Status == "True" {
					if err := DeleteDataExport(ctx, deName, namespace, rtClient); err != nil {
						return nil, err
					}
					if err := CreateDataExport(
						ctx,
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
		if returnErr == nil && deObj.Status.URL == "" {
			returnErr = fmt.Errorf("DataExport %s/%s has no URL", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
		} else if deObj.Spec.Publish && deObj.Status.PublicURL == "" {
			returnErr = fmt.Errorf("DataExport %s/%s has empty PublicURL", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
		}

		if returnErr == nil {
			break
		}
		if i > 60 {
			return nil, returnErr
		}
		time.Sleep(time.Second * 3)
	}

	return deObj, nil
}

func CreateDataExporterIfNeeded(ctx context.Context, log *slog.Logger, deName, namespace string, publish bool, ttl string, rtClient ctrlrtclient.Client) (string, error) {
	var volumeKind, volumeName string
	lowerCaseDeName := strings.ToLower(deName)

	switch {
	// PVC / PersistentVolumeClaim
	case strings.HasPrefix(lowerCaseDeName, "pvc/"):
		volumeKind = PersistentVolumeClaimKind
		volumeName = deName[4:]
		deName = "de-pvc-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "persistentvolumeclaim/"):
		volumeKind = PersistentVolumeClaimKind
		volumeName = deName[len("persistentvolumeclaim/"):]
		deName = "de-pvc-" + volumeName

	// VS / VolumeSnapshot
	case strings.HasPrefix(lowerCaseDeName, "vs/"):
		volumeKind = VolumeSnapshotKind
		volumeName = deName[3:]
		deName = "de-vs-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "volumesnapshot/"):
		volumeKind = VolumeSnapshotKind
		volumeName = deName[len("volumesnapshot/"):]
		deName = "de-vs-" + volumeName

	// VD / VirtualDisk
	case strings.HasPrefix(lowerCaseDeName, "vd/"):
		volumeKind = VirtualDiskKind
		volumeName = deName[3:]
		deName = "de-vd-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "virtualdisk/"):
		volumeKind = VirtualDiskKind
		volumeName = deName[len("virtualdisk/"):]
		deName = "de-vd-" + volumeName

	// VDS / VirtualDiskSnapshot
	case strings.HasPrefix(lowerCaseDeName, "vds/"):
		volumeKind = VirtualDiskSnapshotKind
		volumeName = deName[4:]
		deName = "de-vds-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "virtualdisksnapshot/"):
		volumeKind = VirtualDiskSnapshotKind
		volumeName = deName[len("virtualdisksnapshot/"):]
		deName = "de-vds-" + volumeName

	default:
		return deName, nil
	}

	err := CreateDataExport(ctx, deName, namespace, ttl, volumeKind, volumeName, publish, rtClient)
	if err != nil {
		return deName, err
	}
	log.Info("DataExport creating", slog.String("name", deName), slog.String("namespace", namespace))

	return deName, nil
}

func CreateDataExport(ctx context.Context, deName, namespace, ttl, volumeKind, volumeName string, publish bool, rtClient ctrlrtclient.Client) error {
	if ttl == "" {
		ttl = defaultTTL
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
			TTL: ttl,
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

func DeleteDataExport(ctx context.Context, deName, namespace string, rtClient ctrlrtclient.Client) error {
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
			}
			fmt.Println("Invalid input. Please press 'y' or 'n'.")
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

func getExportStatus(ctx context.Context, log *slog.Logger, deName, namespace string, public bool, rtClient ctrlrtclient.Client) (string, string, string, error) {
	log.Info("Waiting for DataExport to be ready", slog.String("name", deName), slog.String("namespace", namespace))
	deObj, err := GetDataExportWithRestart(ctx, deName, namespace, rtClient)
	if err != nil {
		return "", "", "", err
	}

	var podURL, volumeMode, internalCAData string

	switch {
	case public:
		if deObj.Status.PublicURL == "" {
			return "", "", "", fmt.Errorf("empty PublicURL")
		}
		podURL = deObj.Status.PublicURL
		if !strings.HasPrefix(podURL, "http") {
			podURL += "https://"
		}
	case deObj.Status.URL != "":
		podURL = deObj.Status.URL
		internalCAData = deObj.Status.CA
	default:
		return "", "", "", fmt.Errorf("invalid URL")
	}

	volumeKind := deObj.Spec.TargetRef.Kind
	if !slices.Contains([]string{PersistentVolumeClaimKind, VolumeSnapshotKind, VirtualDiskKind, VirtualDiskSnapshotKind}, volumeKind) {
		return "", "", "", fmt.Errorf("invalid volume kind: %s", volumeKind)
	}

	volumeMode = deObj.Status.VolumeMode
	log.Info("DataExport is ready", slog.String("name", deName), slog.String("namespace", namespace), slog.String("url", podURL), slog.String("volumeMode", volumeMode))
	return podURL, volumeMode, internalCAData, nil
}

func PrepareDownload(ctx context.Context, log *slog.Logger, deName, namespace string, publish bool, sClient *safeClient.SafeClient) (string, string, *safeClient.SafeClient, error) {
	rtClient, err := sClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return "", "", nil, err
	}

	podURL, volumeMode, intrenalCAData, err := getExportStatus(ctx, log, deName, namespace, publish, rtClient)
	if err != nil {
		return "", "", nil, err
	}

	var url string
	// Validate srcPath, dstPath params
	switch volumeMode {
	case "Filesystem":
		url, err = neturl.JoinPath(podURL, "api/v1/files")
		if err != nil {
			return "", "", nil, err
		}
	case "Block":
		url, err = neturl.JoinPath(podURL, "api/v1/block")
		if err != nil {
			return "", "", nil, err
		}
	default:
		return "", "", nil, fmt.Errorf("%w: '%s'", ErrUnsupportedVolumeMode, volumeMode)
	}

	// Reuse the original SafeClient unless we need to inject additional CA.
	subClient := sClient

	if !publish && len(intrenalCAData) > 0 {
		// Create an isolated copy to avoid mutating the original client
		subClient = sClient.Copy()
		decodedBytes, err := base64.StdEncoding.DecodeString(intrenalCAData)
		if err != nil {
			return "", "", nil, fmt.Errorf("CA decoding error: %s", err.Error())
		}
		subClient.SetTLSCAData(decodedBytes)
	}

	return url, volumeMode, subClient, nil
}
