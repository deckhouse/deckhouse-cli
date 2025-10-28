package util

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	neturl "net/url"
	"strconv"
	"time"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxRetryAttempts = 60
	retryInterval    = 3
)

func GetDataImport(ctx context.Context, diName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataImport, error) {
	diObj := &v1alpha1.DataImport{}
	err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: diName}, diObj)
	if err != nil {
		return nil, fmt.Errorf("kube Get dataimport: %s", err.Error())
	}

	for _, condition := range diObj.Status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status != "True" {
				return nil, fmt.Errorf("DataImport %s/%s is not Ready", diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
			}
			break
		}
	}

	return diObj, nil
}

func DeleteDataImport(ctx context.Context, diName, namespace string, rtClient ctrlrtclient.Client) error {
	diObj := &v1alpha1.DataImport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      diName,
			Namespace: namespace,
		},
	}
	err := rtClient.Delete(ctx, diObj)
	return err
}

func CreateDataImport(
	ctx context.Context,
	name, namespace, ttl string,
	publish, waitForFirstConsumer bool,
	pvcTpl *v1alpha1.PersistentVolumeClaimTemplateSpec,
	rtClient ctrlrtclient.Client,
) error {
	if ttl == "" {
		ttl = dataio.DefaultTTL
	}

	obj := &v1alpha1.DataImport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "DataImport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.DataImportSpec{
			Ttl:                  ttl,
			Publish:              publish,
			WaitForFirstConsumer: waitForFirstConsumer,
			TargetRef: v1alpha1.DataImportTargetRefSpec{
				Kind:        "PersistentVolumeClaim",
				PvcTemplate: pvcTpl,
			},
		},
	}

	if err := rtClient.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("DataImport create error: %s", err.Error())
	}
	return nil
}

func GetDataImportWithRestart(
	ctx context.Context,
	diName, namespace string,
	rtClient ctrlrtclient.Client,
) (*v1alpha1.DataImport, error) {
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		diObj := &v1alpha1.DataImport{}
		if err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: diName}, diObj); err != nil {
			return nil, fmt.Errorf("kube Get dataimport with ready: %s", err.Error())
		}

		var notReadyErr error
		for _, condition := range diObj.Status.Conditions {
			if condition.Type == "Expired" && condition.Status == "True" {
				if err := DeleteDataImport(ctx, diName, namespace, rtClient); err != nil {
					return nil, err
				}
				pvcTemplate := &v1alpha1.PersistentVolumeClaimTemplateSpec{}
				if diObj.Spec.TargetRef.PvcTemplate != nil {
					pvcTemplate = diObj.Spec.TargetRef.PvcTemplate
				}
				if err := CreateDataImport(
					ctx,
					diName,
					namespace,
					diObj.Spec.Ttl,
					diObj.Spec.Publish,
					diObj.Spec.WaitForFirstConsumer,
					pvcTemplate,
					rtClient,
				); err != nil {
					return nil, err
				}
			}
			if condition.Type == "Ready" {
				if condition.Status != "True" {
					notReadyErr = fmt.Errorf("DataImport %s/%s is not Ready", diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
				}
			}
		}

		if notReadyErr == nil {
			if diObj.Spec.Publish {
				if diObj.Status.PublicURL == "" {
					notReadyErr = fmt.Errorf("DataImport %s/%s has empty PublicURL", diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
				}
			} else if diObj.Status.Url == "" {
				notReadyErr = fmt.Errorf("DataImport %s/%s has no URL", diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
			}
		}

		if notReadyErr == nil && diObj.Status.VolumeMode == "" {
			notReadyErr = fmt.Errorf("DataImport %s/%s has empty VolumeMode", diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
		}

		if notReadyErr == nil {
			return diObj, nil
		}
		if i > maxRetryAttempts {
			return nil, notReadyErr
		}
		time.Sleep(retryInterval * time.Second)
	}
}

func PrepareUpload(
	ctx context.Context,
	log *slog.Logger,
	diName, namespace string,
	publish bool,
	sClient *safeClient.SafeClient,
) (url, volumeMode string, subClient *safeClient.SafeClient, finErr error) {
	rtClient, err := sClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		finErr = err
		return
	}

	diObj, err := GetDataImportWithRestart(ctx, diName, namespace, rtClient)
	if err != nil {
		finErr = err
		return
	}

	var podURL string
	if publish {
		if diObj.Status.PublicURL == "" {
			finErr = fmt.Errorf("empty PublicURL")
			return
		}
		podURL = diObj.Status.PublicURL
	} else if diObj.Status.Url != "" {
		podURL = diObj.Status.Url
	} else {
		finErr = fmt.Errorf("invalid URL")
		return
	}

	volumeMode = diObj.Status.VolumeMode
	switch volumeMode {
	case "Filesystem":
		url, err = neturl.JoinPath(podURL, "api/v1/files")
		if err != nil {
			finErr = err
			return
		}
	case "Block":
		url, err = neturl.JoinPath(podURL, "api/v1/block")
		if err != nil {
			finErr = err
			return
		}
	default:
		finErr = fmt.Errorf("%w: '%s'", dataio.ErrUnsupportedVolumeMode, volumeMode)
		return
	}

	subClient = sClient
	if !publish && len(diObj.Status.CA) > 0 {
		subClient = sClient.Copy()
		decodedBytes, err := base64.StdEncoding.DecodeString(diObj.Status.CA)
		if err != nil {
			finErr = fmt.Errorf("CA decoding error: %s", err.Error())
			return
		}
		subClient.SetTLSCAData(decodedBytes)
	}

	return
}

func CheckUploadProgress(ctx context.Context, httpClient *safeClient.SafeClient, targetURL string) (int64, error) {
	req, err := http.NewRequest(http.MethodHead, targetURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.HTTPDo(req.WithContext(ctx))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if next := resp.Header.Get("X-Next-Offset"); next != "" {
			if serverOffset, perr := strconv.ParseInt(next, 10, 64); perr == nil && serverOffset >= 0 {
				return serverOffset, nil
			}
			return 0, fmt.Errorf("invalid X-Next-Offset header")
		}
		return 0, nil
	case http.StatusNotFound:
		return 0, nil
	default:
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
}
