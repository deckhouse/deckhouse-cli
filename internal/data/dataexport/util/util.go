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
	"encoding/base64"
	"fmt"
	"log/slog"
	neturl "net/url"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// Function pointers for test stubbing
var (
	PrepareDownloadFunc            = PrepareDownload
	CreateDataExporterIfNeededFunc = CreateDataExporterIfNeeded
	// ResolveClientFunc is stubbable because resolution is the first thing a command does and
	// the only step that contacts the API server before any of the work under test.
	ResolveClientFunc = ResolveClient
)

// var instead of const to allow test override.
var maxRetryAttempts = 60

// ResolveClient resolves which of the two producers serves DataExport to this user in this
// namespace, and returns both the answer and a control-plane client bound to it.
//
// Every `d8 data export` subcommand starts here rather than registering a fixed group, because
// the same binary ships to clusters that serve only storage-foundation's group, only
// storage-volume-data-manager's, or both with the user authorized for one of them. The resolved
// backend is returned alongside the client because callers that build their own client later
// (PrepareDownload) must bind it to the same group.
func ResolveClient(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	namespace string,
	log *slog.Logger,
) (dataapi.Backend, ctrlrtclient.Client, error) {
	backend, err := dataapi.Resolve(ctx, sClient.RESTConfig(), dataapi.ResourceDataExports, namespace, log)
	if err != nil {
		return dataapi.Backend{}, nil, err
	}

	rtClient, err := sClient.NewRTClient(v1alpha1.AddToSchemeFor(backend.GroupVersion))
	if err != nil {
		return dataapi.Backend{}, nil, err
	}

	return backend, rtClient, nil
}

func GetDataExport(ctx context.Context, deName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataExport, error) {
	deObj := &v1alpha1.DataExport{}

	err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
	if err != nil {
		return nil, fmt.Errorf("kube Get dataexport: %s", err.Error())
	}

	// An object with no Ready condition at all has not been reconciled yet rather than failed,
	// so only a Ready condition that is present and not True is an error here.
	if notReady := dataio.NotReady(deObj.Status.Conditions); notReady != nil {
		return nil, fmt.Errorf("DataExport %s/%s is not Ready: %s (%s)",
			deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name,
			notReady.Message, notReady.Reason)
	}

	return deObj, nil
}

func GetDataExportWithRestart(ctx context.Context, deName, namespace string, rtClient ctrlrtclient.Client, log *slog.Logger) (*v1alpha1.DataExport, error) {
	deObj := &v1alpha1.DataExport{}

	for i := 0; ; i++ {
		var returnErr error

		// get DataExport from k8s by name
		err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
		if err != nil {
			return nil, fmt.Errorf("kube Get dataexport with restart: %s", err.Error())
		}

		// An expired export is recreated here rather than waited out: after expiry the producer's
		// garbage collector only removes the object once its retention TTL runs out, so polling
		// would stall for the whole of that retention. Both producers' spellings of expiry are
		// recognised by dataio.IsExpired.
		switch {
		case dataio.IsExpired(deObj.Status.Conditions):
			// Resolved BEFORE the delete, and never fatal. Everything needed to rebuild the
			// export has to be in hand before the existing one is destroyed: failing between the
			// two would leave the user with neither, and this object is theirs, not ours.
			group := recreateTargetGroup(deObj, log)

			if err := DeleteDataExport(ctx, deName, namespace, rtClient); err != nil {
				return nil, err
			}

			if err := CreateDataExport(
				ctx,
				deName, namespace, "",
				group,
				deObj.Spec.TargetRef.Kind,
				deObj.Spec.TargetRef.Name,
				deObj.Spec.Publish, rtClient,
			); err != nil {
				return nil, err
			}
			// Recreated: keep retrying until the fresh export becomes Ready.
			returnErr = fmt.Errorf("DataExport %s/%s expired; recreated, waiting for the new export to become Ready",
				deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
		default:
			if notReady := dataio.NotReady(deObj.Status.Conditions); notReady != nil {
				returnErr = fmt.Errorf("DataExport %s/%s is not Ready: %s (%s)",
					deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name,
					notReady.Message, notReady.Reason)
			}
		}
		// check DataExport Url
		if returnErr == nil {
			switch {
			case deObj.Status.URL == "":
				returnErr = fmt.Errorf("DataExport %s/%s: waiting for internal URL to be assigned by controller",
					deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
			case deObj.Spec.Publish && deObj.Status.PublicURL == "":
				returnErr = fmt.Errorf("DataExport %s/%s: waiting for public URL to be created by controller",
					deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
			}
		}

		if returnErr == nil {
			break
		}

		if i > maxRetryAttempts {
			return nil, fmt.Errorf("%w\n\nTo inspect DataExport status, run:\n  d8 k -n %s get de %s -o yaml",
				returnErr, namespace, deName)
		}
		// Every fifth attempt we output it to the terminal so that the user can see the error.
		if i > 0 && i%5 == 0 {
			remaining := time.Duration((maxRetryAttempts-i)*3) * time.Second
			log.Info("Still waiting for DataExport to be ready",
				slog.String("name", deName),
				slog.String("status", returnErr.Error()),
				slog.String("timeout_in", remaining.String()),
				slog.Int("attempt", i))
		}

		time.Sleep(time.Second * 3)
	}

	return deObj, nil
}

// recreateTargetGroup picks the API group to stamp on an export being rebuilt after expiry.
//
// The group is derived from the kind rather than read back off the object, because
// storage-volume-data-manager's schema has no targetRef.group property and prunes the one the CLI
// sent: an export read from that producer never carries a group, and copying the empty value would
// retarget the rebuilt export at the core group.
//
// An unrecognised kind is not an error here. storage-foundation puts no enum on targetRef.kind and
// documents that any snapshot kind is resolved generically, so a DataExport may legitimately target
// a kind this CLI has never heard of. For those, whatever group the server already recorded is the
// best available answer — and far better than refusing, which at this point in the recreate would
// destroy an object the user still owns.
func recreateTargetGroup(deObj *v1alpha1.DataExport, log *slog.Logger) string {
	group, err := dataio.KindToGroup(deObj.Spec.TargetRef.Kind)
	if err == nil {
		return group
	}

	log.Warn("Rebuilding an expired DataExport for a target kind this CLI does not know; keeping the group recorded on the object",
		slog.String("kind", deObj.Spec.TargetRef.Kind),
		slog.String("group", deObj.Spec.TargetRef.Group))

	return deObj.Spec.TargetRef.Group
}

func CreateDataExporterIfNeeded(ctx context.Context, log *slog.Logger, deName, namespace string, publish bool, ttl string, rtClient ctrlrtclient.Client) (string, error) {
	var volumeKind, volumeName string

	lowerCaseDeName := strings.ToLower(deName)

	switch {
	// PVC / PersistentVolumeClaim
	case strings.HasPrefix(lowerCaseDeName, "pvc/"):
		volumeKind = dataio.PersistentVolumeClaimKind
		volumeName = deName[4:]
		deName = "de-pvc-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "persistentvolumeclaim/"):
		volumeKind = dataio.PersistentVolumeClaimKind
		volumeName = deName[len("persistentvolumeclaim/"):]
		deName = "de-pvc-" + volumeName

	// VS / VolumeSnapshot
	case strings.HasPrefix(lowerCaseDeName, "vs/"):
		volumeKind = dataio.VolumeSnapshotKind
		volumeName = deName[3:]
		deName = "de-vs-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "volumesnapshot/"):
		volumeKind = dataio.VolumeSnapshotKind
		volumeName = deName[len("volumesnapshot/"):]
		deName = "de-vs-" + volumeName

	// VD / VirtualDisk
	case strings.HasPrefix(lowerCaseDeName, "vd/"):
		volumeKind = dataio.VirtualDiskKind
		volumeName = deName[3:]
		deName = "de-vd-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "virtualdisk/"):
		volumeKind = dataio.VirtualDiskKind
		volumeName = deName[len("virtualdisk/"):]
		deName = "de-vd-" + volumeName

	// VDS / VirtualDiskSnapshot
	case strings.HasPrefix(lowerCaseDeName, "vds/"):
		volumeKind = dataio.VirtualDiskSnapshotKind
		volumeName = deName[4:]
		deName = "de-vds-" + volumeName
	case strings.HasPrefix(lowerCaseDeName, "virtualdisksnapshot/"):
		volumeKind = dataio.VirtualDiskSnapshotKind
		volumeName = deName[len("virtualdisksnapshot/"):]
		deName = "de-vds-" + volumeName

	default:
		return deName, nil
	}

	group, err := dataio.KindToGroup(volumeKind)
	if err != nil {
		return deName, err
	}

	err = CreateDataExport(ctx, deName, namespace, ttl, group, volumeKind, volumeName, publish, rtClient)
	if err != nil {
		return deName, err
	}

	log.Info("DataExport creating", slog.String("name", deName), slog.String("namespace", namespace))

	return deName, nil
}

// CreateDataExport creates a DataExport CR targeting the object identified by group, kind, and volumeName.
// group is the API group ("" for core, e.g. "snapshot.storage.k8s.io" for VolumeSnapshot).
// kind is the target object kind (e.g. "VolumeSnapshot", "PersistentVolumeClaim").
func CreateDataExport(ctx context.Context, deName, namespace, ttl, group, kind, volumeName string, publish bool, rtClient ctrlrtclient.Client) error {
	if ttl == "" {
		ttl = dataio.DefaultTTL
	}

	// Create dataexport object. TypeMeta is left empty on purpose: the client stamps the
	// apiVersion from the scheme it was built with, which is the group resolved for this run —
	// a literal here would name one producer's group on every request, including requests to
	// the other one.
	deCfg := &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Spec: v1alpha1.DataexportSpec{
			TTL: ttl,
			TargetRef: v1alpha1.TargetRefSpec{
				Group: group,
				Kind:  kind,
				Name:  volumeName,
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

func getExportStatus(ctx context.Context, log *slog.Logger, deName, namespace string, public bool, rtClient ctrlrtclient.Client) ( /*podURL*/ string /*volumeMode*/, string /*internalCAData*/, string, error) {
	var podURL, volumeMode, internalCAData string

	// Fetch the current state so we can reconcile Spec.Publish before waiting.
	deObj := new(v1alpha1.DataExport)

	err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: deName}, deObj)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to get dataExport: %w", err)
	}

	// Patch Spec.Publish if the resolved value differs from what the object has.
	// Must happen before GetDataExportWithRestart so the loop waits for PublicURL
	// when publish=true.
	if err = EnsureDataExportPublish(ctx, deObj, public, rtClient); err != nil {
		return "", "", "", err
	}

	log.Info("Waiting for DataExport to be ready", slog.String("name", deName), slog.String("namespace", namespace))

	deObj, err = GetDataExportWithRestart(ctx, deName, namespace, rtClient, log)
	if err != nil {
		return "", "", "", err
	}

	switch {
	case public:
		podURL = deObj.Status.PublicURL
		if !strings.HasPrefix(podURL, "http") {
			podURL = "https://" + podURL
		}
	case deObj.Status.URL != "":
		podURL = deObj.Status.URL
		internalCAData = deObj.Status.CA
	default:
		return "", "", "", fmt.Errorf("invalid URL")
	}

	if !slices.Contains([]string{
		dataio.PersistentVolumeClaimKind,
		dataio.VolumeSnapshotKind,
		dataio.VirtualDiskKind,
		dataio.VirtualDiskSnapshotKind,
	}, deObj.Spec.TargetRef.Kind) {
		return "", "", "", fmt.Errorf("invalid volume kind: %s", deObj.Spec.TargetRef.Kind)
	}

	volumeMode = deObj.Status.VolumeMode
	log.Info("DataExport is ready", slog.String("name", deName), slog.String("namespace", namespace), slog.String("url", podURL), slog.String("volumeMode", volumeMode))

	return podURL, volumeMode, internalCAData, nil
}

// PrepareDownload waits for the export to be usable and returns the data-plane URL, the volume
// mode and a client trusting the exporter's CA. backend is the group resolved for this run; it
// builds this function's own control-plane client, which would otherwise default to
// storage-foundation's group regardless of what the cluster serves.
func PrepareDownload(ctx context.Context, log *slog.Logger, backend dataapi.Backend, deName, namespace string, publish bool, sClient *safeClient.SafeClient) (string, string, *safeClient.SafeClient, error) {
	var (
		url, volumeMode string
		subClient       *safeClient.SafeClient
		decodedBytes    []byte
	)

	rtClient, err := sClient.NewRTClient(v1alpha1.AddToSchemeFor(backend.GroupVersion))
	if err != nil {
		return "", "", nil, err
	}

	podURL, volumeMode, internalCAData, err := getExportStatus(ctx, log, deName, namespace, publish, rtClient)
	if err != nil {
		return "", "", nil, err
	}

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
		return "", "", nil, fmt.Errorf("%w: '%s'", dataio.ErrUnsupportedVolumeMode, volumeMode)
	}

	if len(internalCAData) > 0 {
		decodedBytes, err = base64.StdEncoding.DecodeString(internalCAData)
		if err != nil {
			return "", "", nil, fmt.Errorf("CA decoding error: %s", err.Error())
		}
	}

	// Create an isolated copy to avoid mutating the original client
	subClient = sClient.Copy()
	// Always call SetTLSCAData to build a merged trust pool (system CAs + kubeconfig CA + internal CA if present)
	subClient.SetTLSCAData(decodedBytes)

	return url, volumeMode, subClient, nil
}

// EnsureDataExportPublish patches DataExport.Spec.Publish to match the resolved value.
// Only upgrades publish: false -> true is patched, true -> false is intentionally skipped
// to avoid downgrading already-published resources.
func EnsureDataExportPublish(
	ctx context.Context,
	deObj *v1alpha1.DataExport,
	publish bool,
	rtClient ctrlrtclient.Client,
) error {
	if !publish {
		return nil
	}

	if deObj == nil {
		return fmt.Errorf("nil DataExport object")
	}

	if deObj.Spec.Publish == publish {
		return nil
	}

	patch := ctrlrtclient.MergeFrom(deObj.DeepCopy())
	deObj.Spec.Publish = publish

	if err := rtClient.Patch(ctx, deObj, patch); err != nil {
		return fmt.Errorf("patch DataExport publish: %w", err)
	}

	return nil
}
