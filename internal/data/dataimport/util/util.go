package util

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// var instead of const to allow test override.
var maxRetryAttempts = 60

// ResolveClientFunc is a function pointer for test stubbing: resolution is the first thing a
// command does and the only step that contacts the API server before any of the work under test.
var ResolveClientFunc = ResolveClient

const (
	retryInterval = 3

	// uploadFinishedSubpath is the importer endpoint that finalises an upload. It hangs off the
	// same base URL as the /api/v1/{files,block} data endpoints (status.url / status.publicURL).
	uploadFinishedSubpath = "api/v1/finished"
)

// ResolveClient resolves which of the two producers serves DataImport to this user in this
// namespace, and returns both the answer and a control-plane client bound to it.
//
// Every `d8 data import` subcommand starts here rather than registering a fixed group, because
// the same binary ships to clusters that serve only storage-foundation's group, only
// storage-volume-data-manager's, or both with the user authorized for one of them. The resolved
// backend is returned alongside the client because the request body differs between the two
// producers (see CreateDataImport) and because callers that build their own client later
// (PrepareUpload) must bind it to the same group.
func ResolveClient(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	namespace string,
	log *slog.Logger,
) (dataapi.Backend, ctrlrtclient.Client, error) {
	backend, err := dataapi.Resolve(ctx, sClient.RESTConfig(), dataapi.ResourceDataImports, namespace, log)
	if err != nil {
		return dataapi.Backend{}, nil, err
	}

	rtClient, err := sClient.NewRTClient(v1alpha1.AddToSchemeFor(backend.GroupVersion))
	if err != nil {
		return dataapi.Backend{}, nil, err
	}

	return backend, rtClient, nil
}

func GetDataImport(ctx context.Context, diName, namespace string, rtClient ctrlrtclient.Client) (*v1alpha1.DataImport, error) {
	diObj := &v1alpha1.DataImport{}

	err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: diName}, diObj)
	if err != nil {
		return nil, fmt.Errorf("kube Get dataimport: %s", err.Error())
	}

	// An object with no Ready condition at all has not been reconciled yet rather than failed,
	// so only a Ready condition that is present and not True is an error here.
	if notReady := dataio.NotReady(diObj.Status.Conditions); notReady != nil {
		return nil, fmt.Errorf("DataImport %s/%s is not Ready: %s (%s)",
			diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name,
			notReady.Message, notReady.Reason)
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

// CreateDataImport creates a DataImport that streams the uploaded bytes into a newly created PVC,
// in the request shape the resolved backend understands.
//
// backend selects that shape, and there is no shape that satisfies both producers: whichever one
// is addressed rejects a body written for the other. storage-foundation requires spec.mode plus a
// spec.pvcTemplate at the root and has no targetRef property; storage-volume-data-manager requires
// spec.targetRef carrying the same template and has no mode.
func CreateDataImport(
	ctx context.Context,
	backend dataapi.Backend,
	name, namespace, ttl string,
	publish, waitForFirstConsumer bool,
	pvcTpl *v1alpha1.PersistentVolumeClaimTemplateSpec,
	rtClient ctrlrtclient.Client,
) error {
	if ttl == "" {
		ttl = dataio.DefaultTTL
	}

	// Both producers name the imported PVC after the template's metadata.name and reject an
	// empty one. Fail early with a clear message instead of surfacing an opaque admission error.
	if pvcTpl == nil || pvcTpl.Name == "" {
		return fmt.Errorf("DataImport %s/%s requires a PVC template with metadata.name set", namespace, name)
	}

	spec := v1alpha1.DataImportSpec{
		TTL:                  ttl,
		Publish:              publish,
		WaitForFirstConsumer: waitForFirstConsumer,
	}

	if backend.Legacy() {
		spec.TargetRef = &v1alpha1.DataImportTargetRefSpec{
			Kind:        v1alpha1.PersistentVolumeClaimKind,
			PvcTemplate: pvcTpl,
		}
	} else {
		// Mode is sent explicitly even though the CRD defaults it: mode is immutable after
		// creation, so relying on the server-side default would silently bind the object to
		// whatever default a future CRD revision ships.
		spec.Mode = v1alpha1.DataImportModeCreatePVC
		spec.PvcTemplate = pvcTpl
	}

	// TypeMeta is left empty on purpose: the client stamps the apiVersion from the scheme it was
	// built with, which is the group resolved for this run — a literal here would name one
	// producer's group on every request, including requests to the other one.
	obj := &v1alpha1.DataImport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: spec,
	}

	if err := rtClient.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("DataImport create error: %s", err.Error())
	}

	return nil
}

func GetDataImportWithRestart(
	ctx context.Context,
	backend dataapi.Backend,
	diName, namespace string,
	rtClient ctrlrtclient.Client,
	log *slog.Logger,
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

		// An expired import is recreated here rather than waited out: after expiry the producer's
		// garbage collector only removes the object once its retention TTL runs out, so polling
		// would stall for the whole of that retention. Both producers' spellings of expiry are
		// recognised by dataio.IsExpired.
		switch {
		case dataio.IsExpired(diObj.Status.Conditions):
			if err := DeleteDataImport(ctx, diName, namespace, rtClient); err != nil {
				return nil, err
			}

			// DestinationTemplate reads the template from whichever of the two spec shapes the
			// producer that wrote this object uses; reading Spec.PvcTemplate directly would
			// recreate a storage-volume-data-manager import with an empty template.
			pvcTemplate := diObj.Spec.DestinationTemplate()
			if pvcTemplate == nil {
				pvcTemplate = &v1alpha1.PersistentVolumeClaimTemplateSpec{}
			}

			if err := CreateDataImport(
				ctx,
				backend,
				diName,
				namespace,
				diObj.Spec.TTL,
				diObj.Spec.Publish,
				diObj.Spec.WaitForFirstConsumer,
				pvcTemplate,
				rtClient,
			); err != nil {
				return nil, err
			}
			// Recreated: the stale object's status.url/status.volumeMode still belong to the
			// dead importer until retention-GC reaps it, so we must not let this object fall
			// through to the readiness checks below as if it were done. Keep retrying until the
			// fresh import becomes Ready.
			notReadyErr = fmt.Errorf("DataImport %s/%s expired; recreated, waiting for the new import to become Ready",
				diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
		default:
			if notReady := dataio.NotReady(diObj.Status.Conditions); notReady != nil {
				notReadyErr = fmt.Errorf("DataImport %s/%s is not Ready: %s (%s)",
					diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name,
					notReady.Message, notReady.Reason)
			}
		}

		if notReadyErr == nil {
			if diObj.Spec.Publish {
				if diObj.Status.PublicURL == "" {
					notReadyErr = fmt.Errorf("DataImport %s/%s has empty PublicURL", diObj.ObjectMeta.Namespace, diObj.ObjectMeta.Name)
				}
			} else if diObj.Status.URL == "" {
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
		// Every fifth attempt we output it to the terminal so that the user can see the error.
		if i > 0 && i%5 == 0 {
			log.Info("Still waiting for DataImport to be ready",
				slog.String("name", diName),
				slog.String("status", notReadyErr.Error()),
				slog.Int("attempt", i))
		}

		time.Sleep(retryInterval * time.Second)
	}
}

// PrepareUpload waits for the import to be usable and returns the data-plane URL, the importer
// base URL, the volume mode and a client trusting the importer's CA. backend is the group
// resolved for this run; it builds this function's own control-plane client, which would
// otherwise default to storage-foundation's group regardless of what the cluster serves.
func PrepareUpload(
	ctx context.Context,
	backend dataapi.Backend,
	diName, namespace string,
	publish bool,
	sClient *safeClient.SafeClient,
	log *slog.Logger,
) ( /*url*/ string /*baseURL*/, string /*volumeMode*/, string /*subClient*/, *safeClient.SafeClient, error) {
	var (
		url, volumeMode string
		subClient       *safeClient.SafeClient
		decodedBytes    []byte
	)

	rtClient, err := sClient.NewRTClient(v1alpha1.AddToSchemeFor(backend.GroupVersion))
	if err != nil {
		return "", "", "", nil, err
	}

	// Fetch the current state so we can reconcile Spec.Publish before waiting.
	diObj := new(v1alpha1.DataImport)

	err = rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: diName}, diObj)
	if err != nil {
		return "", "", "", nil, fmt.Errorf("failed to get dataImport: %w", err)
	}

	// Patch Spec.Publish if the resolved value differs from what the object has.
	// Must happen before GetDataImportWithRestart so the loop waits for PublicURL
	// when publish=true.
	err = EnsureDataImportPublish(ctx, diObj, publish, rtClient)
	if err != nil {
		return "", "", "", nil, err
	}

	diObj, err = GetDataImportWithRestart(ctx, backend, diName, namespace, rtClient, log)
	if err != nil {
		return "", "", "", nil, err
	}

	// podURL is the importer base URL; the /api/v1/{files,block} data endpoint and the
	// /api/v1/finished finalise endpoint both hang off it, so it is returned to the caller
	// (which needs the base to POST finished after streaming the bytes).
	var podURL string

	switch {
	case publish:
		if diObj.Status.PublicURL == "" {
			return "", "", "", nil, fmt.Errorf("empty PublicURL")
		}

		podURL = diObj.Status.PublicURL
	case diObj.Status.URL != "":
		podURL = diObj.Status.URL
	default:
		return "", "", "", nil, fmt.Errorf("invalid URL")
	}

	volumeMode = diObj.Status.VolumeMode
	switch volumeMode {
	case "Filesystem":
		url, err = neturl.JoinPath(podURL, "api/v1/files")
		if err != nil {
			return "", "", "", nil, err
		}
	case "Block":
		url, err = neturl.JoinPath(podURL, "api/v1/block")
		if err != nil {
			return "", "", "", nil, err
		}
	default:
		return "", "", "", nil, fmt.Errorf("%w: '%s'", dataio.ErrUnsupportedVolumeMode, volumeMode)
	}

	if len(diObj.Status.CA) > 0 {
		decodedBytes, err = base64.StdEncoding.DecodeString(diObj.Status.CA)
		if err != nil {
			return "", "", "", nil, fmt.Errorf("CA decoding error: %s", err.Error())
		}
	}

	// Create an isolated copy to avoid mutating the original client
	subClient = sClient.Copy()
	// Always call SetTLSCAData to build a merged trust pool (system CAs + kubeconfig CA + internal CA if present)
	subClient.SetTLSCAData(decodedBytes)

	return url, podURL, volumeMode, subClient, nil
}

// PostFinished signals end-of-upload to the importer (POST <baseURL>/api/v1/finished), which
// flips the DataImport's serverState to Finished. This is mandatory: the last data chunk (a PUT
// that fills the device / writes the final file) does NOT finalise on its own, and the controller
// only sets UploadFinished=True — the gate for rebinding the target and reaching Completed — once
// serverState is Finished. baseURL is the importer base (status.url / status.publicURL), the same
// base the /api/v1/{files,block} data endpoint hangs off of.
func PostFinished(ctx context.Context, httpClient *safeClient.SafeClient, baseURL string) error {
	finishedURL, err := neturl.JoinPath(baseURL, uploadFinishedSubpath)
	if err != nil {
		return fmt.Errorf("build finished URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, finishedURL, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("finished returned status %d (%s)", resp.StatusCode, resp.Status)
	}

	return nil
}

// EnsureDataImportPublish patches DataImport.Spec.Publish to match the resolved value.
// Only upgrades publish: false -> true is patched, true -> false is intentionally skipped
// to avoid downgrading already-published resources.
func EnsureDataImportPublish(
	ctx context.Context,
	diObj *v1alpha1.DataImport,
	publish bool,
	rtClient ctrlrtclient.Client,
) error {
	if !publish {
		return nil
	}

	if diObj == nil {
		return fmt.Errorf("nil DataImport")
	}

	if diObj.Spec.Publish == publish {
		return nil
	}

	patch := ctrlrtclient.MergeFrom(diObj.DeepCopy())
	diObj.Spec.Publish = publish

	if err := rtClient.Patch(ctx, diObj, patch); err != nil {
		return fmt.Errorf("patch DataImport publish: %w", err)
	}

	return nil
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
