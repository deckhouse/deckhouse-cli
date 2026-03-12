/*
Copyright 2026 Flant JSC

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

package dataio

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	kubeServiceNamespace  = "default"
	kubeServiceName       = "kubernetes"
	kubeServiceServerName = "kubernetes.default.svc"
	ProbeTimeout          = 3 * time.Second
)

var ErrAutoDetectWithHint = errors.New("cannot auto-detect publish mode, specify --publish=true or --publish=false")

// ResolvePublish returns explicit publish value if user set the flag,
// otherwise runs autodetection.
func ResolvePublish(
	ctx context.Context,
	publishFlag PublishFlag,
	rtClient ctrlrtclient.Client,
	sClient *safeClient.SafeClient,
	log *slog.Logger,
) (bool, error) {
	if log == nil {
		log = slog.Default()
	}

	if publishFlag.Explicit {
		// User set the flag, return value without autodetection.
		log.Info("Using explicit publish mode", slog.Bool("publish", publishFlag.Value))
		return publishFlag.Value, nil
	}

	// User didn't set the flag, run autodetection.
	log.Info("Auto-detecting publish mode")
	return DetectPublish(ctx, rtClient, sClient, log)
}

// DetectPublish decides default publish mode when user did not set --publish.
//
// Detection strategy:
//  1. Read Service default/kubernetes via the normal kubeconfig endpoint.
//  2. Read the same Service via https://<ClusterIP>:443 with ServerName override.
//  3. Compare UIDs of both objects.
//
// Decision matrix:
//   - same UID: internal path is reachable -> publish=false
//   - UID mismatch: ClusterIP reached a different cluster (e.g. local minikube/kind) -> publish=true
//   - network-unreachable on probe: internal path is not reachable -> publish=true
//   - TLS/auth rejection on probe: ClusterIP reached a different server -> publish=true
//   - any other probe error (transient 5xx, cancellation, deserialization): ambiguous -> fail fast with hint
func DetectPublish(
	ctx context.Context,
	rtClient ctrlrtclient.Client,
	sClient *safeClient.SafeClient,
	log *slog.Logger,
) (bool, error) {
	if log == nil {
		log = slog.Default()
	}

	firstSvc, err := getKubeService(ctx, rtClient)
	if err != nil {
		return false, ErrAutoDetectWithHint
	}

	targetURL := "https://" + net.JoinHostPort(firstSvc.Spec.ClusterIP, "443")

	// Clone the original client to avoid mutating command-wide kubeconfig settings.
	// Keep auth/CA from kubeconfig, but switch endpoint to ClusterIP and set ServerName
	// so TLS validation uses service DNS name instead of raw IP.
	probeClient := sClient.Copy()
	// Timeout in restConfig.Timeout is required in addition to context.WithTimeout below:
	// context limits Go-level read/write, but restConfig.Timeout sets http.Client.Timeout
	// which also covers TLS handshake and DNS resolve. Without it the HTTP client inherits
	// the default kubeconfig timeout (typically 30s).
	probeClient.SetProbeEndpoint(ProbeTimeout, targetURL, kubeServiceServerName)
	probeRtClient, err := probeClient.NewRTClient()

	if err != nil {
		return false, ErrAutoDetectWithHint
	}

	// Probe timeout limits only autodetect latency
	// main command context stays unchanged.
	probeCtx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	secondSvc, err := getKubeService(probeCtx, probeRtClient)
	if err != nil {
		// Network-level failure means in-cluster endpoint is not reachable
		// from current environment.
		if isNetworkUnreachable(err) {
			log.Info("Publish autodetect: internal endpoint is unreachable, selecting publish=true")
			return true, nil
		}
		// TLS/auth/RBAC rejection: the first request via kubeconfig succeeded
		// with the same credentials, so a rejection here means ClusterIP
		// reached a different server.
		if isProbeRejected(err) {
			log.Info("Publish autodetect: internal endpoint rejected, selecting publish=true")
			return true, nil
		}
		// Remaining errors are ambiguous - the probe endpoint may be the same
		// server experiencing a transient issue:
		//   - context.Canceled: deliberate cancellation, not a detection result
		//   - apierrors 500 InternalError: transient API server failure
		//   - apierrors 503 ServiceUnavailable / ServerTimeout: server overloaded
		//   - apierrors 504 Timeout: API-level processing timeout
		//   - apierrors 429 TooManyRequests: rate limiting
		//   - apierrors 400 BadRequest, 404 NotFound: unexpected but not clearly "different server"
		//   - response deserialization errors (malformed JSON, unexpected content type)
		return false, ErrAutoDetectWithHint
	}

	// UID mismatch: both endpoints responded but belong to different clusters.
	// Typical case: a local cluster (minikube, kind) has the same ClusterIP
	// as the remote target cluster. The user is not inside the target cluster.
	if firstSvc.UID != secondSvc.UID {
		log.Info("Publish autodetect: UID mismatch between external and internal endpoints, selecting publish=true")
		return true, nil
	}

	// Same service identity via both paths -> internal endpoint is reachable.
	log.Info("Publish autodetect: internal endpoint is reachable, selecting publish=false")
	return false, nil
}

// isNetworkUnreachable classifies transport-level failures that indicate
// the in-cluster endpoint is not reachable from the current environment.
//
// Returns true for errors that clearly mean "no network path to ClusterIP":
//   - context.DeadlineExceeded: probe timed out waiting for any response
//   - net.OpError: low-level socket failures (EHOSTUNREACH, ENETUNREACH,
//     ECONNREFUSED, ETIMEDOUT, etc.) - all indicate the ClusterIP is not
//     routable from here.
//   - net.DNSError: DNS resolution failed for the target host
//   - net.Error with Timeout(): any other network-level timeout
//
// Returns false for:
//   - nil: no error
//   - context.Canceled: deliberate cancellation, not a network issue
//   - everything else (TLS, RBAC, HTTP-level errors): the endpoint is
//     reachable but rejected the request - ambiguous for autodetect
func isNetworkUnreachable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	return false
}

// isProbeRejected classifies errors that indicate the ClusterIP endpoint
// is reachable but belongs to a different server than the kubeconfig endpoint.
//
// Precondition: the first request via kubeconfig already succeeded with the
// same CA, token, and RBAC permissions. If the probe to ClusterIP gets a TLS
// or auth rejection, it means ClusterIP reached a different server.
//
// Returns true for:
//   - x509.UnknownAuthorityError: server certificate signed by a different CA
//   - x509.CertificateInvalidError: server certificate expired, not yet valid,
//     constraint violation, incompatible key usage, etc.
//     (also covers errors wrapped in tls.CertificateVerificationError, which
//     has Unwrap() so errors.As finds the inner x509 error automatically)
//   - x509.HostnameError: server certificate CN/SAN doesn't match
//     kubernetes.default.svc
//   - tls.RecordHeaderError: server doesn't speak TLS at all
//     (plain HTTP on HTTPS port, or a non-HTTP service)
//   - tls.AlertError: server sent a TLS alert rejecting the handshake
//     (bad_certificate, handshake_failure, protocol_version, unknown_ca, etc.)
//   - apierrors 401 Unauthorized: server doesn't accept our token
//   - apierrors 403 Forbidden: server has different RBAC rules
//
// Returns false for:
//   - nil: no error
//   - everything else: ambiguous, handled by the caller as ErrAutoDetectWithHint
func isProbeRejected(err error) bool {
	if err == nil {
		return false
	}

	// TLS: certificate signed by unknown authority
	var unknownAuthErr x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthErr) {
		return true
	}

	// TLS: certificate expired, not yet valid, etc.
	var certInvalidErr x509.CertificateInvalidError
	if errors.As(err, &certInvalidErr) {
		return true
	}

	// TLS: certificate CN/SAN doesn't match server name
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return true
	}

	// TLS: server doesn't speak TLS (plain HTTP on HTTPS port)
	var recordHeaderErr tls.RecordHeaderError
	if errors.As(err, &recordHeaderErr) {
		return true
	}

	// TLS: server reject handshake
	var tlsAlertErr tls.AlertError
	if errors.As(err, &tlsAlertErr) {
		return true
	}

	// Auth/RBAC: 401 or 403 from a different API server
	if apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err) {
		return true
	}

	return false
}

func getKubeService(ctx context.Context, rtClient ctrlrtclient.Client) (*corev1.Service, error) {
	var svc corev1.Service
	if err := rtClient.Get(ctx, types.NamespacedName{
		Name:      kubeServiceName,
		Namespace: kubeServiceNamespace,
	}, &svc); err != nil {
		return nil, err
	}

	return &svc, nil
}
