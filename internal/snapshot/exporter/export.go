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

package exporter

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// dataPlaneResponseHeaderTimeout bounds how long the data-plane client waits for
// a server to send response headers after the connection is accepted. The
// exporter is already Ready (WaitReady) before any fetch, so headers arrive in
// seconds; this conservative upper bound only guards the pathological case of an
// endpoint that accepts the connection but never answers. It complements the
// Fetcher's idle-read watchdog (which guards a stall AFTER headers arrive) and
// is applied ONLY to this exporter's own SafeClient copy, so no other
// libsaferequest consumer is affected.
const dataPlaneResponseHeaderTimeout = 2 * time.Minute

// Export holds a resolved DataExport: a ready HTTP endpoint (Fetcher), the
// VolumeMode reported by the controller, and the internal base URL. Callers
// release the underlying DataExport CR via ReleaseDataExport + DataExportName
// using the leaf name they already have, rather than through this value —
// releasing by deterministic name also covers the case where OpenExport never
// returned an Export at all (e.g. cancelled while still waiting for Ready).
type Export struct {
	deName     string
	namespace  string
	volumeMode string
	baseURL    string
	fetcher    *Fetcher
}

// VolumeMode returns the volume mode reported by the DataExport controller
// ("Block" or "Filesystem").
func (e *Export) VolumeMode() string {
	return e.volumeMode
}

// BaseURL returns the internal base URL of the data-exporter server.
func (e *Export) BaseURL() string {
	return e.baseURL
}

// Fetcher returns the HTTP Fetcher wired to the data-exporter endpoint.
func (e *Export) Fetcher() *Fetcher {
	return e.fetcher
}

// NewExport constructs an Export from pre-built components.
// Intended for testing and alternative transport implementations that bypass
// the production DataExport lifecycle.
func NewExport(namespace, deName, volumeMode, baseURL string, fetcher *Fetcher) *Export {
	return &Export{
		deName:     deName,
		namespace:  namespace,
		volumeMode: volumeMode,
		baseURL:    baseURL,
		fetcher:    fetcher,
	}
}

// OpenExport creates (or re-uses) a DataExport targeting the snapshot leaf
// identified by {group, kind, leafName}, waits until it is Ready, and
// returns an Export ready for data transfer.
//
// An isolated copy of sClient is built for the HTTP Fetcher so that CA
// injection does not mutate the caller's client.
func OpenExport(
	ctx context.Context,
	log *slog.Logger,
	c client.Client,
	namespace,
	group,
	resource,
	kind,
	leafName,
	ttl string,
	sc *safeClient.SafeClient,
) (*Export, error) {
	de, err := EnsureDataExport(ctx, c, namespace, group, resource, kind, leafName, ttl)
	if err != nil {
		return nil, fmt.Errorf("ensure DataExport for leaf %q: %w", leafName, err)
	}

	ready, err := WaitReady(ctx, c, log, namespace, de.Name)
	if err != nil {
		return nil, fmt.Errorf("wait DataExport %q ready: %w", de.Name, err)
	}

	sub, err := buildSubClient(sc, ready)
	if err != nil {
		return nil, fmt.Errorf("build sub-client for DataExport %q: %w", de.Name, err)
	}

	return &Export{
		deName:     de.Name,
		namespace:  namespace,
		volumeMode: ready.Status.VolumeMode,
		baseURL:    ready.Status.URL,
		fetcher:    NewFetcher(safeDoer{sub}),
	}, nil
}

// buildSubClient creates an isolated SafeClient copy and merges the DataExport's
// internal CA (base64-encoded PEM) into its trust pool.
func buildSubClient(sc *safeClient.SafeClient, de *deapi.DataExport) (*safeClient.SafeClient, error) {
	var caBytes []byte

	if de.Status.CA != "" {
		decoded, err := base64.StdEncoding.DecodeString(de.Status.CA)
		if err != nil {
			return nil, fmt.Errorf("decode CA from DataExport: %w", err)
		}

		caBytes = decoded
	}

	sub := sc.Copy()
	sub.SetTLSCAData(caBytes)
	// Apply the response-header timeout AFTER SetTLSCAData so it chains onto the
	// CA-injecting WrapTransport rather than replacing it (both must apply).
	sub.SetResponseHeaderTimeout(dataPlaneResponseHeaderTimeout)

	return sub, nil
}

// safeDoer adapts *safeClient.SafeClient to the Doer interface expected by Fetcher.
// SafeClient exposes HTTPDo rather than Do, so a thin wrapper is needed.
type safeDoer struct {
	c *safeClient.SafeClient
}

func (d safeDoer) Do(req *http.Request) (*http.Response, error) {
	return d.c.HTTPDo(req)
}
