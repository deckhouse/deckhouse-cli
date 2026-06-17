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

	"sigs.k8s.io/controller-runtime/pkg/client"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// Export holds a resolved DataExport: a ready HTTP endpoint (Fetcher), the
// VolumeMode reported by the controller, and the internal base URL.
// Callers must call Release when the transfer is complete or aborted.
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

// Release deletes the DataExport CR. It is idempotent and safe to call on
// error paths to ensure the cluster resource is cleaned up.
func (e *Export) Release(ctx context.Context, c client.Client) error {
	return ReleaseDataExport(ctx, c, e.namespace, e.deName)
}

// OpenExport creates (or re-uses) a DataExport targeting shadowVSName, waits
// until it is Ready, and returns an Export ready for data transfer.
//
// An isolated copy of sClient is built for the HTTP Fetcher so that CA
// injection does not mutate the caller's client.
func OpenExport(
	ctx context.Context,
	log *slog.Logger,
	c client.Client,
	namespace,
	shadowVSName,
	ttl string,
	sc *safeClient.SafeClient,
) (*Export, error) {
	de, err := EnsureDataExport(ctx, c, namespace, shadowVSName, ttl)
	if err != nil {
		return nil, fmt.Errorf("ensure DataExport for shadow VS %q: %w", shadowVSName, err)
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
