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

package adapters

import (
	"context"
	"io"
	"net/http"

	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// Compile-time check that SafeClientAdapter implements usecase.HTTPClient
var _ usecase.HTTPClient = (*SafeClientAdapter)(nil)

// SafeClientAdapter adapts SafeClient to usecase.HTTPClient interface
type SafeClientAdapter struct {
	client *safeClient.SafeClient
}

// NewSafeClientAdapter creates a new SafeClientAdapter
func NewSafeClientAdapter(client *safeClient.SafeClient) *SafeClientAdapter {
	return &SafeClientAdapter{client: client}
}

func (a *SafeClientAdapter) Get(ctx context.Context, url string) (io.ReadCloser, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := a.client.HTTPDo(req)
	if err != nil {
		return nil, 0, err
	}

	return resp.Body, resp.StatusCode, nil
}

func (a *SafeClientAdapter) Head(ctx context.Context, url string) (map[string]string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := a.client.HTTPDo(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	headers := make(map[string]string)
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	return headers, resp.StatusCode, nil
}

func (a *SafeClientAdapter) Put(ctx context.Context, url string, body io.Reader, headers map[string]string) (map[string]string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return nil, 0, err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := a.client.HTTPDo(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	respHeaders := make(map[string]string)
	for key, values := range resp.Header {
		if len(values) > 0 {
			respHeaders[key] = values[0]
		}
	}

	return respHeaders, resp.StatusCode, nil
}

func (a *SafeClientAdapter) SetCA(caData []byte) {
	a.client.SetTLSCAData(caData)
}

func (a *SafeClientAdapter) Copy() usecase.HTTPClient {
	return &SafeClientAdapter{client: a.client.Copy()}
}

