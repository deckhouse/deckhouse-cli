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

package rpp

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

func TestNewValidatesEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr error
	}{
		{name: "valid https", baseURL: "https://master:4219", wantErr: nil},
		{name: "empty", baseURL: "", wantErr: ErrInvalidEndpoint},
		{name: "missing scheme", baseURL: "master:4219", wantErr: ErrInvalidEndpoint},
		{name: "plain http", baseURL: "http://master:4219", wantErr: ErrInvalidEndpoint},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.baseURL, &rest.Config{}, dkplog.NewNop())
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestNewRejectsContradictoryTLSOptions(t *testing.T) {
	const endpoint = "https://master:4219"

	t.Run("insecure with CA data", func(t *testing.T) {
		_, err := New(endpoint, &rest.Config{}, dkplog.NewNop(),
			WithInsecureSkipTLSVerify(), WithCAData([]byte("pem")))
		require.ErrorIs(t, err, ErrUnsupportedConfig)
	})

	t.Run("CA file and CA data together", func(t *testing.T) {
		_, err := New(endpoint, &rest.Config{}, dkplog.NewNop(),
			WithCAFile("/tmp/ca.pem"), WithCAData([]byte("pem")))
		require.ErrorIs(t, err, ErrUnsupportedConfig)
	})
}

func TestNewRejectsUnparseableCAData(t *testing.T) {
	_, err := New("https://master:4219", &rest.Config{}, dkplog.NewNop(),
		WithCAData([]byte("not a pem certificate")))
	require.ErrorIs(t, err, ErrInvalidCA)
}

func TestNewRejectsCustomTransport(t *testing.T) {
	cfg := &rest.Config{Transport: http.DefaultTransport}

	_, err := New("https://master:4219", cfg, dkplog.NewNop(), WithCAData([]byte("pem")))
	require.ErrorIs(t, err, ErrUnsupportedConfig)
}
