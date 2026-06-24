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
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

// TestNewForwardsKubeconfigIdentityAndTrustsCA verifies the end-to-end transport:
// the bearer token from the rest.Config reaches the proxy over TLS that is
// verified against the explicitly supplied CA (the test server's own cert).
func TestNewForwardsKubeconfigIdentityAndTrustsCA(t *testing.T) {
	const token = "kubeconfig-bearer-token"

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer "+token, r.Header.Get("Authorization"))

		_, _ = io.WriteString(w, `{"name":"deckhouse-cli","tags":["v0.13.1"]}`)
	}))
	t.Cleanup(srv.Close)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	restConfig := &rest.Config{BearerToken: token}

	client, err := New(srv.URL, restConfig, dkplog.NewNop(), WithCAData(caPEM))
	require.NoError(t, err)

	tags, err := client.ListTags(context.Background(), CLIImage())
	require.NoError(t, err)
	assert.Equal(t, []string{"v0.13.1"}, tags)
}

// TestNewInsecureSkipsVerification verifies that WithInsecureSkipTLSVerify lets
// the client reach a TLS server whose certificate is not otherwise trusted.
func TestNewInsecureSkipsVerification(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"name":"deckhouse-cli","tags":["v0.13.1"]}`)
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, &rest.Config{}, dkplog.NewNop(), WithInsecureSkipTLSVerify())
	require.NoError(t, err)

	tags, err := client.ListTags(context.Background(), CLIImage())
	require.NoError(t, err)
	assert.Equal(t, []string{"v0.13.1"}, tags)
}

// TestNewRejectsUntrustedCA confirms that without a matching CA (and without
// insecure) the proxy certificate is rejected.
func TestNewRejectsUntrustedCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, &rest.Config{}, dkplog.NewNop())
	require.NoError(t, err)

	_, err = client.ListTags(context.Background(), CLIImage())
	require.Error(t, err)
}
