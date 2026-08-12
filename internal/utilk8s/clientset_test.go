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

package utilk8s

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// selfSignedCAPEM builds a CA that client-go can actually parse: an unparsable
// blob fails at clientset construction and never reaches the trust settings.
func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// writeKubeconfig writes a kubeconfig that pins a CA, so a plain read must
// produce a verifying config.
func writeKubeconfig(t *testing.T) string {
	t.Helper()

	body := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://api.example.test
    certificate-authority-data: %s
contexts:
- name: c
  context: {cluster: c, user: u}
current-context: c
users:
- name: u
  user: {token: t}
`, base64.StdEncoding.EncodeToString(selfSignedCAPEM(t)))

	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	return path
}

func TestSetupK8sClientSetVerifiesByDefault(t *testing.T) {
	restConfig, _, err := SetupK8sClientSet(writeKubeconfig(t), DefaultKubeContext)
	require.NoError(t, err)

	require.False(t, restConfig.Insecure)
	require.NotEmpty(t, restConfig.CAData)
	require.Equal(t, "t", restConfig.BearerToken)
}

func TestSetupK8sClientSetInsecureDropsCA(t *testing.T) {
	restConfig, _, err := SetupK8sClientSet(writeKubeconfig(t), DefaultKubeContext,
		WithInsecureSkipTLSVerify(true))
	require.NoError(t, err)

	// A CA and skipped verification cannot both apply: client-go rejects the
	// combination, so the option has to clear the kubeconfig CA.
	require.True(t, restConfig.Insecure)
	require.Empty(t, restConfig.CAData)
	require.Empty(t, restConfig.CAFile)

	// The identity survives: only server trust changes.
	require.Equal(t, "t", restConfig.BearerToken)
}

func TestSetupK8sClientSetInsecureFalseKeepsCA(t *testing.T) {
	restConfig, _, err := SetupK8sClientSet(writeKubeconfig(t), DefaultKubeContext,
		WithInsecureSkipTLSVerify(false))
	require.NoError(t, err)

	require.False(t, restConfig.Insecure)
	require.NotEmpty(t, restConfig.CAData)
}
