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

package strongholddiscovery

import (
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAddrConfigured(t *testing.T) {
	tests := []struct {
		name      string
		vaultAddr string
		shAddr    string
		want      bool
	}{
		{name: "none", want: false},
		{name: "vault only", vaultAddr: "https://vault.example.com", want: true},
		{name: "stronghold only", shAddr: "https://stronghold.example.com", want: true},
		{
			name:      "both",
			vaultAddr: "https://vault.example.com",
			shAddr:    "https://stronghold.example.com",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(VaultAddrEnv, tt.vaultAddr)
			t.Setenv(AddrEnv, tt.shAddr)

			if got := addrConfigured(); got != tt.want {
				t.Fatalf("addrConfigured() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStrongholdAddrFromIngress(t *testing.T) {
	tests := []struct {
		name    string
		ingress *networkingv1.Ingress
		want    string
		ok      bool
	}{
		{
			name:    "nil ingress",
			ingress: nil,
			ok:      false,
		},
		{
			name: "no rules",
			ingress: &networkingv1.Ingress{
				ObjectMeta: metav1.ObjectMeta{Name: IngressName},
			},
			ok: false,
		},
		{
			name: "http host",
			ingress: &networkingv1.Ingress{
				Spec: networkingv1.IngressSpec{
					Rules: []networkingv1.IngressRule{
						{Host: "stronghold.example.com"},
					},
				},
			},
			want: "http://stronghold.example.com",
			ok:   true,
		},
		{
			name: "https with tls",
			ingress: &networkingv1.Ingress{
				Spec: networkingv1.IngressSpec{
					Rules: []networkingv1.IngressRule{
						{Host: "stronghold.stronghold-demo.test.dev"},
					},
					TLS: []networkingv1.IngressTLS{
						{Hosts: []string{"stronghold.stronghold-demo.test.dev"}},
					},
				},
			},
			want: "https://stronghold.stronghold-demo.test.dev",
			ok:   true,
		},
		{
			name: "first non-empty host",
			ingress: &networkingv1.Ingress{
				Spec: networkingv1.IngressSpec{
					Rules: []networkingv1.IngressRule{
						{Host: ""},
						{Host: "stronghold.example.com"},
					},
					TLS: []networkingv1.IngressTLS{{}},
				},
			},
			want: "https://stronghold.example.com",
			ok:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := AddrFromIngress(tt.ingress)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("addr = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTokenConfigured(t *testing.T) {
	tests := []struct {
		name       string
		vaultToken string
		shToken    string
		tokenFile  bool
		want       bool
	}{
		{name: "none", want: false},
		{name: "vault token env", vaultToken: "hvs.vault", want: true},
		{name: "stronghold token env", shToken: "hvs.stronghold", want: true},
		{name: "vault token file", tokenFile: true, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(VaultTokenEnv, tt.vaultToken)
			t.Setenv(TokenEnv, tt.shToken)

			if tt.tokenFile {
				dir := t.TempDir()
				t.Setenv("HOME", dir)
				if err := os.WriteFile(filepath.Join(dir, VaultTokenFileName), []byte("token"), 0o600); err != nil {
					t.Fatalf("write vault token file: %v", err)
				}
			}

			if got := tokenConfigured(); got != tt.want {
				t.Fatalf("tokenConfigured() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTokenFromSecret(t *testing.T) {
	tests := []struct {
		name    string
		secret  *corev1.Secret
		want    string
		wantErr bool
	}{
		{
			name:    "nil secret",
			secret:  nil,
			wantErr: true,
		},
		{
			name: "missing rootToken",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: KeysSecretName},
				Data:       map[string][]byte{},
			},
			wantErr: true,
		},
		{
			name: "root token",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: KeysSecretName},
				Data: map[string][]byte{
					RootTokenKey: []byte("s.23dKWH3vTnJVT7xsxxNuBdnN"),
				},
			},
			want: "s.23dKWH3vTnJVT7xsxxNuBdnN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TokenFromSecret(tt.secret)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("token = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCAConfigured(t *testing.T) {
	envs := []string{
		VaultCABytesEnv,
		VaultCAEnv,
		VaultCAPathEnv,
		VaultSkipVerifyEnv,
		CABytesEnv,
		StrongholdCAEnv,
		StrongholdCAPathEnv,
		StrongholdSkipVerify,
	}

	for _, env := range envs {
		t.Run(env, func(t *testing.T) {
			for _, other := range envs {
				t.Setenv(other, "")
			}
			t.Setenv(env, "configured")

			if got := caConfigured(); !got {
				t.Fatalf("caConfigured() = false, want true when %s is set", env)
			}
		})
	}

	t.Run("none", func(t *testing.T) {
		for _, env := range envs {
			t.Setenv(env, "")
		}
		if got := caConfigured(); got {
			t.Fatalf("caConfigured() = true, want false")
		}
	})
}

func TestCAFromIngressTLSSecret(t *testing.T) {
	pemCert := "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"

	tests := []struct {
		name    string
		secret  *corev1.Secret
		want    string
		wantErr bool
	}{
		{
			name:    "nil secret",
			secret:  nil,
			wantErr: true,
		},
		{
			name: "missing tls.crt",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: IngressTLSSecret},
				Data:       map[string][]byte{},
			},
			wantErr: true,
		},
		{
			name: "tls.crt",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: IngressTLSSecret},
				Data: map[string][]byte{
					IngressTLSCertKey: []byte(pemCert),
				},
			},
			want: pemCert,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CAFromIngressTLSSecret(tt.secret)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("cert = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestApplyFromCluster_SkipsWhenConfigured(t *testing.T) {
	t.Setenv(AddrEnv, "https://configured.example.com")
	t.Setenv(TokenEnv, "hvs.configured")
	t.Setenv(CABytesEnv, "-----BEGIN CERTIFICATE-----\nconfigured\n-----END CERTIFICATE-----\n")

	ApplyFromCluster()

	if got := os.Getenv(AddrEnv); got != "https://configured.example.com" {
		t.Fatalf("STRONGHOLD_ADDR = %q, want unchanged configured value", got)
	}
	if got := os.Getenv(TokenEnv); got != "hvs.configured" {
		t.Fatalf("STRONGHOLD_TOKEN = %q, want unchanged configured value", got)
	}
	if got := os.Getenv(CABytesEnv); got != "-----BEGIN CERTIFICATE-----\nconfigured\n-----END CERTIFICATE-----\n" {
		t.Fatalf("STRONGHOLD_CACERT_BYTES = %q, want unchanged configured value", got)
	}
}
