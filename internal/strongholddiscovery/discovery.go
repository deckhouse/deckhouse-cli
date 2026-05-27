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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
)

const (
	Namespace            = "d8-stronghold"
	IngressName          = "stronghold"
	KeysSecretName       = "stronghold-keys"
	IngressTLSSecret     = "ingress-tls"
	IngressTLSCertKey    = "tls.crt"
	RootTokenKey         = "rootToken"
	AddrEnv              = "STRONGHOLD_ADDR"
	TokenEnv             = "STRONGHOLD_TOKEN"
	CABytesEnv           = "STRONGHOLD_CACERT_BYTES"
	VaultAddrEnv         = "VAULT_ADDR"
	VaultTokenEnv        = "VAULT_TOKEN"
	VaultCABytesEnv      = "VAULT_CACERT_BYTES"
	VaultCAEnv           = "VAULT_CACERT"
	VaultCAPathEnv       = "VAULT_CAPATH"
	VaultSkipVerifyEnv   = "VAULT_SKIP_VERIFY"
	StrongholdCAEnv      = "STRONGHOLD_CACERT"
	StrongholdCAPathEnv  = "STRONGHOLD_CAPATH"
	StrongholdSkipVerify = "STRONGHOLD_SKIP_VERIFY"
	VaultTokenFileName   = ".vault-token"
)

func addrConfigured() bool {
	return os.Getenv(VaultAddrEnv) != "" || os.Getenv(AddrEnv) != ""
}

func tokenConfigured() bool {
	if os.Getenv(VaultTokenEnv) != "" || os.Getenv(TokenEnv) != "" {
		return true
	}

	return vaultTokenFileExists()
}

func caConfigured() bool {
	return os.Getenv(VaultCABytesEnv) != "" ||
		os.Getenv(VaultCAEnv) != "" ||
		os.Getenv(VaultCAPathEnv) != "" ||
		os.Getenv(VaultSkipVerifyEnv) != "" ||
		os.Getenv(CABytesEnv) != "" ||
		os.Getenv(StrongholdCAEnv) != "" ||
		os.Getenv(StrongholdCAPathEnv) != "" ||
		os.Getenv(StrongholdSkipVerify) != ""
}

func vaultTokenFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	return filepath.Join(home, VaultTokenFileName)
}

func vaultTokenFileExists() bool {
	path := vaultTokenFilePath()
	if path == "" {
		return false
	}

	_, err := os.Stat(path)

	return err == nil
}

// ApplyFromCluster sets STRONGHOLD_ADDR, STRONGHOLD_TOKEN and STRONGHOLD_CACERT_BYTES
// from the cluster when they are not already configured. Failures are ignored silently.
func ApplyFromCluster() {
	needAddr := !addrConfigured()
	needToken := !tokenConfigured()

	needCA := !caConfigured()
	if !needAddr && !needToken && !needCA {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kubeCl, err := kubeClient()
	if err != nil {
		return
	}

	if needAddr {
		if addr, err := discoverAddr(ctx, kubeCl); err == nil {
			_ = os.Setenv(AddrEnv, addr)
		}
	}

	if needToken {
		if token, err := discoverToken(ctx, kubeCl); err == nil {
			_ = os.Setenv(TokenEnv, token)
		}
	}

	if needCA {
		if ca, err := discoverCA(ctx, kubeCl); err == nil {
			_ = os.Setenv(CABytesEnv, ca)
		}
	}
}

func kubeClient() (kubernetes.Interface, error) {
	configFlags := genericclioptions.NewConfigFlags(true)

	restConfig, err := configFlags.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(restConfig)
}

func discoverAddr(ctx context.Context, kubeCl kubernetes.Interface) (string, error) {
	ingress, err := kubeCl.NetworkingV1().Ingresses(Namespace).Get(
		ctx,
		IngressName,
		metav1.GetOptions{},
	)
	if err != nil {
		return "", err
	}

	addr, ok := AddrFromIngress(ingress)
	if !ok {
		return "", fmt.Errorf("ingress %s/%s has no host", Namespace, IngressName)
	}

	return addr, nil
}

func discoverToken(ctx context.Context, kubeCl kubernetes.Interface) (string, error) {
	secret, err := kubeCl.CoreV1().Secrets(Namespace).Get(
		ctx,
		KeysSecretName,
		metav1.GetOptions{},
	)
	if err != nil {
		return "", err
	}

	return TokenFromSecret(secret)
}

func discoverCA(ctx context.Context, kubeCl kubernetes.Interface) (string, error) {
	secret, err := kubeCl.CoreV1().Secrets(Namespace).Get(
		ctx,
		IngressTLSSecret,
		metav1.GetOptions{},
	)
	if err != nil {
		return "", err
	}

	return CAFromIngressTLSSecret(secret)
}

// CAFromIngressTLSSecret extracts the ingress TLS certificate from secret data.
// Kubernetes stores tls.crt base64-encoded; client-go returns decoded PEM bytes.
func CAFromIngressTLSSecret(secret *corev1.Secret) (string, error) {
	if secret == nil {
		return "", fmt.Errorf("secret is nil")
	}

	certBytes, ok := secret.Data[IngressTLSCertKey]
	if !ok || len(certBytes) == 0 {
		return "", fmt.Errorf("secret %s/%s has no %s", Namespace, IngressTLSSecret, IngressTLSCertKey)
	}

	cert := string(certBytes)
	if cert == "" {
		return "", fmt.Errorf("secret %s/%s has empty %s", Namespace, IngressTLSSecret, IngressTLSCertKey)
	}

	return cert, nil
}

// TokenFromSecret extracts the root token from the stronghold-keys secret.
func TokenFromSecret(secret *corev1.Secret) (string, error) {
	if secret == nil {
		return "", fmt.Errorf("secret is nil")
	}

	tokenBytes, ok := secret.Data[RootTokenKey]
	if !ok || len(tokenBytes) == 0 {
		return "", fmt.Errorf("secret %s/%s has no %s", Namespace, KeysSecretName, RootTokenKey)
	}

	token := string(tokenBytes)
	if token == "" {
		return "", fmt.Errorf("secret %s/%s has empty %s", Namespace, KeysSecretName, RootTokenKey)
	}

	return token, nil
}

// AddrFromIngress builds a Stronghold API URL from an Ingress resource.
func AddrFromIngress(ingress *networkingv1.Ingress) (string, bool) {
	if ingress == nil {
		return "", false
	}

	host := ingressHost(ingress)
	if host == "" {
		return "", false
	}

	scheme := "http"
	if len(ingress.Spec.TLS) > 0 {
		scheme = "https"
	}

	return fmt.Sprintf("%s://%s", scheme, host), true
}

func ingressHost(ingress *networkingv1.Ingress) string {
	for _, rule := range ingress.Spec.Rules {
		if rule.Host != "" {
			return rule.Host
		}
	}

	return ""
}
