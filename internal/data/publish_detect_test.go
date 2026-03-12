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
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// timeoutError implements net.Error with Timeout()=true but is neither
// net.OpError nor net.DNSError, so it exercises the final net.Error branch.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return false }

// nonTimeoutNetError implements net.Error with Timeout()=false.
type nonTimeoutNetError struct{}

func (e *nonTimeoutNetError) Error() string   { return "net error" }
func (e *nonTimeoutNetError) Timeout() bool   { return false }
func (e *nonTimeoutNetError) Temporary() bool { return false }

func TestIsNetworkUnreachable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "deadline exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "wrapped deadline exceeded",
			err:  fmt.Errorf("get service: %w", context.DeadlineExceeded),
			want: true,
		},
		{
			name: "context canceled",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "wrapped context canceled",
			err:  fmt.Errorf("probe: %w", context.Canceled),
			want: false,
		},
		{
			name: "net.OpError dial",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			want: true,
		},
		{
			name: "net.OpError wrapped",
			err:  fmt.Errorf("probe: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("no route to host")}),
			want: true,
		},
		{
			name: "net.DNSError",
			err:  &net.DNSError{Err: "no such host", Name: "kubernetes.default.svc"},
			want: true,
		},
		{
			name: "net.DNSError wrapped",
			err:  fmt.Errorf("resolve: %w", &net.DNSError{Err: "server misbehaving", Name: "example.com"}),
			want: true,
		},
		{
			name: "net.Error with timeout",
			err:  &timeoutError{},
			want: true,
		},
		{
			name: "net.Error without timeout",
			err:  &nonTimeoutNetError{},
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("something went wrong"),
			want: false,
		},
		{
			name: "wrapped generic error",
			err:  fmt.Errorf("outer: %w", errors.New("inner")),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNetworkUnreachable(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolvePublish_Explicit(t *testing.T) {
	ctx := context.Background()
	log := slog.Default()

	tests := []struct {
		name string
		flag PublishFlag
		want bool
	}{
		{
			name: "explicit true",
			flag: PublishFlag{Explicit: true, Value: true},
			want: true,
		},
		{
			name: "explicit false",
			flag: PublishFlag{Explicit: true, Value: false},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// rtClient and sClient are nil — explicit path returns early.
			got, err := ResolvePublish(ctx, tt.flag, nil, nil, log)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolvePublish_NilLogger(t *testing.T) {
	ctx := context.Background()

	got, err := ResolvePublish(ctx, PublishFlag{Explicit: true, Value: true}, nil, nil, nil)
	require.NoError(t, err)
	assert.True(t, got)
}

func TestGetKubeService(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, kubescheme.AddToScheme(scheme))

	ctx := context.Background()

	t.Run("service exists", func(t *testing.T) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      kubeServiceName,
				Namespace: kubeServiceNamespace,
				UID:       types.UID("test-uid-123"),
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "10.96.0.1",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()

		got, err := getKubeService(ctx, c)
		require.NoError(t, err)
		assert.Equal(t, types.UID("test-uid-123"), got.UID)
		assert.Equal(t, "10.96.0.1", got.Spec.ClusterIP)
	})

	t.Run("service not found", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		_, err := getKubeService(ctx, c)
		require.Error(t, err)
	})
}

func TestDetectPublish_ServiceNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, kubescheme.AddToScheme(scheme))

	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	log := slog.Default()

	_, err := DetectPublish(context.Background(), c, nil, log)
	require.ErrorIs(t, err, ErrAutoDetectWithHint)
}

// newFakeSafeClient writes a minimal kubeconfig to a temp file and constructs
// a SafeClient from it, bypassing the real ~/.kube/config on disk.
func newFakeSafeClient(t *testing.T) *safeClient.SafeClient {
	t.Helper()
	const kubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://fake-server:6443
    insecure-skip-tls-verify: true
  name: fake
contexts:
- context:
    cluster: fake
    user: fake
  name: fake
current-context: fake
users:
- name: fake
  user:
    token: fake-token
`
	f, err := os.CreateTemp(t.TempDir(), "kubeconfig-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(kubeconfig)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Setenv("KUBECONFIG", f.Name())

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	c, err := safeClient.NewSafeClient(fs)
	require.NoError(t, err)
	return c
}

// TestDetectPublish_ProbeConnRefused tests the full DetectPublish pipeline:
// first service is found, probe client is built from a fake SafeClient,
// probe to 127.0.0.1:443 gets connection refused -> isNetworkUnreachable -> publish=true.
func TestDetectPublish_ProbeConnRefused(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, kubescheme.AddToScheme(scheme))

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeServiceName,
			Namespace: kubeServiceNamespace,
			UID:       types.UID("uid-abc"),
		},
		Spec: corev1.ServiceSpec{
			// Probe will dial https://127.0.0.1:443 - nothing listens there,
			// so we get connection refused immediately.
			ClusterIP: "127.0.0.1",
		},
	}
	rtClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()
	sClient := newFakeSafeClient(t)

	got, err := DetectPublish(context.Background(), rtClient, sClient, slog.Default())
	require.NoError(t, err)
	// connection refused → isNetworkUnreachable → publish=true
	assert.True(t, got)
}

func TestIsProbeRejected(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "x509.UnknownAuthorityError",
			err:  x509.UnknownAuthorityError{},
			want: true,
		},
		{
			name: "x509.CertificateInvalidError expired",
			err:  x509.CertificateInvalidError{Cert: &x509.Certificate{}, Reason: x509.Expired},
			want: true,
		},
		{
			name: "x509.HostnameError",
			err:  x509.HostnameError{Host: "10.96.0.1"},
			want: true,
		},
		{
			name: "tls.RecordHeaderError",
			err:  tls.RecordHeaderError{Msg: "not a TLS packet"},
			want: true,
		},
		{
			name: "tls.AlertError handshake_failure",
			err:  tls.AlertError(0x28),
			want: true,
		},
		{
			name: "wrapped x509.UnknownAuthorityError",
			err:  fmt.Errorf("tls verify: %w", x509.UnknownAuthorityError{}),
			want: true,
		},
		{
			name: "connection refused",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "context.Canceled",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "500 Internal Server Error",
			err:  fmt.Errorf("500 Internal Server Error"),
			want: false,
		},
		{
			name: "503 Service Unavailable",
			err:  fmt.Errorf("503 Service Unavailable"),
			want: false,
		},
		{
			name: "net.OpError dial (not TLS)",
			err:  &net.OpError{Op: "dial"},
			want: false,
		},
		{
			name: "apierrors 401 Unauthorized",
			err:  apierrors.NewUnauthorized("token not accepted"),
			want: true,
		},
		{
			name: "apierrors 403 Forbidden",
			err:  apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "kubernetes", errors.New("access denied")),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isProbeRejected(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParsePublishFlag(t *testing.T) {
	tests := []struct {
		name         string
		setupFlags   func() *pflag.FlagSet
		wantExplicit bool
		wantValue    bool
		wantErr      bool
	}{
		{
			name:       "nil FlagSet",
			setupFlags: func() *pflag.FlagSet { return nil },
			wantErr:    true,
		},
		{
			name: "FlagSet without publish registered",
			setupFlags: func() *pflag.FlagSet {
				return pflag.NewFlagSet("test", pflag.ContinueOnError)
			},
			wantErr: true,
		},
		{
			name: "publish registered but not changed",
			setupFlags: func() *pflag.FlagSet {
				fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
				fs.Bool("publish", false, "")
				return fs
			},
			wantExplicit: false,
			wantValue:    false,
		},
		{
			name: "--publish=true changed",
			setupFlags: func() *pflag.FlagSet {
				fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
				fs.Bool("publish", false, "")
				require.NoError(t, fs.Parse([]string{"--publish=true"}))
				return fs
			},
			wantExplicit: true,
			wantValue:    true,
		},
		{
			name: "--publish=false changed",
			setupFlags: func() *pflag.FlagSet {
				fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
				fs.Bool("publish", false, "")
				require.NoError(t, fs.Parse([]string{"--publish=false"}))
				return fs
			},
			wantExplicit: true,
			wantValue:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := tt.setupFlags()
			got, err := ParsePublishFlag(fs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantExplicit, got.Explicit)
			assert.Equal(t, tt.wantValue, got.Value)
		})
	}
}
