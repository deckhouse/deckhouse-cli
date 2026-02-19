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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
