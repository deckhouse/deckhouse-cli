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

package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
)

// newConfigTestKubeconfigFlags builds a --kubeconfig flag pointing at a
// throwaway kubeconfig fixture, mirroring the exporter package's test helper,
// so transport.NewClient never falls back to $HOME/.kube/config.
func newConfigTestKubeconfigFlags(t *testing.T) *pflag.FlagSet {
	t.Helper()

	kubeconfigPath := filepath.Join(t.TempDir(), "config")
	kubeconfig := []byte(`apiVersion: v1
kind: Config
clusters:
- name: default
  cluster:
    server: https://test.invalid
contexts:
- name: default
  context:
    cluster: default
current-context: default
`)
	require.NoError(t, os.WriteFile(kubeconfigPath, kubeconfig, 0o600))

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("kubeconfig", "", "")
	require.NoError(t, flags.Set("kubeconfig", kubeconfigPath))

	return flags
}

// TestApplyDefaults_ForwardsPublishToOpenExport verifies that Config.Publish is
// forwarded, unmodified, all the way through applyDefaults' generated
// OpenExportWithTargetAcquisition closure into exporter.WithPublish, by
// observing the OBSERVABLE effect: the DataExport EnsureDataExport creates
// carries spec.publish equal to Config.Publish. WaitReady is expected to time
// out (the fake DataExport never gets a Ready condition, since no controller is
// running against the fake client) — only the CREATED object's spec.publish is
// asserted, proving the option reached exporter.EnsureDataExport without needing
// a full working data-plane transport.
func TestApplyDefaults_ForwardsPublishToOpenExport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		publish bool
	}{
		{name: "success: publish=true is forwarded to the created DataExport", publish: true},
		{name: "success: publish=false is forwarded to the created DataExport", publish: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const (
				namespace = "forward-ns"
				leafName  = "forward-leaf"
				runID     = "run-forward-publish"
			)

			targetUID := types.UID("uid-forward-publish-" + tt.name)

			scheme := runtime.NewScheme()
			require.NoError(t, deapi.AddToScheme(scheme))

			// The fake client does not auto-assign a UID on Create, but
			// EnsureDataExport's recordAcquisition requires a non-empty UID
			// (mirroring what a real API server returns). Stamp one, matching the
			// pattern already used for this in dataexport_test.go.
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if de, ok := obj.(*deapi.DataExport); ok && de.UID == "" {
							de.UID = types.UID("uid-" + tt.name)
						}

						return cl.Create(ctx, obj, opts...)
					},
				}).Build()

			sc, err := transport.NewClient(newConfigTestKubeconfigFlags(t))
			require.NoError(t, err)

			aggClient := aggapi.NewClient(nil, nil)

			cfg := applyDefaults(Config{
				KubeClient:       kubeClient,
				AggClient:        aggClient,
				TransportClient:  sc,
				RunID:            runID,
				Publish:          tt.publish,
				ReadinessTimeout: 30 * time.Millisecond,
			})

			require.NotNil(t, cfg.OpenExportWithTargetAcquisition,
				"applyDefaults must wire the production OpenExportWithTargetAcquisition callback")

			leafRef := aggapi.NodeRef{
				APIVersion: aggapi.VolumeSnapshotGroup + "/v1",
				Kind:       aggapi.VolumeSnapshotKind,
				Name:       leafName,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, _, openErr := cfg.OpenExportWithTargetAcquisition(ctx, namespace, leafRef, targetUID, "1h")
			require.Error(t, openErr, "WaitReady must time out against a DataExport the fake client never marks Ready")
			assert.True(t, errors.Is(openErr, context.DeadlineExceeded),
				"expected a wrapped context.DeadlineExceeded from WaitReady; got: %v", openErr)

			deName := exporter.DataExportName(
				namespace, aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind,
				leafName, targetUID)

			created := new(deapi.DataExport)
			require.NoError(t, kubeClient.Get(context.Background(),
				client.ObjectKey{Namespace: namespace, Name: deName}, created))

			assert.Equal(t, tt.publish, created.Spec.Publish,
				"the created DataExport's spec.publish must match Config.Publish")
		})
	}
}
