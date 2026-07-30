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

package util

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// readyCond builds a Ready condition with the given status/reason/message.
func readyCond(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:    "Ready",
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

func TestCreateDataImport_BuildsModeBSpec(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	pvcTpl := &v1alpha1.PersistentVolumeClaimTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-pvc"},
	}

	require.NoError(t, CreateDataImport(ctx, "import-into-pvc", "my-ns", "15m", false, true, pvcTpl, c))

	var stored v1alpha1.DataImport
	require.NoError(t, c.Get(ctx, ctrlclient.ObjectKey{Name: "import-into-pvc", Namespace: "my-ns"}, &stored))

	assert.Equal(t, v1alpha1.KindPersistentVolumeClaim, stored.Spec.TargetRef.Kind)
	assert.Equal(t, "15m", stored.Spec.TTL)
	assert.True(t, stored.Spec.WaitForFirstConsumer)
	require.NotNil(t, stored.Spec.TargetRef.PvcTemplate)
	assert.Equal(t, "restored-pvc", stored.Spec.TargetRef.PvcTemplate.Name)
}

func TestCreateDataImport_RejectsTemplateWithoutName(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	cases := map[string]*v1alpha1.PersistentVolumeClaimTemplateSpec{
		"nil template":      nil,
		"template w/o name": {},
	}

	for name, tpl := range cases {
		t.Run(name, func(t *testing.T) {
			err := CreateDataImport(ctx, "di", "my-ns", "15m", false, false, tpl, c)
			require.Error(t, err)

			var stored v1alpha1.DataImport
			getErr := c.Get(ctx, ctrlclient.ObjectKey{Name: "di", Namespace: "my-ns"}, &stored)
			require.Error(t, getErr, "no DataImport should be created when the PVC template is invalid")
		})
	}
}

// newNoAuthSafe builds a SafeClient that talks plain HTTP to an httptest server without
// requiring cluster auth (mirrors the download/list HTTP tests).
func newNoAuthSafe(t *testing.T) *safeClient.SafeClient {
	t.Helper()

	// Allow unauthenticated HTTP requests in unit tests, and point KUBECONFIG at /dev/null so
	// the client does not pick up ambient auth from a real kubeconfig.
	safeClient.SupportNoAuth = true

	oldKubeconfig := os.Getenv("KUBECONFIG")
	require.NoError(t, os.Setenv("KUBECONFIG", "/dev/null"))

	defer func() { _ = os.Setenv("KUBECONFIG", oldKubeconfig) }()

	sc, err := safeClient.NewSafeClient()
	require.NoError(t, err)

	return sc.Copy()
}

func TestPostFinished(t *testing.T) {
	t.Run("posts to <base>/api/v1/finished and succeeds on 2xx", func(t *testing.T) {
		var gotMethod, gotPath string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		// A trailing slash on the base must not produce a "//api/v1/finished" path that the
		// server's mux would 404: neturl.JoinPath cleans it to a single slash.
		require.NoError(t, PostFinished(context.Background(), newNoAuthSafe(t), srv.URL+"/"))
		assert.Equal(t, http.MethodPost, gotMethod)
		assert.Equal(t, "/api/v1/finished", gotPath)
	})

	t.Run("returns an error on non-2xx", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer srv.Close()

		err := PostFinished(context.Background(), newNoAuthSafe(t), srv.URL)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})
}

func TestEnsureDataImportPublish(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()

	newDI := func(publish bool) *v1alpha1.DataImport {
		return &v1alpha1.DataImport{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-di",
				Namespace: "test-ns",
			},
			Spec: v1alpha1.DataImportSpec{
				Publish: publish,
			},
		}
	}

	tests := []struct {
		name          string
		diObj         *v1alpha1.DataImport
		publish       bool
		wantErr       bool
		wantPublishIn bool // expected Spec.Publish in store after call
	}{
		{
			name:          "publish=false, object has Publish=false: no patch",
			diObj:         newDI(false),
			publish:       false,
			wantPublishIn: false,
		},
		{
			name:          "publish=false, object has Publish=true: no downgrade",
			diObj:         newDI(true),
			publish:       false,
			wantPublishIn: true,
		},
		{
			name:          "publish=true, object already has Publish=true: no patch",
			diObj:         newDI(true),
			publish:       true,
			wantPublishIn: true,
		},
		{
			name:          "publish=true, object has Publish=false: patched to true",
			diObj:         newDI(false),
			publish:       true,
			wantPublishIn: true,
		},
		{
			name:    "nil diObj with publish=true: returns error",
			diObj:   nil,
			publish: true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.diObj != nil {
				builder = builder.WithObjects(tt.diObj)
			}
			c := builder.Build()

			err := EnsureDataImportPublish(ctx, tt.diObj, tt.publish, c)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.diObj == nil {
				return
			}

			// Verify store state
			var stored v1alpha1.DataImport
			require.NoError(t, c.Get(ctx, ctrlclient.ObjectKey{Name: "test-di", Namespace: "test-ns"}, &stored))
			assert.Equal(t, tt.wantPublishIn, stored.Spec.Publish)
		})
	}
}

// TestGetDataImportWithRestart_ExpiredRecreates covers the auto-restart branch: a stale import whose
// Ready condition is False with Reason=Expired (the current expiry model — the producer's GC only
// deletes it after its retention TTL, so within that window it lingers with a stale status.url/
// status.volumeMode) must be deleted and recreated, and the call must return an error asking the
// caller to keep polling for the fresh import rather than returning the stale object as if it were
// ready.
// Deliberately NOT t.Parallel: this test overrides the package-level maxRetryAttempts, which is
// shared mutable state. Running it concurrently with the other overriding tests is a data race
// (caught by -race) and lets one test observe another's restored value, turning a one-iteration
// run into a 60-attempt / ~3-minute wait with the wrong error.
func TestGetDataImportWithRestart_ExpiredRecreates(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	logger := slog.Default()

	// Return after the first iteration instead of looping 60+ times.
	orig := maxRetryAttempts
	maxRetryAttempts = -1
	t.Cleanup(func() { maxRetryAttempts = orig })

	pvcTpl := &v1alpha1.PersistentVolumeClaimTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-pvc"},
	}

	expired := &v1alpha1.DataImport{
		ObjectMeta: metav1.ObjectMeta{Name: "test-di", Namespace: "test-ns"},
		Spec: v1alpha1.DataImportSpec{
			TTL:                  "15m",
			Publish:              true,
			WaitForFirstConsumer: true,
			TargetRef: v1alpha1.DataImportTargetRefSpec{
				Kind:        v1alpha1.KindPersistentVolumeClaim,
				PvcTemplate: pvcTpl,
			},
		},
		Status: v1alpha1.DataExportImportStatus{
			// Deliberately filled in, as a dying importer's status is until retention-GC reaps it.
			// Before the fix, GetDataImportWithRestart would fall through the recreate branch,
			// see these already populated, and hand back the stale object as if it were ready.
			URL:        "https://10.0.0.1:8085/",
			VolumeMode: "Filesystem",
			Conditions: []metav1.Condition{
				readyCond(metav1.ConditionFalse, "Expired", "import idle timeout reached"),
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(expired).Build()

	_, err := GetDataImportWithRestart(ctx, "test-di", "test-ns", c, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expired; recreated, waiting")

	// The stale object must have been replaced by a fresh one: same name, but the Expired
	// condition (and all status) is gone because CreateDataImport writes a spec-only object.
	var recreated v1alpha1.DataImport
	require.NoError(t, c.Get(ctx, ctrlclient.ObjectKey{Name: "test-di", Namespace: "test-ns"}, &recreated))
	assert.Empty(t, recreated.Status.Conditions, "expired condition must be cleared after recreate")
	assert.Empty(t, recreated.Status.URL, "stale URL must be cleared after recreate")
	assert.Empty(t, recreated.Status.VolumeMode, "stale VolumeMode must be cleared after recreate")
	assert.Equal(t, "15m", recreated.Spec.TTL, "TTL must be carried over to the fresh import")
	assert.True(t, recreated.Spec.Publish, "Publish must be carried over to the fresh import")
	assert.True(t, recreated.Spec.WaitForFirstConsumer, "WaitForFirstConsumer must be carried over to the fresh import")
	require.NotNil(t, recreated.Spec.TargetRef.PvcTemplate)
	assert.Equal(t, "restored-pvc", recreated.Spec.TargetRef.PvcTemplate.Name, "PvcTemplate must be carried over to the fresh import")
}

// TestGetDataImportWithRestart_LegacyExpiredConditionIsNotUsed asserts the contract change: a
// legacy standalone Type=="Expired" condition (the old, now version-skewed, detection signal) must
// not trigger a recreate anymore. Only the current controller's Ready=False/Reason=Expired signal
// does.
//
// Deliberately NOT t.Parallel: it overrides the package-level maxRetryAttempts (see the note on
// TestGetDataImportWithRestart_ExpiredRecreates).
func TestGetDataImportWithRestart_LegacyExpiredConditionIsNotUsed(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	logger := slog.Default()

	orig := maxRetryAttempts
	maxRetryAttempts = -1
	t.Cleanup(func() { maxRetryAttempts = orig })

	di := &v1alpha1.DataImport{
		ObjectMeta: metav1.ObjectMeta{Name: "test-di", Namespace: "test-ns"},
		Status: v1alpha1.DataExportImportStatus{
			URL:        "https://10.0.0.1:8085/",
			VolumeMode: "Filesystem",
			Conditions: []metav1.Condition{
				// Legacy signal: a standalone Expired condition, distinct from Ready.
				{Type: "Expired", Status: metav1.ConditionTrue},
				readyCond(metav1.ConditionTrue, "PodReady", "Pod is ready and import completed"),
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(di).Build()

	got, err := GetDataImportWithRestart(ctx, "test-di", "test-ns", c, logger)
	require.NoError(t, err)
	assert.Equal(t, "https://10.0.0.1:8085/", got.Status.URL)

	// Confirm no delete+recreate happened: the object in the store is still the original.
	var stored v1alpha1.DataImport
	require.NoError(t, c.Get(ctx, ctrlclient.ObjectKey{Name: "test-di", Namespace: "test-ns"}, &stored))
	assert.Len(t, stored.Status.Conditions, 2, "legacy Expired condition must not trigger a recreate")
}

// TestGetDataImportWithRestart_CompletedIsNotTreatedAsExpired guards against too broad a predicate:
// a Ready=False/Reason=Completed condition (normal completion signalling, not expiry) must fall into
// the not-Ready branch, not the recreate branch.
// Deliberately NOT t.Parallel: it overrides the package-level maxRetryAttempts (see the note on
// TestGetDataImportWithRestart_ExpiredRecreates).
func TestGetDataImportWithRestart_CompletedIsNotTreatedAsExpired(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	logger := slog.Default()

	orig := maxRetryAttempts
	maxRetryAttempts = -1
	t.Cleanup(func() { maxRetryAttempts = orig })

	di := &v1alpha1.DataImport{
		ObjectMeta: metav1.ObjectMeta{Name: "test-di", Namespace: "test-ns"},
		Status: v1alpha1.DataExportImportStatus{
			Conditions: []metav1.Condition{
				readyCond(metav1.ConditionFalse, "Completed", "import finished"),
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(di).Build()

	_, err := GetDataImportWithRestart(ctx, "test-di", "test-ns", c, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not Ready")
	assert.Contains(t, err.Error(), "(Completed)")

	// Confirm no delete+recreate happened.
	var stored v1alpha1.DataImport
	require.NoError(t, c.Get(ctx, ctrlclient.ObjectKey{Name: "test-di", Namespace: "test-ns"}, &stored))
	assert.Len(t, stored.Status.Conditions, 1, "Completed reason must not trigger a recreate")
}
