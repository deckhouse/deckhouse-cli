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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
)

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
