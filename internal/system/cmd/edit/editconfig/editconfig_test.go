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

package edit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	testSecret  = "d8-static-cluster-configuration"
	testDataKey = "static-cluster-configuration.yaml"
)

var testCreateOpts = &CreateOptions{Labels: map[string]string{"name": testSecret}}

func replaceWith(content string) editFunc {
	return func([]byte) ([]byte, error) {
		return []byte(content), nil
	}
}

func hasAction(client *fake.Clientset, verb string) bool {
	for _, action := range client.Actions() {
		if action.GetVerb() == verb {
			return true
		}
	}

	return false
}

func getSecret(t *testing.T, client *fake.Clientset) *v1.Secret {
	t.Helper()

	secret, err := client.CoreV1().Secrets(secretNamespace).Get(context.Background(), testSecret, metav1.GetOptions{})
	require.NoError(t, err)

	return secret
}

func TestEditSecretPatchesExisting(t *testing.T) {
	existing := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSecret,
			Namespace: secretNamespace,
			Labels:    map[string]string{"name": testSecret, "heritage": "deckhouse"},
		},
		Data: map[string][]byte{testDataKey: []byte("old: config\n")},
	}
	client := fake.NewClientset(existing)

	var edited []byte

	err := editSecret(context.Background(), client, testSecret, testDataKey, testCreateOpts, func(content []byte) ([]byte, error) {
		edited = content
		return []byte("new: config\n"), nil
	})
	require.NoError(t, err)

	assert.Equal(t, "old: config\n", string(edited))
	assert.True(t, hasAction(client, "patch"))
	assert.False(t, hasAction(client, "create"))

	secret := getSecret(t, client)
	assert.Equal(t, "new: config\n", string(secret.Data[testDataKey]))
	assert.Equal(t, existing.Labels, secret.Labels)
}

func TestEditSecretSkipsUnchangedExisting(t *testing.T) {
	client := fake.NewClientset(&v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testSecret, Namespace: secretNamespace},
		Data:       map[string][]byte{testDataKey: []byte("same\n")},
	})

	err := editSecret(context.Background(), client, testSecret, testDataKey, testCreateOpts, replaceWith("same\n"))
	require.NoError(t, err)

	assert.False(t, hasAction(client, "patch"))
	assert.False(t, hasAction(client, "create"))
}

func TestEditSecretCreatesAbsent(t *testing.T) {
	client := fake.NewClientset()

	var edited []byte

	err := editSecret(context.Background(), client, testSecret, testDataKey, testCreateOpts, func(content []byte) ([]byte, error) {
		edited = content
		return []byte("apiVersion: deckhouse.io/v1\nkind: StaticClusterConfiguration\n"), nil
	})
	require.NoError(t, err)

	assert.Empty(t, edited)
	assert.True(t, hasAction(client, "create"))
	assert.False(t, hasAction(client, "patch"))

	secret := getSecret(t, client)
	assert.Equal(t, secretNamespace, secret.Namespace)
	assert.Equal(t, v1.SecretTypeOpaque, secret.Type)
	assert.Equal(t, map[string]string{"name": testSecret}, secret.Labels)
	assert.Equal(t, map[string][]byte{
		testDataKey: []byte("apiVersion: deckhouse.io/v1\nkind: StaticClusterConfiguration\n"),
	}, secret.Data)
}

func TestEditSecretSkipsEmptyAbsent(t *testing.T) {
	for name, content := range map[string]string{
		"empty":      "",
		"whitespace": " \n\t\n",
	} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewClientset()

			err := editSecret(context.Background(), client, testSecret, testDataKey, testCreateOpts, replaceWith(content))
			require.NoError(t, err)

			assert.False(t, hasAction(client, "create"))
		})
	}
}

func TestEditSecretAbsentWithoutCreateOpts(t *testing.T) {
	client := fake.NewClientset()
	editCalled := false

	err := editSecret(context.Background(), client, testSecret, testDataKey, nil, func([]byte) ([]byte, error) {
		editCalled = true
		return nil, nil
	})

	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
	assert.False(t, editCalled)
	assert.False(t, hasAction(client, "create"))
}

func TestEditSecretGetErrorDoesNotCreate(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, testSecret, nil)
	})

	err := editSecret(context.Background(), client, testSecret, testDataKey, testCreateOpts, replaceWith("a: b\n"))

	require.Error(t, err)
	assert.True(t, apierrors.IsForbidden(err))
	assert.False(t, hasAction(client, "create"))
}

func TestEditSecretCreateConflict(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, testSecret)
	})

	err := editSecret(context.Background(), client, testSecret, testDataKey, testCreateOpts, replaceWith("a: b\n"))

	require.Error(t, err)
	assert.True(t, apierrors.IsAlreadyExists(err))
	assert.False(t, hasAction(client, "patch"))
}
