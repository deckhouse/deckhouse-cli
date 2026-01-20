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

package auth

import (
	"reflect"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/stretchr/testify/require"
)

func TestMakeRemoteRegistryRequestOptionsAnonymous(t *testing.T) {
	nameOpts, remoteOpts := MakeRemoteRegistryRequestOptions(nil, false, false)
	require.Len(t, remoteOpts, 2, "should have only 2 remote options for puller and pusher reuse")
	require.Len(t, nameOpts, 0)
}

func TestMakeRemoteRegistryRequestOptionsAnonymousInsecure(t *testing.T) {
	nameOpts, remoteOpts := MakeRemoteRegistryRequestOptions(nil, true, false)
	require.Len(t, remoteOpts, 2, "should have only 2 remote options for puller and pusher reuse")
	require.Len(t, nameOpts, 1, "should have only 1 name option for http protocol")

	expectedOptionFnPtr := reflect.PointerTo(reflect.TypeOf(name.Option(name.Insecure)))
	gotOptionFnPtr := reflect.PointerTo(reflect.TypeOf(nameOpts[0]))
	require.Equal(t, expectedOptionFnPtr, gotOptionFnPtr)
}

func TestMakeRemoteRegistryRequestOptions_InsecureHTTPScheme(t *testing.T) {
	t.Run("insecure flag enables HTTP scheme for registry references", func(t *testing.T) {
		nameOpts, _ := MakeRemoteRegistryRequestOptions(nil, true, false)
		require.Len(t, nameOpts, 1, "should return name.Insecure option")

		ref, err := name.ParseReference("localhost:5000/repo:tag", nameOpts...)
		require.NoError(t, err)
		require.Equal(t, "http", ref.Context().Registry.Scheme(), "should use HTTP scheme with insecure flag")
	})

	t.Run("secure mode uses HTTPS scheme", func(t *testing.T) {
		nameOpts, _ := MakeRemoteRegistryRequestOptions(nil, false, false)
		require.Len(t, nameOpts, 0, "should return no name options")

		ref, err := name.ParseReference("registry.example.com/repo:tag", nameOpts...)
		require.NoError(t, err)
		require.Equal(t, "https", ref.Context().Registry.Scheme(), "should use HTTPS scheme by default")
	})

	t.Run("insecure flag works with localhost registry", func(t *testing.T) {
		nameOpts, _ := MakeRemoteRegistryRequestOptions(nil, true, false)

		ref, err := name.ParseReference("localhost:5000/deckhouse/install:v1.0.0", nameOpts...)
		require.NoError(t, err)
		require.Equal(t, "http", ref.Context().Registry.Scheme())
		require.Equal(t, "localhost:5000", ref.Context().RegistryStr())
	})

	t.Run("insecure flag works with IP-based registry", func(t *testing.T) {
		nameOpts, _ := MakeRemoteRegistryRequestOptions(nil, true, false)

		ref, err := name.ParseReference("192.168.1.100:5000/repo:tag", nameOpts...)
		require.NoError(t, err)
		require.Equal(t, "http", ref.Context().Registry.Scheme())
	})
}

func TestMakeRemoteRegistryRequestOptions_TLSSkipVerify(t *testing.T) {
	t.Run("TLS skip verify creates custom transport", func(t *testing.T) {
		_, remoteOpts := MakeRemoteRegistryRequestOptions(nil, false, true)
		require.Len(t, remoteOpts, 3, "should have 3 remote options: transport + puller + pusher")
	})

	t.Run("both insecure and TLS skip verify", func(t *testing.T) {
		nameOpts, remoteOpts := MakeRemoteRegistryRequestOptions(nil, true, true)
		require.Len(t, nameOpts, 1, "should have name.Insecure option")
		require.Len(t, remoteOpts, 3, "should have transport + puller + pusher options")
	})

	t.Run("secure mode without TLS skip", func(t *testing.T) {
		nameOpts, remoteOpts := MakeRemoteRegistryRequestOptions(nil, false, false)
		require.Len(t, nameOpts, 0, "should have no name options")
		require.Len(t, remoteOpts, 2, "should have only puller + pusher options")
	})
}

func TestMakeRemoteRegistryRequestOptions_RegressionTest(t *testing.T) {
	t.Run("insecure flag must be passed to name.ParseReference", func(t *testing.T) {
		nameOpts, _ := MakeRemoteRegistryRequestOptions(nil, true, false)

		require.NotEmpty(t, nameOpts, "name options must not be empty when insecure=true")

		ref, err := name.ParseReference("localhost:5000/deckhouse/ee:v1.63.0", nameOpts...)
		require.NoError(t, err, "should parse reference with insecure option")
		require.Equal(t, "http", ref.Context().Registry.Scheme(),
			"REGRESSION: insecure flag must result in HTTP scheme, not HTTPS")
	})

	t.Run("without insecure flag remote registry defaults to HTTPS", func(t *testing.T) {
		nameOpts, _ := MakeRemoteRegistryRequestOptions(nil, false, false)

		ref, err := name.ParseReference("registry.example.com:5000/repo:tag", nameOpts...)
		require.NoError(t, err)
		require.Equal(t, "https", ref.Context().Registry.Scheme(),
			"without insecure flag, remote registry should default to HTTPS")
	})
}
