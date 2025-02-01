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
