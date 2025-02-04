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
	"crypto/tls"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/hashicorp/go-cleanhttp"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

func MakeRemoteRegistryRequestOptions(authProvider authn.Authenticator, insecure, skipTLSVerification bool) ([]name.Option, []remote.Option) {
	n, r := make([]name.Option, 0), make([]remote.Option, 0)

	if insecure {
		n = append(n, name.Insecure)
	}

	if authProvider != nil && authProvider != authn.Anonymous {
		r = append(r, remote.WithAuth(authProvider))
	}

	if skipTLSVerification {
		transport := cleanhttp.DefaultTransport()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		r = append(r, remote.WithTransport(transport))
	}

	pusher, err := remote.NewPusher(r...)
	if err != nil {
		panic(err)
	}
	puller, err := remote.NewPuller(r...)
	if err != nil {
		panic(err)
	}

	r = append(r, remote.Reuse(puller), remote.Reuse(pusher))
	return n, r
}

func MakeRemoteRegistryRequestOptionsFromMirrorParams(params *params.BaseParams) ([]name.Option, []remote.Option) {
	return MakeRemoteRegistryRequestOptions(params.RegistryAuth, params.Insecure, params.SkipTLSVerification)
}
