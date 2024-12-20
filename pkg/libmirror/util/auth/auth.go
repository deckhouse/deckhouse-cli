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
	"context"
	"crypto/tls"
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/hashicorp/go-cleanhttp"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
)

func ValidateReadAccessForImage(imageTag string, authProvider authn.Authenticator, insecure, skipVerifyTLS bool) error {
	return ValidateReadAccessForImageContext(context.Background(), imageTag, authProvider, insecure, skipVerifyTLS)
}

func ValidateReadAccessForImageContext(
	ctx context.Context,
	imageTag string,
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
) error {
	nameOpts, remoteOpts := MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)
	ref, err := name.ParseReference(imageTag, nameOpts...)
	if err != nil {
		return fmt.Errorf("Parse registry address: %w", err)
	}

	remoteOpts = append(remoteOpts, remote.WithContext(ctx))
	_, err = remote.Head(ref, remoteOpts...)
	if err != nil {
		return err
	}

	return nil
}

func ValidateWriteAccessForRepo(repo string, authProvider authn.Authenticator, insecure, skipVerifyTLS bool) error {
	return ValidateWriteAccessForRepoContext(context.Background(), repo, authProvider, insecure, skipVerifyTLS)
}

func ValidateWriteAccessForRepoContext(
	ctx context.Context,
	repo string,
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
) error {
	nameOpts, remoteOpts := MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)
	remoteOpts = append(remoteOpts, remote.WithContext(ctx))
	ref, err := name.NewTag(repo+":d8WriteCheck", nameOpts...)
	if err != nil {
		return err
	}

	syntheticImage, err := random.Image(512, 1)
	if err != nil {
		return fmt.Errorf("Generate random image: %w", err)
	}

	// We do not delete uploaded synthetic image, not all registries are set up to take DELETE requests kindly
	if err = remote.Write(ref, syntheticImage, remoteOpts...); err != nil {
		return err
	}
	return nil
}

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

	return n, r
}

func MakeRemoteRegistryRequestOptionsFromMirrorContext(mirrorCtx *contexts.BaseContext) ([]name.Option, []remote.Option) {
	return MakeRemoteRegistryRequestOptions(mirrorCtx.RegistryAuth, mirrorCtx.Insecure, mirrorCtx.SkipTLSVerification)
}
