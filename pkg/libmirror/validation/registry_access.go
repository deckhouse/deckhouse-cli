/*
Copyright 2025 Flant JSC

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

package validation

import (
	"context"
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

type options struct {
	plainHTTP           bool
	skipTLSVerification bool
	authProvider        authn.Authenticator
}

type Option func(o *options)

func WithInsecure(insecure bool) Option {
	return func(o *options) {
		o.plainHTTP = insecure
	}
}

func WithTLSVerificationSkip(skipVerifyTLS bool) Option {
	return func(o *options) {
		o.skipTLSVerification = skipVerifyTLS
	}
}

func UseAuthProvider(authProvider authn.Authenticator) Option {
	return func(o *options) {
		o.authProvider = authProvider
	}
}

type RegistryReadAccessValidator interface {
	ValidateReadAccessForImage(ctx context.Context, imageTag string, opts ...Option) error
}

type RegistryWriteAccessValidator interface {
	ValidateWriteAccessForRepo(ctx context.Context, repo string, opts ...Option) error
}

type RegistryTagsListingAccessValidator interface {
	ValidateListAccessForRepo(ctx context.Context, repo string, opts ...Option) error
}

type RemoteRegistryAccessValidator struct{}

func NewRemoteRegistryAccessValidator() *RemoteRegistryAccessValidator {
	return &RemoteRegistryAccessValidator{}
}

func (v *RemoteRegistryAccessValidator) ValidateReadAccessForImage(ctx context.Context, imageRef string, o ...Option) error {
	opts := v.useOptions(o)
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(opts.authProvider, opts.plainHTTP, opts.skipTLSVerification)
	remoteOpts = append(remoteOpts, remote.WithContext(ctx))

	ref, err := name.ParseReference(imageRef, nameOpts...)
	if err != nil {
		return fmt.Errorf("Parse registry address: %w", err)
	}

	_, err = remote.Head(ref, remoteOpts...)
	if err != nil {
		return err
	}

	return nil
}

func (v *RemoteRegistryAccessValidator) ValidateWriteAccessForRepo(ctx context.Context, repo string, o ...Option) error {
	opts := v.useOptions(o)
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(opts.authProvider, opts.plainHTTP, opts.skipTLSVerification)
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

func (v *RemoteRegistryAccessValidator) useOptions(opts []Option) *options {
	o := &options{
		authProvider: authn.Anonymous,
	}

	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (v *RemoteRegistryAccessValidator) ValidateListAccessForRepo(ctx context.Context, repo string, o ...Option) error {
	opts := v.useOptions(o)
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(opts.authProvider, opts.plainHTTP, opts.skipTLSVerification)
	remoteOpts = append(remoteOpts, remote.WithContext(ctx))

	ref, err := name.NewRepository(repo, nameOpts...)
	if err != nil {
		return fmt.Errorf("Parse registry repository: %w", err)
	}

	_, err = remote.List(ref, remoteOpts...)
	if err != nil {
		return fmt.Errorf("List tags: %w", err)
	}

	return nil
}
