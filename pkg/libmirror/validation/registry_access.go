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

func UsePlainHTTP() Option {
	return func(o *options) {
		o.plainHTTP = true
	}
}

func SkipTLSVerification() Option {
	return func(o *options) {
		o.skipTLSVerification = true
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

type RemoteRegistryAccessValidator struct{}

func NewRemoteRegistryAccessValidator() *RemoteRegistryAccessValidator {
	return &RemoteRegistryAccessValidator{}
}

func (v *RemoteRegistryAccessValidator) ValidateReadAccessForImage(ctx context.Context, imageTag string, o ...Option) error {
	opts := v.useOptions(o)
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(opts.authProvider, opts.plainHTTP, opts.skipTLSVerification)
	remoteOpts = append(remoteOpts, remote.WithContext(ctx))

	ref, err := name.ParseReference(imageTag, nameOpts...)
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
