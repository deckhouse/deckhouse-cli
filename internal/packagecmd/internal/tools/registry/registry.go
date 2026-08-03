package registry

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/stream"
)

// options holds the resolved configuration of a registry request.
type options struct {
	// auth authenticates the request.
	auth remote.Option
}

// Option customizes how a registry request authenticates.
type Option func(*options)

// WithBasicAuth authenticates requests with username and password instead of the
// ambient Docker keychain. An empty username or password leaves the keychain in place,
// so a partially configured caller keeps working rather than losing its credentials.
func WithBasicAuth(username, password string) Option {
	return func(o *options) {
		if username == "" || password == "" {
			return
		}

		o.auth = remote.WithAuth(&authn.Basic{Username: username, Password: password})
	}
}

// Auth resolves opts into the authentication option for a remote request, defaulting
// to the ambient Docker keychain. It is exported for packages that issue their own
// registry requests instead of going through this one, such as imagefs.
func Auth(opts ...Option) remote.Option {
	o := options{auth: remote.WithAuthFromKeychain(authn.DefaultKeychain)}

	for _, opt := range opts {
		opt(&o)
	}

	return o.auth
}

// Copy copies a container image from srcRef to destRef using credentials from the default keychain.
func Copy(ctx context.Context, srcRef, destRef string) error {
	ref, err := name.ParseReference(srcRef, name.Insecure)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	src, err := remote.Image(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain), remote.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to get image: %w", err)
	}

	opts := []remote.Option{
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
		remote.WithContext(ctx),
	}

	dest, err := name.ParseReference(destRef)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	if err = remote.Write(dest, src, opts...); err != nil {
		return fmt.Errorf("failed to write image: %w", err)
	}

	return nil
}

// PushPackageIndex creates an empty package index marker image for repository.
// It extracts the package name from the registry path (e.g., "registry.io/org/pkg" -> "pkg")
// and pushes an empty image tagged as "registry.io/org:pkg".
func PushPackageIndex(ctx context.Context, repository string) error {
	img := empty.Image

	// Match the marker image produced by crane with --new_layer "".
	emptyLayer := stream.NewLayer(io.NopCloser(strings.NewReader("")))

	img, err := mutate.AppendLayers(img, emptyLayer)
	if err != nil {
		return fmt.Errorf("failed to append empty layer: %w", err)
	}

	splits := strings.Split(repository, "/")
	if len(splits) < 2 {
		return fmt.Errorf("repository must contain registry and package path: %q", repository)
	}

	base := strings.Join(splits[:len(splits)-1], "/")
	index := splits[len(splits)-1]

	ref, err := name.ParseReference(fmt.Sprintf("%s:%s", base, index))
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := []remote.Option{
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
		remote.WithContext(ctx),
	}

	if err = remote.Write(ref, img, opts...); err != nil {
		return fmt.Errorf("failed to write image to registry: %w", err)
	}

	return nil
}

// Tags lists the tags of repository, which is a repository path without a tag.
// An empty result means the repository exists but carries no tags.
func Tags(ctx context.Context, repository string, opts ...Option) ([]string, error) {
	repo, err := name.NewRepository(repository)
	if err != nil {
		return nil, fmt.Errorf("failed to parse repository: %w", err)
	}

	tags, err := remote.List(repo, Auth(opts...), remote.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to list tags of %q: %w", repository, err)
	}

	return tags, nil
}

// Exists verifies that ref exists by performing a HEAD request for its manifest.
func Exists(ctx context.Context, ref string, opts ...Option) error {
	r, err := name.ParseReference(ref)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	if _, err = remote.Head(r, Auth(opts...), remote.WithContext(ctx)); err != nil {
		return fmt.Errorf("image %q not found in registry: %w", ref, err)
	}

	return nil
}
