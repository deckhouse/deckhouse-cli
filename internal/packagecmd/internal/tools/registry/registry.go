package registry

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
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
	// insecure allows plain HTTP registries and skips TLS verification.
	insecure bool
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

// WithInsecure talks to the registry over plain HTTP and skips TLS certificate
// verification. Use only when the user explicitly opts in.
func WithInsecure() Option {
	return func(o *options) {
		o.insecure = true
	}
}

// resolve applies opts over the defaults: the ambient Docker keychain and verified HTTPS.
func resolve(opts ...Option) options {
	o := options{auth: remote.WithAuthFromKeychain(authn.DefaultKeychain)}

	for _, opt := range opts {
		opt(&o)
	}

	return o
}

// Auth resolves opts into the authentication option for a remote request, defaulting
// to the ambient Docker keychain. It is exported for packages that issue their own
// registry requests instead of going through this one, such as imagefs.
func Auth(opts ...Option) remote.Option {
	return resolve(opts...).auth
}

// RemoteOptions resolves opts into the options of a remote request bound to ctx.
func RemoteOptions(ctx context.Context, opts ...Option) []remote.Option {
	o := resolve(opts...)

	r := []remote.Option{o.auth, remote.WithContext(ctx)}
	if o.insecure {
		r = append(r, remote.WithTransport(insecureTransport()))
	}

	return r
}

// NameOptions resolves opts into the options of reference and repository parsing.
func NameOptions(opts ...Option) []name.Option {
	if resolve(opts...).insecure {
		return []name.Option{name.Insecure}
	}

	return nil
}

// insecureTransport clones remote's default transport with TLS verification disabled.
func insecureTransport() http.RoundTripper {
	t := remote.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // user-opted via --insecure

	return t
}

// Copy copies a container image from srcRef to destRef using credentials from the default keychain.
func Copy(ctx context.Context, srcRef, destRef string, opts ...Option) error {
	nameOpts := NameOptions(opts...)
	remoteOpts := RemoteOptions(ctx, opts...)

	ref, err := name.ParseReference(srcRef, nameOpts...)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	src, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		return fmt.Errorf("failed to get image: %w", err)
	}

	dest, err := name.ParseReference(destRef, nameOpts...)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	if err = remote.Write(dest, src, remoteOpts...); err != nil {
		return fmt.Errorf("failed to write image: %w", err)
	}

	return nil
}

// PushPackageIndex creates an empty package index marker image for repository.
// It extracts the package name from the registry path (e.g., "registry.io/org/pkg" -> "pkg")
// and pushes an empty image tagged as "registry.io/org:pkg".
func PushPackageIndex(ctx context.Context, repository string, opts ...Option) error {
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

	ref, err := name.ParseReference(fmt.Sprintf("%s:%s", base, index), NameOptions(opts...)...)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	if err = remote.Write(ref, img, RemoteOptions(ctx, opts...)...); err != nil {
		return fmt.Errorf("failed to write image to registry: %w", err)
	}

	return nil
}

// Tags lists the tags of repository, which is a repository path without a tag.
// An empty result means the repository exists but carries no tags.
func Tags(ctx context.Context, repository string, opts ...Option) ([]string, error) {
	repo, err := name.NewRepository(repository, NameOptions(opts...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse repository: %w", err)
	}

	tags, err := remote.List(repo, RemoteOptions(ctx, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags of %q: %w", repository, err)
	}

	return tags, nil
}

// Exists verifies that ref exists by performing a HEAD request for its manifest.
func Exists(ctx context.Context, ref string, opts ...Option) error {
	r, err := name.ParseReference(ref, NameOptions(opts...)...)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	if _, err = remote.Head(r, RemoteOptions(ctx, opts...)...); err != nil {
		return fmt.Errorf("image %q not found in registry: %w", ref, err)
	}

	return nil
}
