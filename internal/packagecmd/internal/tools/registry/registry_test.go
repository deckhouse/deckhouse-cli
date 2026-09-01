package registry

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"
)

func TestNameOptions(t *testing.T) {
	// A private-range or localhost registry resolves to HTTP on its own, so the
	// fixture uses a public host to tell the two code paths apart.
	t.Run("default parses a remote registry as HTTPS", func(t *testing.T) {
		ref, err := name.ParseReference("registry.example.com:5000/packages/app:v1.0.0", NameOptions()...)
		require.NoError(t, err)
		require.Equal(t, "https", ref.Context().Registry.Scheme())
	})

	t.Run("insecure parses a remote registry as HTTP", func(t *testing.T) {
		ref, err := name.ParseReference("registry.example.com:5000/packages/app:v1.0.0", NameOptions(WithInsecure())...)
		require.NoError(t, err)
		require.Equal(t, "http", ref.Context().Registry.Scheme())
	})
}

func TestRemoteOptions(t *testing.T) {
	ctx := context.Background()

	require.Len(t, RemoteOptions(ctx), 2, "auth and context only")
	require.Len(t, RemoteOptions(ctx, WithInsecure()), 3, "auth, context and transport")
}

func TestInsecureTransport(t *testing.T) {
	transport, ok := insecureTransport().(*http.Transport)
	require.True(t, ok)
	require.True(t, transport.TLSClientConfig.InsecureSkipVerify)

	// The shared ggcr default transport must stay untouched.
	require.NotSame(t, remote.DefaultTransport, transport)
}
