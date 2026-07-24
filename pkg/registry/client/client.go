package client

import (
	"strings"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"
)

// PathToSegments splits a slash-separated path into its non-empty segments,
// ready to spread into Client.WithSegment (which takes segments one by one and
// does not split slashes). Leading, trailing, and doubled slashes - and the
// empty string - drop out, so they never become a bogus empty segment.
//
//   - PathToSegments("modules/csi") -> ["modules", "csi"]
//   - PathToSegments("/modules/")   -> ["modules"]
//   - PathToSegments("")            -> [] (WithSegment() then leaves the client unchanged)
func PathToSegments(path string) []string {
	segments := make([]string, 0)

	for _, segment := range strings.Split(path, "/") {
		if segment == "" {
			continue
		}

		segments = append(segments, segment)
	}

	return segments
}

// adapter wraps the upstream dkpreg.Client interface and makes it satisfy
// the local Client interface by overriding WithSegment to return the local type.
// All other methods are promoted from the embedded dkpreg.Client.
type adapter struct {
	dkpreg.Client
}

// WithSegment overrides the upstream method so that it returns the local Client
// interface instead of the upstream dkpreg.Client, satisfying the interface
// covariance requirement that Go does not allow otherwise.
func (a *adapter) WithSegment(segments ...string) dkpreg.Client {
	return &adapter{a.Client.WithSegment(segments...)}
}

// Adapt wraps an upstream dkpreg.Client so that it satisfies the local
// Client interface.  This is useful for fake/stub clients that return the
// upstream interface type.
func Adapt(c dkpreg.Client) dkpreg.Client {
	return &adapter{c}
}

// NewFromOptions wraps regclient.New (the functional-options constructor)
// and returns a value that fully satisfies the local Client interface.
// Use this when building options via Option functions (e.g. regclient.WithAuth).
func NewFromOptions(host string, opts ...regclient.Option) dkpreg.Client {
	return &adapter{regclient.New(host, opts...)}
}
