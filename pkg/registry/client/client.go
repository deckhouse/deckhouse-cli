package client

import (
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	localreg "github.com/deckhouse/deckhouse-cli/pkg/registry"
)

// adapter wraps *regclient.Client from the upstream package and makes it satisfy
// the local Client interface by overriding WithSegment to return the local type.
// All other methods are promoted from the embedded *regclient.Client.
type adapter struct {
	*regclient.Client
}

// WithSegment overrides the upstream method so that it returns the local Client
// interface instead of the concrete *regclient.Client, satisfying the interface
// covariance requirement that Go does not allow otherwise.
func (a *adapter) WithSegment(segments ...string) localreg.Client {
	return &adapter{a.Client.WithSegment(segments...)}
}

// NewFromOptions wraps regclient.New (the functional-options constructor)
// and returns a value that fully satisfies the local Client interface.
// Use this when building options via Option functions (e.g. regclient.WithAuth).
func NewFromOptions(host string, opts ...regclient.Option) localreg.Client {
	return &adapter{regclient.New(host, opts...)}
}
