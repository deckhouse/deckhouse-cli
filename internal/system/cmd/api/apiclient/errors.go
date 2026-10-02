/*
Copyright 2026 Flant JSC

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

package apiclient

import (
	"errors"
	"fmt"
	"net/http"
)

// Sentinel errors of the client, for errors.Is. A StatusError unwraps to the one
// of its status class; the others come wrapped with the name or object they concern.
var (
	// ErrNotFound is a 404: a route the controller does not serve over HTTP, such as
	// any route of a controller without Module v2, or a package it does not know.
	ErrNotFound = errors.New("not found")
	// ErrBadRequest is a 400: an output format the API does not know, or a render
	// of a package that has no Helm chart.
	ErrBadRequest = errors.New("bad request")
	// ErrServerError is a 5xx: the controller failed to answer, a render failure for
	// one, an unknown package included.
	ErrServerError = errors.New("server error")
	// ErrUnknownPackage is a dump that answered null for the package asked for.
	ErrUnknownPackage = errors.New("unknown package")
	// ErrGlobalNotLoaded is the global module dump before the runtime has
	// initialized the global module.
	ErrGlobalNotLoaded = errors.New("global module is not loaded")
	// ErrNotReady is a replica whose /readyz answers that it is not ready.
	ErrNotReady = errors.New("not ready")
	// ErrNoLeader is a cluster without a pod of the leading controller replica.
	ErrNoLeader = errors.New("no Deckhouse leader pod")
	// ErrNoSelfPort is a controller pod whose container declares no "self" port,
	// where the API listens.
	ErrNoSelfPort = errors.New(`no "self" port`)
)

// StatusError is an answer of the API outside 2xx.
type StatusError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("GET %s: HTTP %d: %s", e.Path, e.StatusCode, e.Body)
}

// Unwrap returns the sentinel of the status class, nil for a status without one.
func (e *StatusError) Unwrap() error {
	switch {
	case e.StatusCode == http.StatusNotFound:
		return ErrNotFound
	case e.StatusCode == http.StatusBadRequest:
		return ErrBadRequest
	case e.StatusCode >= http.StatusInternalServerError:
		return ErrServerError
	}

	return nil
}
