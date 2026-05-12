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

package registry

import (
	"crypto/tls"
	"net/http"

	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// InsecureTransport returns a fresh http.Transport cloned from remote's
// default with TLS verification disabled. Use only when the user explicitly
// opts in via --insecure.
func InsecureTransport() http.RoundTripper {
	t := remote.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // user-opted via --insecure
	return t
}
