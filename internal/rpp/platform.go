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

package rpp

import "runtime"

// currentPlatform is the platform string of the running binary ("os-arch",
// e.g. "linux-amd64"). It is sent as the ?platform= query on a pull so the proxy
// picks the matching child manifest from a multi-platform image index. The dash
// separator keeps the value a single unescaped URL token (a slash would be
// percent-encoded and split path routers).
func currentPlatform() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}
