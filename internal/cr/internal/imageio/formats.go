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

package imageio

// Pull output formats. Single source of truth - the cobra command (basic/pull.go)
// and the shell-completion enum (cmd/completion) both read from here.
const (
	PullFormatTarball = "tarball"
	PullFormatLegacy  = "legacy"
	PullFormatOCI     = "oci"
)

var pullFormats = []string{PullFormatTarball, PullFormatLegacy, PullFormatOCI}

// PullFormats returns a defensive copy of the format enum suitable for
// cobra completion or help-text generation.
func PullFormats() []string {
	return append([]string(nil), pullFormats...)
}
