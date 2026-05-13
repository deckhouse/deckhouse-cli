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

// Package rootflagnames holds the persistent-flag names of the `d8 cr`
// root command in one place. Both the producer (cmd/rootflags.go, where
// the flags are registered) and the consumers (e.g. cmd/completion, which
// re-reads them at completion time because PersistentPreRunE does not run
// during shell completion) must agree on the literal name strings -
// keeping them here prevents silent breakage on future renames.
package rootflagnames

const (
	Verbose               = "verbose"
	Insecure              = "insecure"
	AllowNondistributable = "allow-nondistributable-artifacts"
	Platform              = "platform"
)
