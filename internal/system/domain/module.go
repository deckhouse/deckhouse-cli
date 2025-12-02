/*
Copyright 2024 Flant JSC

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

package domain

// Module represents a Deckhouse module
type Module struct {
	Name    string
	Enabled bool
	State   ModuleState
	Weight  int
}

// ModuleState represents module state
type ModuleState string

const (
	ModuleStateEnabled  ModuleState = "Enabled"
	ModuleStateDisabled ModuleState = "Disabled"
)

// ModuleValues represents module values/configuration
type ModuleValues struct {
	ModuleName string
	Values     map[string]interface{}
}

// ModuleSnapshot represents a module snapshot
type ModuleSnapshot struct {
	ModuleName string
	Snapshots  []SnapshotInfo
}

// SnapshotInfo contains snapshot information
type SnapshotInfo struct {
	Name      string
	Binding   string
	Queue     string
	Snapshots []string
}

