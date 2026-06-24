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

package plugins

// SetBuiltinCommands records the d8 built-in command names that satisfy a plugin
// dependency of the same name. It bridges capabilities that ship as a built-in
// command but are not yet published as standalone plugins (e.g. delivery-kit):
// such a dependency counts as satisfied by the command's mere presence, with no
// on-disk install and no registry lookup.
func (m *Manager) SetBuiltinCommands(names []string) {
	if len(names) == 0 {
		m.builtins = nil

		return
	}

	m.builtins = make(map[string]struct{}, len(names))
	for _, name := range names {
		m.builtins[name] = struct{}{}
	}
}

// isBuiltinCommand reports whether name is provided by a built-in command and so
// satisfies a plugin dependency on it. Version constraints are not enforced: a
// built-in cannot be upgraded, so its presence alone satisfies the dependency.
func (m *Manager) isBuiltinCommand(name string) bool {
	_, ok := m.builtins[name]

	return ok
}
