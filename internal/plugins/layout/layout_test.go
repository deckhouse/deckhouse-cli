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

package layout

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestValidatePluginName pins the name grammar: one lowercase OCI path
// component. Anything that could leave the plugins root or change a registry
// route is rejected.
func TestValidatePluginName(t *testing.T) {
	valid := []string{"stronghold", "postgresql-mgr", "db_connector", "tool.v2", "a1"}
	for _, name := range valid {
		assert.NoError(t, ValidatePluginName(name), name)
	}

	invalid := []string{
		"",
		"..",
		"../../outside",
		"a/b",
		"Stronghold",
		"-leading",
		"trailing-",
		"double--dash",
		"with space",
		"tag:v1",
		"name@v1",
	}
	for _, name := range invalid {
		assert.Error(t, ValidatePluginName(name), name)
	}
}
