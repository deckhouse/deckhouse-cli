//go:build !windows

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

package archive

import (
	"fmt"
	"os"
)

func renameDurably(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

// syncDir makes preceding renames and creates visible after a power loss.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening dir %s for sync: %w", path, err)
	}

	if err := d.Sync(); err != nil {
		_ = d.Close()

		return fmt.Errorf("syncing dir %s: %w", path, err)
	}

	return d.Close()
}
