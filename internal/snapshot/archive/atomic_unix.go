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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type durableDir interface {
	OpenRoot(string) (durableDir, error)
	Mkdir(string, os.FileMode) error
	Sync() error
	Close() error
}

type rootedDurableDir struct {
	root *os.Root
}

type durableDirStep struct {
	dir  durableDir
	path string
}

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

func ensureDirDurably(path string) error {
	rootPath, components, err := durableDirPath(path)
	if err != nil {
		return fmt.Errorf("resolving dir %s: %w", path, err)
	}

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("opening durability root %s for dir %s: %w", rootPath, path, err)
	}

	return ensureDirAt(&rootedDurableDir{root: root}, rootPath, components)
}

func durableDirPath(path string) (string, []string, error) {
	if path == "" {
		return "", nil, os.ErrInvalid
	}

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", nil, err
	}

	rootPath := string(filepath.Separator)

	relativePath, err := filepath.Rel(rootPath, absolutePath)
	if err != nil {
		return "", nil, err
	}

	if relativePath == "." {
		return rootPath, nil, nil
	}

	return rootPath, strings.Split(relativePath, string(filepath.Separator)), nil
}

// ensureDirAt keeps a rooted handle for every component. This pins each
// traversed directory across concurrent renames and lets the durability pass
// sync the actual containing directories rather than reopening mutable paths.
// Every call confirms the full chain because an existing component may be
// residue from a previous process that failed before syncing its parent.
func ensureDirAt(root durableDir, rootPath string, components []string) error {
	steps := make([]durableDirStep, 1, len(components)+1)
	steps[0] = durableDirStep{dir: root, path: rootPath}

	for _, component := range components {
		parent := steps[len(steps)-1]
		childPath := filepath.Join(parent.path, component)

		child, err := parent.dir.OpenRoot(component)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return closeDurableDirs(steps, fmt.Errorf("opening dir %s: %w", childPath, err))
		}

		if errors.Is(err, os.ErrNotExist) {
			if mkdirErr := parent.dir.Mkdir(component, 0o755); mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
				return closeDurableDirs(steps, fmt.Errorf("creating dir %s: %w", childPath, mkdirErr))
			}

			child, err = parent.dir.OpenRoot(component)
			if err != nil {
				return closeDurableDirs(steps, fmt.Errorf("opening created dir %s: %w", childPath, err))
			}
		}

		steps = append(steps, durableDirStep{dir: child, path: childPath})
	}

	for i := len(steps) - 1; i >= 0; i-- {
		if err := steps[i].dir.Sync(); err != nil {
			syncErr := fmt.Errorf("syncing dir %s: %w", steps[i].path, err)

			return closeDurableDirs(steps, syncErr)
		}
	}

	return closeDurableDirs(steps, nil)
}

func closeDurableDirs(steps []durableDirStep, operationErr error) error {
	var closeErr error

	for i := len(steps) - 1; i >= 0; i-- {
		if err := steps[i].dir.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("closing dir %s: %w", steps[i].path, err)
		}
	}

	if operationErr != nil {
		return operationErr
	}

	return closeErr
}

func (d *rootedDurableDir) OpenRoot(name string) (durableDir, error) {
	root, err := d.root.OpenRoot(name)
	if err != nil {
		return nil, err
	}

	return &rootedDurableDir{root: root}, nil
}

func (d *rootedDurableDir) Mkdir(name string, mode os.FileMode) error {
	return d.root.Mkdir(name, mode)
}

func (d *rootedDurableDir) Sync() error {
	f, err := d.root.Open(".")
	if err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return err
	}

	return f.Close()
}

func (d *rootedDurableDir) Close() error {
	return d.root.Close()
}
