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

func renameRootedDurably(oldRoot *os.Root, oldName string, newRoot *os.Root, newName string) error {
	same, err := sameRootedDirectory(oldRoot, newRoot)
	if err != nil {
		return err
	}

	if !same {
		return fmt.Errorf("rooted atomic rename crosses directories: %w", ErrNonRegularArchiveArtifact)
	}

	return oldRoot.Rename(oldName, newName)
}

func sameRootedDirectory(left, right *os.Root) (bool, error) {
	leftFile, err := left.Open(".")
	if err != nil {
		return false, err
	}

	defer func() { _ = leftFile.Close() }()

	rightFile, err := right.Open(".")
	if err != nil {
		return false, err
	}

	defer func() { _ = rightFile.Close() }()

	leftInfo, err := leftFile.Stat()
	if err != nil {
		return false, err
	}

	rightInfo, err := rightFile.Stat()
	if err != nil {
		return false, err
	}

	return os.SameFile(leftInfo, rightInfo), nil
}

func syncRootedDirectory(root *os.Root) error {
	file, err := root.Open(".")
	if err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()

		return err
	}

	return file.Close()
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

// ensureDirAt rolls one parent and child handle through the path. A parent is
// synced before it is released, so every child entry is confirmed through the
// exact descriptor that created or opened it without retaining descriptors in
// proportion to path depth. Every call confirms the full chain because an
// existing component may be residue from a previous process that failed before
// syncing its parent.
func ensureDirAt(root durableDir, rootPath string, components []string) error {
	parent := durableDirStep{dir: root, path: rootPath}

	for _, component := range components {
		childPath := filepath.Join(parent.path, component)

		child, err := parent.dir.OpenRoot(component)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return closeDurableDir(parent, nil, fmt.Errorf("opening dir %s: %w", childPath, err))
		}

		if errors.Is(err, os.ErrNotExist) {
			if mkdirErr := parent.dir.Mkdir(component, 0o755); mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
				return closeDurableDir(parent, nil, fmt.Errorf("creating dir %s: %w", childPath, mkdirErr))
			}

			child, err = parent.dir.OpenRoot(component)
			if err != nil {
				return closeDurableDir(parent, nil, fmt.Errorf("opening created dir %s: %w", childPath, err))
			}
		}

		childStep := durableDirStep{dir: child, path: childPath}

		if err := parent.dir.Sync(); err != nil {
			syncErr := fmt.Errorf("syncing dir %s: %w", parent.path, err)

			return closeDurableDir(parent, &childStep, syncErr)
		}

		if err := parent.dir.Close(); err != nil {
			return closeDurableDir(childStep, nil, fmt.Errorf("closing dir %s: %w", parent.path, err))
		}

		parent = childStep
	}

	if err := parent.dir.Sync(); err != nil {
		return closeDurableDir(parent, nil, fmt.Errorf("syncing dir %s: %w", parent.path, err))
	}

	return closeDurableDir(parent, nil, nil)
}

func closeDurableDir(primary durableDirStep, secondary *durableDirStep, operationErr error) error {
	var secondaryCloseErr error

	if secondary != nil {
		if err := secondary.dir.Close(); err != nil {
			secondaryCloseErr = fmt.Errorf("closing dir %s: %w", secondary.path, err)
		}
	}

	primaryCloseErr := primary.dir.Close()
	if operationErr != nil || secondaryCloseErr != nil || primaryCloseErr != nil {
		return errors.Join(
			operationErr,
			secondaryCloseErr,
			wrapDurableDirCloseError(primary.path, primaryCloseErr),
		)
	}

	return nil
}

func wrapDurableDirCloseError(path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("closing dir %s: %w", path, err)
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
