//go:build unix && !linux

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
	"reflect"

	"golang.org/x/sys/unix"
)

func openArchiveRoot(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, classifyArchiveOpenError(path, true, err)
	}

	dir := os.NewFile(uintptr(fd), path)
	if dir == nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("open archive root %s: invalid directory descriptor", path)
	}

	return dir, nil
}

func openArchiveDirectoryAt(parent *os.File, name, path string) (*os.File, error) {
	return openArchiveAt(parent, name, path, true)
}

func openArchiveRegularAt(parent *os.File, name, path string) (*os.File, error) {
	return openArchiveAt(parent, name, path, false)
}

func openArchiveAt(parent *os.File, name, path string, wantDir bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if wantDir {
		flags |= unix.O_DIRECTORY
	}

	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return nil, classifyArchiveOpenError(path, wantDir, err)
	}

	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("open archive path %s: invalid descriptor", path)
	}

	if err := verifySameArchiveMount(parent, file, path); err != nil {
		_ = file.Close()

		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("inspect opened archive path %s: %w", path, err)
	}

	if !archiveModeMatches(info.Mode(), wantDir) {
		_ = file.Close()

		return nil, archiveModeError(path, info.Mode(), wantDir)
	}

	return file, nil
}

func verifySameArchiveMount(parent, child *os.File, path string) error {
	parentIdentity, err := archiveMountIdentity(parent)
	if err != nil {
		return fmt.Errorf("identify parent mount for archive path %s: %w", path, err)
	}

	childIdentity, err := archiveMountIdentity(child)
	if err != nil {
		return fmt.Errorf("identify opened mount for archive path %s: %w", path, err)
	}

	if !reflect.DeepEqual(parentIdentity, childIdentity) {
		return fmt.Errorf("%s crosses an archive mount boundary: %w", path, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func archiveMountIdentity(file *os.File) (any, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return nil, errors.Join(ErrArchiveMountBoundaryUnsupported, err)
	}

	value := reflect.ValueOf(stat)
	for _, name := range []string{"Fsid", "F_fsid"} {
		field := value.FieldByName(name)
		if field.IsValid() && field.CanInterface() {
			return field.Interface(), nil
		}
	}

	return nil, fmt.Errorf("statfs exposes no filesystem identity: %w", ErrArchiveMountBoundaryUnsupported)
}
