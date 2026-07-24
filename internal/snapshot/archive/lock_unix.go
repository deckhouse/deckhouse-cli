//go:build linux || darwin || freebsd || openbsd

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

	"golang.org/x/sys/unix"
)

func openArchiveLockAnchor(source *RootedSource) (*os.File, error) {
	fd, err := unix.Openat(
		int(source.dir.Fd()),
		"..",
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, err
	}

	anchor := os.NewFile(uintptr(fd), source.path+" namespace anchor")
	if anchor == nil {
		_ = unix.Close(fd)

		return nil, errors.New("open archive namespace anchor: invalid descriptor")
	}

	anchorInfo, err := anchor.Stat()
	if err != nil {
		_ = anchor.Close()

		return nil, fmt.Errorf("inspect archive namespace anchor: %w", err)
	}

	rootInfo, err := source.dir.Stat()
	if err != nil {
		_ = anchor.Close()

		return nil, fmt.Errorf("inspect archive root for namespace anchor: %w", err)
	}

	if os.SameFile(anchorInfo, rootInfo) {
		if err := anchor.Close(); err != nil {
			return nil, fmt.Errorf("close duplicate archive namespace anchor: %w", err)
		}

		return nil, nil
	}

	return anchor, nil
}

func openArchiveLockAt(parent *os.File, name, path string) (*os.File, error) {
	var stat unix.Stat_t

	err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil && stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, fmt.Errorf("%s is not a regular lock entry (mode %#o): %w",
			path, stat.Mode&unix.S_IFMT, ErrNonRegularArchiveArtifact)
	}

	if err != nil && !errors.Is(err, unix.ENOENT) {
		return nil, classifyArchiveOpenError(path, false, err)
	}

	flags := unix.O_RDWR | unix.O_CREAT | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK

	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0o600)
	if err != nil {
		return nil, classifyArchiveOpenError(path, false, err)
	}

	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("open archive lock %s: invalid descriptor", path)
	}

	if err := verifyArchiveLockMount(parent, file, path); err != nil {
		_ = file.Close()

		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("inspect archive lock %s: %w", path, err)
	}

	if !info.Mode().IsRegular() {
		_ = file.Close()

		return nil, archiveModeError(path, info.Mode(), false)
	}

	return file, nil
}

func tryArchiveAnchorLock(anchor *os.File, exclusive bool) (bool, error) {
	if anchor == nil {
		return true, nil
	}

	return tryUnixArchiveLock(anchor, exclusive)
}

func tryArchiveRootLock(root *os.File, exclusive bool) (bool, error) {
	return tryUnixArchiveLock(root, exclusive)
}

func tryArchiveFileLock(file *os.File, exclusive bool) (bool, error) {
	return tryUnixArchiveLock(file, exclusive)
}

func tryUnixArchiveLock(file *os.File, exclusive bool) (bool, error) {
	operation := unix.LOCK_SH | unix.LOCK_NB
	if exclusive {
		operation = unix.LOCK_EX | unix.LOCK_NB
	}

	err := unix.Flock(int(file.Fd()), operation)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func unlockArchiveAnchorLock(anchor *os.File) error {
	if anchor == nil {
		return nil
	}

	return unix.Flock(int(anchor.Fd()), unix.LOCK_UN)
}

func closeArchiveLockAnchor(anchor *os.File) error {
	if anchor == nil {
		return nil
	}

	return anchor.Close()
}

func unlockArchiveFileLock(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}

func unlockArchiveRootLock(root *os.File) error {
	return unix.Flock(int(root.Fd()), unix.LOCK_UN)
}
