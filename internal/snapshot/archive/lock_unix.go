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
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sys/unix"
)

func openArchiveLockAnchor(source *RootedSource) (*os.File, error) {
	domainPath, err := archiveLockDomainPath(source.path)
	if err != nil {
		return nil, err
	}

	domainDirectory := filepath.Dir(domainPath)

	fd, err := unix.Open(
		domainDirectory,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open archive lock domain directory %s: %w", domainDirectory, err)
	}

	directory := os.NewFile(uintptr(fd), domainDirectory)
	if directory == nil {
		_ = unix.Close(fd)

		return nil, errors.New("open archive lock domain directory: invalid descriptor")
	}

	directoryInfo, err := directory.Stat()
	if err != nil {
		_ = directory.Close()

		return nil, fmt.Errorf("inspect archive lock domain directory %s: %w", domainDirectory, err)
	}

	if !directoryInfo.IsDir() {
		_ = directory.Close()

		return nil, archiveModeError(domainDirectory, directoryInfo.Mode(), true)
	}

	anchor, openErr := openArchiveLockAt(
		directory,
		filepath.Base(domainPath),
		domainPath,
	)
	closeErr := directory.Close()

	if openErr != nil {
		return nil, errors.Join(openErr, wrapArchiveLockCloseError(domainDirectory, closeErr))
	}

	if closeErr != nil {
		return nil, errors.Join(
			wrapArchiveLockCloseError(domainDirectory, closeErr),
			wrapArchiveLockCloseError(domainPath, anchor.Close()),
		)
	}

	return anchor, nil
}

// archiveLockDomainPath maps one normalized archive pathname to one coordination inode outside
// the replaceable archive ancestry. The digest is only a bounded filename encoding: acquisition
// still binds and verifies the pinned root and its descriptor-relative in-root lock entry.
func archiveLockDomainPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve archive lock domain path %s: %w", path, err)
	}

	sum := sha256.Sum256([]byte(filepath.Clean(absolute)))

	return filepath.Join(
		archiveLockDomainDirectory(),
		fmt.Sprintf(".d8-snapshot-archive-lock-%d-%x", os.Getuid(), sum),
	), nil
}

func archiveLockDomainDirectory() string {
	if runtime.GOOS == "darwin" {
		return "/private/tmp"
	}

	return "/tmp"
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

func archiveLockDomainIdentity(source *RootedSource) (string, string, error) {
	absolute, err := filepath.Abs(source.path)
	if err != nil {
		return "", "", fmt.Errorf("resolve archive lock identity path %s: %w", source.path, err)
	}

	var stat unix.Stat_t
	if err := unix.Fstat(int(source.dir.Fd()), &stat); err != nil {
		return "", "", fmt.Errorf("identify pinned archive root %s: %w", source.path, err)
	}

	return filepath.Clean(absolute), fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}

func tryArchiveAnchorLock(anchor *os.File, source *RootedSource, exclusive bool) (bool, error) {
	if anchor == nil {
		return true, nil
	}

	return bindArchiveLockDomain(
		anchor,
		source,
		exclusive,
		func(lockExclusive bool) (bool, error) {
			return tryUnixArchiveLock(anchor, lockExclusive)
		},
		func() error {
			return unix.Flock(int(anchor.Fd()), unix.LOCK_UN)
		},
	)
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
