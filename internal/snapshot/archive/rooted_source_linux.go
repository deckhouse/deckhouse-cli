//go:build linux

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
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const archiveResolveFlags = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV

type archiveOpenat2Func func(dirfd int, path string, how *unix.OpenHow) (int, error)
type archiveOpenatFunc func(dirfd int, path string, flags int, mode uint32) (int, error)
type archiveMountIDFunc func(fd int) (uint64, error)

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
	return openArchiveAtLinux(parent, name, path, true, unix.Openat2, unix.Openat, readLinuxMountID)
}

func openArchiveRegularAt(parent *os.File, name, path string) (*os.File, error) {
	return openArchiveAtLinux(parent, name, path, false, unix.Openat2, unix.Openat, readLinuxMountID)
}

func verifyArchiveLockMount(parent, lock *os.File, path string) error {
	return verifyLinuxArchiveMount(parent, lock, path, readLinuxMountID)
}

func openArchiveAtLinux(
	parent *os.File,
	name string,
	path string,
	wantDir bool,
	openat2 archiveOpenat2Func,
	openat archiveOpenatFunc,
	mountID archiveMountIDFunc,
) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if wantDir {
		flags |= unix.O_DIRECTORY
	}

	how := &unix.OpenHow{
		Flags:   uint64(flags),
		Resolve: archiveResolveFlags,
	}

	fd, err := openat2(int(parent.Fd()), name, how)
	if err == nil {
		return finishLinuxArchiveOpen(fd, path, wantDir)
	}

	if !errors.Is(err, unix.ENOSYS) {
		return nil, classifyLinuxArchiveOpenError(path, wantDir, err)
	}

	fd, err = openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return nil, classifyLinuxArchiveOpenError(path, wantDir, err)
	}

	file, err := finishLinuxArchiveOpen(fd, path, wantDir)
	if err != nil {
		return nil, err
	}

	if err := verifyLinuxArchiveMount(parent, file, path, mountID); err != nil {
		_ = file.Close()

		return nil, err
	}

	return file, nil
}

func finishLinuxArchiveOpen(fd int, path string, wantDir bool) (*os.File, error) {
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("open archive path %s: invalid descriptor", path)
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

func verifyLinuxArchiveMount(parent, child *os.File, path string, mountID archiveMountIDFunc) error {
	parentID, err := mountID(int(parent.Fd()))
	if err != nil {
		return fmt.Errorf("identify parent mount for archive path %s: %w",
			path, errors.Join(ErrArchiveMountBoundaryUnsupported, err))
	}

	childID, err := mountID(int(child.Fd()))
	if err != nil {
		return fmt.Errorf("identify opened mount for archive path %s: %w",
			path, errors.Join(ErrArchiveMountBoundaryUnsupported, err))
	}

	if parentID != childID {
		return fmt.Errorf("%s crosses an archive mount boundary: %w", path, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func readLinuxMountID(fd int) (uint64, error) {
	path := fmt.Sprintf("/proc/self/fdinfo/%d", fd)

	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}

	defer func() { _ = file.Close() }()

	id, err := parseLinuxMountID(file)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}

	return id, nil
}

func parseLinuxMountID(reader io.Reader) (uint64, error) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), ":")
		if !found || key != "mnt_id" {
			continue
		}

		id, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse mount ID %q: %w", value, err)
		}

		return id, nil
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan mount metadata: %w", err)
	}

	return 0, errors.New("mount ID is absent")
}

func classifyLinuxArchiveOpenError(path string, wantDir bool, openErr error) error {
	if errors.Is(openErr, unix.EXDEV) {
		return fmt.Errorf("%s crosses an archive mount boundary: %w", path, ErrNonRegularArchiveArtifact)
	}

	if errors.Is(openErr, unix.ELOOP) {
		return fmt.Errorf("%s resolves through a link: %w", path, ErrNonRegularArchiveArtifact)
	}

	return classifyArchiveOpenError(path, wantDir, openErr)
}
