//go:build windows

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
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openArchiveLockAnchor(source *RootedSource) (*os.File, error) {
	pathIdentity, _, err := archiveLockDomainIdentity(source)
	if err != nil {
		return nil, err
	}

	domainPath, err := archiveLockDomainPathForIdentity(pathIdentity)
	if err != nil {
		return nil, err
	}

	directory, err := openArchiveRoot(filepath.Dir(domainPath))
	if err != nil {
		return nil, fmt.Errorf("open archive lock domain directory: %w", err)
	}

	anchor, openErr := openArchiveLockAt(
		directory,
		filepath.Base(domainPath),
		domainPath,
	)
	closeErr := directory.Close()

	if openErr != nil {
		return nil, errors.Join(openErr, wrapArchiveLockCloseError(filepath.Dir(domainPath), closeErr))
	}

	if closeErr != nil {
		return nil, errors.Join(
			wrapArchiveLockCloseError(filepath.Dir(domainPath), closeErr),
			wrapArchiveLockCloseError(domainPath, anchor.Close()),
		)
	}

	return anchor, nil
}

// archiveLockDomainPath resolves aliases through a root handle before selecting the external
// domain. The opened domain handle denies delete sharing and is therefore the stable namespace
// entry while any reader or writer holds it.
func archiveLockDomainPath(path string) (string, error) {
	root, err := openArchiveRoot(path)
	if err != nil {
		return "", fmt.Errorf("open archive root for lock-domain identity %s: %w", path, err)
	}
	defer func() { _ = root.Close() }()

	identity, err := archiveLockCanonicalHandlePath(root, path)
	if err != nil {
		return "", err
	}

	return archiveLockDomainPathForIdentity(identity)
}

func archiveLockDomainPathForIdentity(identity string) (string, error) {
	domainDirectory, err := windows.KnownFolderPath(
		windows.FOLDERID_LocalAppData,
		windows.KF_FLAG_DEFAULT,
	)
	if err != nil {
		return "", fmt.Errorf("resolve Windows archive lock domain directory: %w", err)
	}

	sum := sha256.Sum256([]byte(identity))

	return filepath.Join(
		domainDirectory,
		fmt.Sprintf(".d8-snapshot-archive-lock-%x", sum),
	), nil
}

func openArchiveLockAt(parent *os.File, name, path string) (*os.File, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, fmt.Errorf("encode archive lock path %s: %w", path, err)
	}

	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	options := uint32(windows.FILE_NON_DIRECTORY_FILE |
		windows.FILE_OPEN_REPARSE_POINT |
		windows.FILE_SYNCHRONOUS_IO_NONALERT)

	var handle windows.Handle

	err = windows.NtCreateFile(
		&handle,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.SYNCHRONIZE,
		attributes,
		&windows.IO_STATUS_BLOCK{},
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_OPEN_IF,
		options,
		0,
		0,
	)
	if err != nil {
		return nil, classifyWindowsArchiveOpenError(path, false, err)
	}

	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)

		return nil, fmt.Errorf("open archive lock %s: invalid handle", path)
	}

	if err := rejectWindowsArchiveReparsePoint(file, path); err != nil {
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
	path, err := archiveLockCanonicalHandlePath(source.dir, source.path)
	if err != nil {
		return "", "", err
	}

	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(source.dir.Fd()), &info); err != nil {
		return "", "", fmt.Errorf("identify pinned archive root %s: %w", source.path, err)
	}

	rootID := fmt.Sprintf(
		"%d:%d:%d",
		info.VolumeSerialNumber,
		info.FileIndexHigh,
		info.FileIndexLow,
	)

	return path, rootID, nil
}

func archiveLockCanonicalHandlePath(file *os.File, displayPath string) (string, error) {
	const (
		fileNameNormalized = 0
		volumeNameGUID     = 1
	)

	buffer := make([]uint16, windows.MAX_LONG_PATH)
	for {
		length, err := windows.GetFinalPathNameByHandle(
			windows.Handle(file.Fd()),
			&buffer[0],
			uint32(len(buffer)),
			fileNameNormalized|volumeNameGUID,
		)
		if err != nil {
			return "", fmt.Errorf("resolve canonical archive lock path %s: %w", displayPath, err)
		}

		if int(length) < len(buffer) {
			return strings.ToLower(windows.UTF16ToString(buffer[:length])), nil
		}

		buffer = make([]uint16, int(length)+1)
	}
}

func tryArchiveAnchorLock(anchor *os.File, source *RootedSource, exclusive bool) (bool, error) {
	return bindArchiveLockDomain(
		anchor,
		source,
		exclusive,
		func(lockExclusive bool) (bool, error) {
			return tryWindowsArchiveLock(anchor, lockExclusive)
		},
		func() error {
			return unlockWindowsArchiveLock(anchor)
		},
	)
}

func tryArchiveRootLock(_ *os.File, _ bool) (bool, error) {
	// openArchiveRoot denies delete sharing while the rooted view is held.
	return true, nil
}

func tryArchiveFileLock(file *os.File, exclusive bool) (bool, error) {
	return tryWindowsArchiveLock(file, exclusive)
}

func tryWindowsArchiveLock(file *os.File, exclusive bool) (bool, error) {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}

	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		flags,
		0,
		1,
		0,
		&windows.Overlapped{},
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func unlockArchiveAnchorLock(anchor *os.File) error {
	return unlockWindowsArchiveLock(anchor)
}

func closeArchiveLockAnchor(anchor *os.File) error {
	return anchor.Close()
}

func unlockArchiveFileLock(file *os.File) error {
	return unlockWindowsArchiveLock(file)
}

func unlockWindowsArchiveLock(file *os.File) error {
	return windows.UnlockFileEx(
		windows.Handle(file.Fd()),
		0,
		1,
		0,
		&windows.Overlapped{},
	)
}

func unlockArchiveRootLock(_ *os.File) error {
	return nil
}
