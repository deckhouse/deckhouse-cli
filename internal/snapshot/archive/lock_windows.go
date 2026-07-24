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
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openArchiveLockAnchor(_ *RootedSource) (*os.File, error) {
	// The root handle and lock-entry handle both deny delete sharing. Windows therefore
	// enforces the namespace anchor directly instead of requiring a parent-directory lock.
	return nil, nil
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

func tryArchiveAnchorLock(_ *os.File, _ bool) (bool, error) {
	return true, nil
}

func tryArchiveRootLock(_ *os.File, _ bool) (bool, error) {
	// openArchiveRoot denies delete sharing while the rooted view is held. That prevents a
	// cooperating Windows contender from replacing the directory identity before lock release.
	return true, nil
}

func tryArchiveFileLock(file *os.File, exclusive bool) (bool, error) {
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

func unlockArchiveAnchorLock(_ *os.File) error {
	return nil
}

func closeArchiveLockAnchor(_ *os.File) error {
	return nil
}

func unlockArchiveFileLock(file *os.File) error {
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
