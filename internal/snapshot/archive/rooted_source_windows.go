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
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsFileAttributeTagInfo struct {
	fileAttributes uint32
	reparseTag     uint32
}

func openArchiveRoot(path string) (*os.File, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("encode archive root %s: %w", path, err)
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, classifyArchiveOpenError(path, true, err)
	}

	dir := os.NewFile(uintptr(handle), path)
	if dir == nil {
		_ = windows.CloseHandle(handle)

		return nil, fmt.Errorf("open archive root %s: invalid directory handle", path)
	}

	if err := rejectWindowsArchiveReparsePoint(dir, path); err != nil {
		_ = dir.Close()

		return nil, err
	}

	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()

		return nil, fmt.Errorf("inspect archive root %s: %w", path, err)
	}

	if !info.Mode().IsDir() {
		_ = dir.Close()

		return nil, archiveModeError(path, info.Mode(), true)
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
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, fmt.Errorf("encode archive path %s: %w", path, err)
	}

	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}

	options := uint32(windows.FILE_OPEN_FOR_BACKUP_INTENT |
		windows.FILE_OPEN_REPARSE_POINT |
		windows.FILE_SYNCHRONOUS_IO_NONALERT)
	if wantDir {
		options |= windows.FILE_DIRECTORY_FILE
	} else {
		options |= windows.FILE_NON_DIRECTORY_FILE
	}

	var handle windows.Handle

	err = windows.NtCreateFile(
		&handle,
		windows.FILE_GENERIC_READ|windows.SYNCHRONIZE,
		attributes,
		&windows.IO_STATUS_BLOCK{},
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		options,
		0,
		0,
	)
	if err != nil {
		return nil, classifyWindowsArchiveOpenError(path, wantDir, err)
	}

	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)

		return nil, fmt.Errorf("open archive path %s: invalid handle", path)
	}

	if err := rejectWindowsArchiveReparsePoint(file, path); err != nil {
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

func rejectWindowsArchiveReparsePoint(file *os.File, path string) error {
	var info windowsFileAttributeTagInfo

	err := windows.GetFileInformationByHandleEx(
		windows.Handle(file.Fd()),
		windows.FileAttributeTagInfo,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
	if err != nil {
		return fmt.Errorf("inspect reparse attributes for archive path %s: %w", path, err)
	}

	if info.fileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("%s is a Windows reparse point (tag %#x): %w",
			path, info.reparseTag, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func classifyWindowsArchiveOpenError(path string, wantDir bool, openErr error) error {
	status, ok := openErr.(windows.NTStatus)
	if ok {
		switch status {
		case windows.STATUS_REPARSE_POINT_ENCOUNTERED:
			openErr = errors.Join(syscall.ELOOP, ErrNonRegularArchiveArtifact)
		case windows.STATUS_NOT_A_DIRECTORY:
			openErr = syscall.ENOTDIR
		case windows.STATUS_FILE_IS_A_DIRECTORY:
			openErr = syscall.EISDIR
		default:
			openErr = status.Errno()
		}
	}

	return classifyArchiveOpenError(path, wantDir, openErr)
}
