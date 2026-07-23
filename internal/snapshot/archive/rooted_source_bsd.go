//go:build unix

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

type archiveMountStatFunc func(fd int) (any, error)

type archiveMountIdentity struct {
	fsID       [2]int32
	mountPoint string
	source     string
	fsType     string
}

func openArchiveRootUnix(path string) (*os.File, error) {
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

func openArchiveAtUnix(
	parent *os.File,
	name string,
	path string,
	wantDir bool,
	mountStat archiveMountStatFunc,
) (*os.File, error) {
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

	if err := verifySameArchiveMount(parent, file, path, mountStat); err != nil {
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

func verifySameArchiveMount(
	parent *os.File,
	child *os.File,
	path string,
	mountStat archiveMountStatFunc,
) error {
	parentIdentity, err := archiveMountIdentityForFile(parent, mountStat)
	if err != nil {
		return fmt.Errorf("identify parent mount for archive path %s: %w", path, err)
	}

	childIdentity, err := archiveMountIdentityForFile(child, mountStat)
	if err != nil {
		return fmt.Errorf("identify opened mount for archive path %s: %w", path, err)
	}

	return verifyArchiveMountIdentities(parentIdentity, childIdentity, path)
}

func archiveMountIdentityForFile(file *os.File, mountStat archiveMountStatFunc) (archiveMountIdentity, error) {
	stat, err := mountStat(int(file.Fd()))
	if err != nil {
		return archiveMountIdentity{}, errors.Join(ErrArchiveMountBoundaryUnsupported, err)
	}

	return archiveMountIdentityFromStat(stat)
}

func archiveMountIdentityFromStat(stat any) (archiveMountIdentity, error) {
	value := reflect.ValueOf(stat)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return archiveMountIdentity{}, fmt.Errorf("nil descriptor mount statistics: %w",
				ErrArchiveMountBoundaryUnsupported)
		}

		value = value.Elem()
	}

	if value.Kind() != reflect.Struct {
		return archiveMountIdentity{}, fmt.Errorf("invalid descriptor mount statistics: %w",
			ErrArchiveMountBoundaryUnsupported)
	}

	fsID, err := archiveFSID(value)
	if err != nil {
		return archiveMountIdentity{}, err
	}

	mountPoint, err := archiveMountField(value, "Mntonname", "F_mntonname")
	if err != nil {
		return archiveMountIdentity{}, fmt.Errorf("read mounted-on identity: %w", err)
	}

	source, err := archiveMountField(value, "Mntfromname", "F_mntfromname")
	if err != nil {
		return archiveMountIdentity{}, fmt.Errorf("read mounted-from identity: %w", err)
	}

	fsType, err := archiveMountField(value, "Fstypename", "F_fstypename")
	if err != nil {
		return archiveMountIdentity{}, fmt.Errorf("read filesystem-type identity: %w", err)
	}

	identity := archiveMountIdentity{
		fsID:       fsID,
		mountPoint: mountPoint,
		source:     source,
		fsType:     fsType,
	}
	if err := validateArchiveMountIdentity(identity); err != nil {
		return archiveMountIdentity{}, err
	}

	return identity, nil
}

func archiveFSID(stat reflect.Value) ([2]int32, error) {
	field := archiveStructField(stat, "Fsid", "F_fsid", "Fsidx")
	if !field.IsValid() {
		return [2]int32{}, fmt.Errorf("descriptor mount statistics expose no filesystem ID: %w",
			ErrArchiveMountBoundaryUnsupported)
	}

	if field.Kind() != reflect.Struct {
		return [2]int32{}, fmt.Errorf("descriptor filesystem ID has invalid shape: %w",
			ErrArchiveMountBoundaryUnsupported)
	}

	values := archiveStructField(field, "Val", "X__fsid_val")
	if !values.IsValid() || values.Kind() != reflect.Array || values.Len() != 2 {
		return [2]int32{}, fmt.Errorf("descriptor filesystem ID has invalid values: %w",
			ErrArchiveMountBoundaryUnsupported)
	}

	var fsID [2]int32
	for index := range fsID {
		value := values.Index(index)
		if value.Kind() < reflect.Int || value.Kind() > reflect.Int64 {
			return [2]int32{}, fmt.Errorf("descriptor filesystem ID value is malformed: %w",
				ErrArchiveMountBoundaryUnsupported)
		}

		number := value.Int()
		if number < -1<<31 || number > 1<<31-1 {
			return [2]int32{}, fmt.Errorf("descriptor filesystem ID value is out of range: %w",
				ErrArchiveMountBoundaryUnsupported)
		}

		fsID[index] = int32(number)
	}

	return fsID, nil
}

func archiveMountField(stat reflect.Value, names ...string) (string, error) {
	field := archiveStructField(stat, names...)
	if !field.IsValid() || field.Kind() != reflect.Array {
		return "", fmt.Errorf("descriptor mount statistics expose no %s field: %w",
			names[0], ErrArchiveMountBoundaryUnsupported)
	}

	bytes := make([]byte, field.Len())
	for index := range bytes {
		value := field.Index(index)
		switch value.Kind() {
		case reflect.Int8:
			bytes[index] = byte(value.Int())
		case reflect.Uint8:
			bytes[index] = byte(value.Uint())
		default:
			return "", fmt.Errorf("descriptor mount field %s has invalid element type: %w",
				names[0], ErrArchiveMountBoundaryUnsupported)
		}
	}

	for index, value := range bytes {
		if value == 0 {
			return string(bytes[:index]), nil
		}
	}

	return "", fmt.Errorf("descriptor mount field %s is not terminated: %w",
		names[0], ErrArchiveMountBoundaryUnsupported)
}

func archiveStructField(value reflect.Value, names ...string) reflect.Value {
	for _, name := range names {
		field := value.FieldByName(name)
		if field.IsValid() {
			return field
		}
	}

	return reflect.Value{}
}

func validateArchiveMountIdentity(identity archiveMountIdentity) error {
	if identity.fsID == [2]int32{} {
		return fmt.Errorf("descriptor filesystem ID is zero: %w", ErrArchiveMountBoundaryUnsupported)
	}

	if identity.mountPoint == "" {
		return fmt.Errorf("descriptor mount point is empty: %w", ErrArchiveMountBoundaryUnsupported)
	}

	if identity.fsType == "" {
		return fmt.Errorf("descriptor filesystem type is empty: %w", ErrArchiveMountBoundaryUnsupported)
	}

	return nil
}

func verifyArchiveMountIdentities(parent, child archiveMountIdentity, path string) error {
	if err := validateArchiveMountIdentity(parent); err != nil {
		return fmt.Errorf("validate parent mount identity: %w", err)
	}

	if err := validateArchiveMountIdentity(child); err != nil {
		return fmt.Errorf("validate opened mount identity: %w", err)
	}

	if parent != child {
		return fmt.Errorf("%s crosses an archive mount boundary: %w", path, ErrNonRegularArchiveArtifact)
	}

	return nil
}
