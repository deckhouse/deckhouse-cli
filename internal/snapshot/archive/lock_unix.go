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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	"golang.org/x/sys/unix"
)

const archiveLockCarrierPath = "/dev/zero"

type archiveLockCarrierRange struct {
	readers int
	writer  bool
}

type archiveLockAnchorState struct {
	lockKeys  []int64
	exclusive bool
}

var archiveLockCarrier = struct {
	sync.Mutex
	file    *os.File
	ranges  map[int64]archiveLockCarrierRange
	anchors map[*os.File]*archiveLockAnchorState
}{}

func openArchiveLockAnchor(source *RootedSource) (*os.File, error) {
	pathIdentity, rootIdentity, err := archiveLockDomainIdentity(source)
	if err != nil {
		return nil, err
	}

	domainPath := archiveLockDomainPathForIdentity(pathIdentity)
	domainDirectory := filepath.Dir(domainPath)

	directory, err := openArchiveLockDomainDirectory(domainDirectory)
	if err != nil {
		return nil, err
	}

	record, openErr := openArchiveLockAt(
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
			wrapArchiveLockCloseError(domainPath, record.Close()),
		)
	}

	archiveLockCarrier.Lock()
	defer archiveLockCarrier.Unlock()

	if archiveLockCarrier.anchors == nil {
		archiveLockCarrier.anchors = make(map[*os.File]*archiveLockAnchorState)
	}

	archiveLockCarrier.anchors[record] = &archiveLockAnchorState{
		lockKeys: archiveLockDomainKeys(pathIdentity, rootIdentity),
	}

	return record, nil
}

func openArchiveLockDomainDirectory(path string) (*os.File, error) {
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open archive lock domain directory %s: %w", path, err)
	}

	directory := os.NewFile(uintptr(fd), path)
	if directory == nil {
		_ = unix.Close(fd)

		return nil, errors.New("open archive lock domain directory: invalid descriptor")
	}

	info, err := directory.Stat()
	if err != nil {
		_ = directory.Close()

		return nil, fmt.Errorf("inspect archive lock domain directory %s: %w", path, err)
	}

	if !info.IsDir() {
		_ = directory.Close()

		return nil, archiveModeError(path, info.Mode(), true)
	}

	return directory, nil
}

// archiveLockDomainPath maps aliases that resolve to one archive root to the same record name.
// The record is diagnostic binding state; byte-range locks on a system-owned device are the
// non-replaceable coordination namespace.
func archiveLockDomainPath(path string) (string, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve archive lock domain alias %s: %w", path, err)
	}

	absolute, err := filepath.Abs(canonical)
	if err != nil {
		return "", fmt.Errorf("resolve archive lock domain path %s: %w", path, err)
	}

	return archiveLockDomainPathForIdentity(filepath.Clean(absolute)), nil
}

func archiveLockDomainPathForIdentity(identity string) string {
	sum := sha256.Sum256([]byte(identity))

	return filepath.Join(
		archiveLockDomainDirectory(),
		fmt.Sprintf(".d8-snapshot-archive-lock-%d-%x", os.Getuid(), sum),
	)
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
	canonical, err := filepath.EvalSymlinks(source.path)
	if err != nil {
		return "", "", fmt.Errorf("resolve archive lock identity alias %s: %w", source.path, err)
	}

	absolute, err := filepath.Abs(canonical)
	if err != nil {
		return "", "", fmt.Errorf("resolve archive lock identity path %s: %w", source.path, err)
	}

	var stat unix.Stat_t
	if err := unix.Fstat(int(source.dir.Fd()), &stat); err != nil {
		return "", "", fmt.Errorf("identify pinned archive root %s: %w", source.path, err)
	}

	canonicalInfo, err := os.Stat(absolute)
	if err != nil {
		return "", "", fmt.Errorf("inspect canonical archive root %s: %w", absolute, err)
	}

	pinnedInfo, err := source.dir.Stat()
	if err != nil {
		return "", "", fmt.Errorf("inspect pinned archive root %s: %w", source.path, err)
	}

	if !os.SameFile(canonicalInfo, pinnedInfo) {
		return "", "", fmt.Errorf("canonical archive root %s changed during lock acquisition: %w",
			source.path, ErrArchiveLockChanged)
	}

	return filepath.Clean(absolute), fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}

func archiveLockDomainKeys(pathIdentity, rootIdentity string) []int64 {
	keys := []int64{
		archiveLockDomainKey("path\x00" + pathIdentity),
		archiveLockDomainKey("root\x00" + rootIdentity),
	}
	slices.Sort(keys)

	if keys[0] == keys[1] {
		return keys[:1]
	}

	return keys
}

func archiveLockDomainKey(identity string) int64 {
	sum := sha256.Sum256([]byte(identity))

	key := int64(binary.LittleEndian.Uint64(sum[:8]) & (1<<63 - 1))
	if key == 0 {
		return 1
	}

	return key
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
			return tryArchiveCarrierLock(anchor, lockExclusive)
		},
		func() error {
			return unlockArchiveCarrierLock(anchor)
		},
	)
}

func tryArchiveCarrierLock(anchor *os.File, exclusive bool) (bool, error) {
	archiveLockCarrier.Lock()
	defer archiveLockCarrier.Unlock()

	anchorState, found := archiveLockCarrier.anchors[anchor]
	if !found {
		return false, errors.New("lock stable archive carrier: anchor is not registered")
	}

	carrier, err := openArchiveLockCarrier()
	if err != nil {
		return false, err
	}

	acquired := make([]int64, 0, len(anchorState.lockKeys))
	for _, key := range anchorState.lockKeys {
		state := archiveLockCarrier.ranges[key]
		if state.writer || exclusive && state.readers > 0 {
			rollbackArchiveCarrierRanges(carrier, acquired, exclusive)

			return false, closeIdleArchiveLockCarrier()
		}

		if state.readers == 0 && !state.writer {
			locked, lockErr := setArchiveCarrierRangeLock(carrier, key, exclusive)
			if lockErr != nil {
				rollbackArchiveCarrierRanges(carrier, acquired, exclusive)

				return false, errors.Join(lockErr, closeIdleArchiveLockCarrier())
			}

			if !locked {
				rollbackArchiveCarrierRanges(carrier, acquired, exclusive)

				return false, closeIdleArchiveLockCarrier()
			}
		}

		if exclusive {
			state.writer = true
		} else {
			state.readers++
		}

		archiveLockCarrier.ranges[key] = state
		acquired = append(acquired, key)
	}

	anchorState.exclusive = exclusive

	return true, nil
}

func openArchiveLockCarrier() (*os.File, error) {
	if archiveLockCarrier.file != nil {
		return archiveLockCarrier.file, nil
	}

	fd, err := unix.Open(
		archiveLockCarrierPath,
		unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open stable archive lock carrier %s: %w", archiveLockCarrierPath, err)
	}

	carrier := os.NewFile(uintptr(fd), archiveLockCarrierPath)
	if carrier == nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("open stable archive lock carrier %s: invalid descriptor", archiveLockCarrierPath)
	}

	info, err := carrier.Stat()
	if err != nil {
		_ = carrier.Close()

		return nil, fmt.Errorf("inspect stable archive lock carrier %s: %w", archiveLockCarrierPath, err)
	}

	if info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice == 0 {
		_ = carrier.Close()

		return nil, fmt.Errorf("%s is not a character-device lock carrier (mode %s): %w",
			archiveLockCarrierPath, info.Mode(), ErrNonRegularArchiveArtifact)
	}

	archiveLockCarrier.file = carrier
	archiveLockCarrier.ranges = make(map[int64]archiveLockCarrierRange)

	return carrier, nil
}

func setArchiveCarrierRangeLock(carrier *os.File, key int64, exclusive bool) (bool, error) {
	lockType := int16(unix.F_RDLCK)
	if exclusive {
		lockType = unix.F_WRLCK
	}

	lock := unix.Flock_t{
		Type:   lockType,
		Whence: int16(ioSeekStart),
		Start:  key,
		Len:    1,
	}

	err := unix.FcntlFlock(carrier.Fd(), unix.F_SETLK, &lock)
	if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("lock stable archive coordination range %d: %w", key, err)
	}

	return true, nil
}

const ioSeekStart = 0

func rollbackArchiveCarrierRanges(carrier *os.File, keys []int64, exclusive bool) {
	for index := len(keys) - 1; index >= 0; index-- {
		_ = releaseArchiveCarrierRange(carrier, keys[index], exclusive)
	}
}

func unlockArchiveCarrierLock(anchor *os.File) error {
	archiveLockCarrier.Lock()
	defer archiveLockCarrier.Unlock()

	anchorState, found := archiveLockCarrier.anchors[anchor]
	if !found {
		return errors.New("release stable archive lock carrier: anchor is not registered")
	}

	if archiveLockCarrier.file == nil {
		return errors.New("release stable archive lock carrier: carrier is not open")
	}

	var result error
	for index := len(anchorState.lockKeys) - 1; index >= 0; index-- {
		result = errors.Join(
			result,
			releaseArchiveCarrierRange(
				archiveLockCarrier.file,
				anchorState.lockKeys[index],
				anchorState.exclusive,
			),
		)
	}

	return errors.Join(result, closeIdleArchiveLockCarrier())
}

func closeIdleArchiveLockCarrier() error {
	if len(archiveLockCarrier.ranges) != 0 || archiveLockCarrier.file == nil {
		return nil
	}

	err := archiveLockCarrier.file.Close()
	archiveLockCarrier.file = nil
	archiveLockCarrier.ranges = nil

	if err != nil {
		return fmt.Errorf("close stable archive lock carrier: %w", err)
	}

	return nil
}

func releaseArchiveCarrierRange(carrier *os.File, key int64, exclusive bool) error {
	state, found := archiveLockCarrier.ranges[key]
	if !found {
		return fmt.Errorf("release stable archive coordination range %d: lock is not held", key)
	}

	if exclusive {
		state.writer = false
	} else {
		state.readers--
	}

	if state.readers > 0 || state.writer {
		archiveLockCarrier.ranges[key] = state

		return nil
	}

	lock := unix.Flock_t{
		Type:   unix.F_UNLCK,
		Whence: int16(ioSeekStart),
		Start:  key,
		Len:    1,
	}
	if err := unix.FcntlFlock(carrier.Fd(), unix.F_SETLK, &lock); err != nil {
		return fmt.Errorf("unlock stable archive coordination range %d: %w", key, err)
	}

	delete(archiveLockCarrier.ranges, key)

	return nil
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

	return unlockArchiveCarrierLock(anchor)
}

func closeArchiveLockAnchor(anchor *os.File) error {
	if anchor == nil {
		return nil
	}

	archiveLockCarrier.Lock()
	delete(archiveLockCarrier.anchors, anchor)
	archiveLockCarrier.Unlock()

	return anchor.Close()
}

func unlockArchiveFileLock(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}

func unlockArchiveRootLock(root *os.File) error {
	return unix.Flock(int(root.Fd()), unix.LOCK_UN)
}
