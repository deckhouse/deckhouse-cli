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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeDurableFS struct {
	dirs            map[string]struct{}
	files           map[string]struct{}
	events          []string
	syncFailures    map[string]error
	mkdirFailures   map[string]error
	concurrentDirs  map[string]struct{}
	concurrentFiles map[string]struct{}
	openHandles     int
	maxOpenHandles  int
}

type fakeDurableDir struct {
	fs   *fakeDurableFS
	path string
}

func newFakeDurableFS(existingDirs ...string) *fakeDurableFS {
	dirs := make(map[string]struct{}, len(existingDirs))
	for _, dir := range existingDirs {
		dirs[dir] = struct{}{}
	}

	return &fakeDurableFS{
		dirs:            dirs,
		files:           make(map[string]struct{}),
		syncFailures:    make(map[string]error),
		mkdirFailures:   make(map[string]error),
		concurrentDirs:  make(map[string]struct{}),
		concurrentFiles: make(map[string]struct{}),
	}
}

func (f *fakeDurableFS) root(path string) durableDir {
	f.openHandles++
	f.maxOpenHandles = max(f.maxOpenHandles, f.openHandles)

	return &fakeDurableDir{fs: f, path: path}
}

func (d *fakeDurableDir) OpenRoot(name string) (durableDir, error) {
	childPath := filepath.Join(d.path, name)
	d.fs.events = append(d.fs.events, "open "+childPath)

	if _, ok := d.fs.files[childPath]; ok {
		return nil, syscall.ENOTDIR
	}

	if _, ok := d.fs.dirs[childPath]; !ok {
		return nil, fs.ErrNotExist
	}

	d.fs.openHandles++
	d.fs.maxOpenHandles = max(d.fs.maxOpenHandles, d.fs.openHandles)

	return &fakeDurableDir{fs: d.fs, path: childPath}, nil
}

func (d *fakeDurableDir) Mkdir(name string, _ os.FileMode) error {
	childPath := filepath.Join(d.path, name)
	d.fs.events = append(d.fs.events, "mkdir "+childPath)

	if err := d.fs.mkdirFailures[childPath]; err != nil {
		return err
	}

	if _, ok := d.fs.concurrentDirs[childPath]; ok {
		delete(d.fs.concurrentDirs, childPath)
		d.fs.dirs[childPath] = struct{}{}

		return fs.ErrExist
	}

	if _, ok := d.fs.concurrentFiles[childPath]; ok {
		delete(d.fs.concurrentFiles, childPath)
		d.fs.files[childPath] = struct{}{}

		return fs.ErrExist
	}

	if _, ok := d.fs.dirs[childPath]; ok {
		return fs.ErrExist
	}

	if _, ok := d.fs.files[childPath]; ok {
		return fs.ErrExist
	}

	d.fs.dirs[childPath] = struct{}{}

	return nil
}

func (d *fakeDurableDir) Sync() error {
	d.fs.events = append(d.fs.events, "sync "+d.path)

	return d.fs.syncFailures[d.path]
}

func (d *fakeDurableDir) Close() error {
	d.fs.openHandles--

	return nil
}

func TestEnsureDirAtCreationAndSyncOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		existingDirs []string
		components   []string
		wantEvents   []string
	}{
		{
			name:         "several missing components",
			existingDirs: []string{"root"},
			components:   []string{"a", "b", "c"},
			wantEvents: []string{
				"open root/a", "mkdir root/a", "open root/a", "sync root",
				"open root/a/b", "mkdir root/a/b", "open root/a/b", "sync root/a",
				"open root/a/b/c", "mkdir root/a/b/c", "open root/a/b/c", "sync root/a/b",
				"sync root/a/b/c",
			},
		},
		{
			name:         "one missing leaf",
			existingDirs: []string{"root", "root/a", "root/a/b"},
			components:   []string{"a", "b", "c"},
			wantEvents: []string{
				"open root/a", "sync root",
				"open root/a/b", "sync root/a",
				"open root/a/b/c", "mkdir root/a/b/c", "open root/a/b/c", "sync root/a/b",
				"sync root/a/b/c",
			},
		},
		{
			name:         "fully existing input",
			existingDirs: []string{"root", "root/a", "root/a/b", "root/a/b/c"},
			components:   []string{"a", "b", "c"},
			wantEvents: []string{
				"open root/a", "sync root",
				"open root/a/b", "sync root/a",
				"open root/a/b/c", "sync root/a/b",
				"sync root/a/b/c",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fakeFS := newFakeDurableFS(tt.existingDirs...)
			err := ensureDirAt(fakeFS.root("root"), "root", tt.components)
			require.NoError(t, err)
			require.Equal(t, tt.wantEvents, fakeFS.events)
			require.Zero(t, fakeFS.openHandles)
			require.LessOrEqual(t, fakeFS.maxOpenHandles, 2)
		})
	}
}

func TestOpenDurableDirectoryAtRejectsNonDirectoriesWithoutFollowing(t *testing.T) {
	base := t.TempDir()
	outside := t.TempDir()
	parent, err := os.Open(base)
	require.NoError(t, err)
	defer func() { _ = parent.Close() }()

	require.NoError(t, os.WriteFile(filepath.Join(base, "regular"), []byte("data"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(base, "symlink")))

	for _, name := range []string{"regular", "symlink"} {
		t.Run(name, func(t *testing.T) {
			directory, openErr := openDurableDirectoryAt(parent, name, filepath.Join(base, name))
			if directory != nil {
				_ = directory.Close()
			}

			require.Error(t, openErr)
		})
	}
}

func TestEnsureDirAtRepeatedCallReconfirmsExistingChain(t *testing.T) {
	t.Parallel()

	fakeFS := newFakeDurableFS("root")
	components := []string{"a", "b"}

	require.NoError(t, ensureDirAt(fakeFS.root("root"), "root", components))

	fakeFS.events = nil
	require.NoError(t, ensureDirAt(fakeFS.root("root"), "root", components))
	require.Equal(t, []string{
		"open root/a", "sync root",
		"open root/a/b", "sync root/a",
		"sync root/a/b",
	}, fakeFS.events)
	require.Zero(t, fakeFS.openHandles)
	require.LessOrEqual(t, fakeFS.maxOpenHandles, 2)
}

func TestEnsureDirAtConcurrentCreator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		makeEntry func(*fakeDurableFS)
		wantErr   error
	}{
		{
			name: "directory wins race",
			makeEntry: func(fakeFS *fakeDurableFS) {
				fakeFS.concurrentDirs["root/a"] = struct{}{}
			},
		},
		{
			name: "non-directory wins race",
			makeEntry: func(fakeFS *fakeDurableFS) {
				fakeFS.concurrentFiles["root/a"] = struct{}{}
			},
			wantErr: syscall.ENOTDIR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fakeFS := newFakeDurableFS("root")
			tt.makeEntry(fakeFS)

			err := ensureDirAt(fakeFS.root("root"), "root", []string{"a"})
			if tt.wantErr == nil {
				require.NoError(t, err)
				require.Equal(t, []string{
					"open root/a", "mkdir root/a", "open root/a",
					"sync root", "sync root/a",
				}, fakeFS.events)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
				require.Equal(t, []string{
					"open root/a", "mkdir root/a", "open root/a",
				}, fakeFS.events)
			}

			require.Zero(t, fakeFS.openHandles)
			require.LessOrEqual(t, fakeFS.maxOpenHandles, 2)
		})
	}
}

func TestEnsureDirAtSyncFailureIsRetryable(t *testing.T) {
	t.Parallel()

	syncPaths := []string{"root", "root/a", "root/a/b", "root/a/b/c"}
	for _, failurePath := range syncPaths {
		t.Run(failurePath, func(t *testing.T) {
			t.Parallel()

			sentinel := errors.New("directory sync sentinel")
			fakeFS := newFakeDurableFS("root")
			fakeFS.syncFailures[failurePath] = sentinel

			err := ensureDirAt(fakeFS.root("root"), "root", []string{"a", "b", "c"})
			require.ErrorIs(t, err, sentinel)
			require.Zero(t, fakeFS.openHandles)

			delete(fakeFS.syncFailures, failurePath)
			fakeFS.events = nil

			require.NoError(t, ensureDirAt(fakeFS.root("root"), "root", []string{"a", "b", "c"}))
			require.Equal(t, []string{
				"sync root", "sync root/a", "sync root/a/b", "sync root/a/b/c",
			}, syncEvents(fakeFS.events))

			for _, dir := range []string{"root/a", "root/a/b", "root/a/b/c"} {
				_, ok := fakeFS.dirs[dir]
				require.True(t, ok, "created directory %s must remain retryable", dir)
			}

			require.Zero(t, fakeFS.openHandles)
			require.LessOrEqual(t, fakeFS.maxOpenHandles, 2)
		})
	}
}

func TestEnsureDirAtCreationFailurePreservesCause(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("mkdir sentinel")
	fakeFS := newFakeDurableFS("root")
	fakeFS.mkdirFailures["root/a"] = sentinel

	err := ensureDirAt(fakeFS.root("root"), "root", []string{"a"})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, []string{"open root/a", "mkdir root/a"}, fakeFS.events)
	require.Zero(t, fakeFS.openHandles)
}

func TestEnsureDirAtCreationFailuresAreRetryable(t *testing.T) {
	t.Parallel()

	for _, failurePath := range []string{"root/a", "root/a/b", "root/a/b/c"} {
		t.Run(failurePath, func(t *testing.T) {
			t.Parallel()

			sentinel := errors.New("mkdir retry sentinel")
			fakeFS := newFakeDurableFS("root")
			fakeFS.mkdirFailures[failurePath] = sentinel

			err := ensureDirAt(fakeFS.root("root"), "root", []string{"a", "b", "c"})
			require.ErrorIs(t, err, sentinel)
			require.Zero(t, fakeFS.openHandles)

			delete(fakeFS.mkdirFailures, failurePath)
			fakeFS.events = nil

			require.NoError(t, ensureDirAt(fakeFS.root("root"), "root", []string{"a", "b", "c"}))
			require.Equal(t, []string{
				"sync root", "sync root/a", "sync root/a/b", "sync root/a/b/c",
			}, syncEvents(fakeFS.events))
			require.Zero(t, fakeFS.openHandles)
			require.LessOrEqual(t, fakeFS.maxOpenHandles, 2)
		})
	}
}

const ensureDirLowDescriptorPath = "D8_ENSURE_DIR_LOW_DESCRIPTOR_PATH"

func TestEnsureDirDurablyUsesBoundedDescriptors(t *testing.T) {
	path := os.Getenv(ensureDirLowDescriptorPath)
	if path != "" {
		limit := &syscall.Rlimit{Cur: 32, Max: 32}
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, limit); err != nil {
			t.Fatalf("lower descriptor limit: %v", err)
		}

		if err := EnsureDir(path); err != nil {
			t.Fatalf("ensure deep directory under low descriptor limit: %v", err)
		}

		return
	}

	path = t.TempDir()
	for i := range 128 {
		path = filepath.Join(path, fmt.Sprintf("d%03d", i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEnsureDirDurablyUsesBoundedDescriptors$")
	cmd.Env = append(os.Environ(), ensureDirLowDescriptorPath+"="+path)

	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("low-descriptor helper did not return promptly: %v", ctx.Err())
	}

	if err != nil {
		t.Fatalf("low-descriptor helper failed: %v\n%s", err, output)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat deep output path: %v", err)
	}

	if !info.IsDir() {
		t.Fatalf("deep output mode = %s, want directory", info.Mode())
	}
}

func syncEvents(events []string) []string {
	syncs := make([]string, 0, len(events))
	for _, event := range events {
		if strings.HasPrefix(event, "sync ") {
			syncs = append(syncs, event)
		}
	}

	return syncs
}
