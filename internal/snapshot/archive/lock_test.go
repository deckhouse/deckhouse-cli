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
	"testing"
)

func TestArchiveLockCompatibility(t *testing.T) {
	t.Helper()

	t.Run("readers coexist and exclude writer", func(t *testing.T) {
		root := t.TempDir()

		first, err := AcquireReadLock(root)
		if err != nil {
			t.Fatalf("acquire first reader: %v", err)
		}
		defer func() { _ = first.Unlock() }()

		second, err := AcquireReadLock(root)
		if err != nil {
			t.Fatalf("acquire second reader: %v", err)
		}
		defer func() { _ = second.Unlock() }()

		_, err = AcquireWriteLock(root)
		if !errors.Is(err, ErrArchiveLocked) {
			t.Fatalf("writer error = %v, want ErrArchiveLocked", err)
		}
	})

	t.Run("writer excludes reader and releases", func(t *testing.T) {
		root := t.TempDir()

		writer, err := AcquireWriteLock(root)
		if err != nil {
			t.Fatalf("acquire writer: %v", err)
		}

		_, err = AcquireReadLock(root)
		if !errors.Is(err, ErrArchiveLocked) {
			t.Fatalf("reader error = %v, want ErrArchiveLocked", err)
		}

		if err := writer.Unlock(); err != nil {
			t.Fatalf("release writer: %v", err)
		}

		reader, err := AcquireReadLock(root)
		if err != nil {
			t.Fatalf("acquire reader after release: %v", err)
		}
		defer func() { _ = reader.Unlock() }()
	})
}
