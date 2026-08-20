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

package htpasswd

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// defaultFileMode is applied to a freshly created password file. Apache htpasswd
// instead requests 0666 and lets the umask reduce it (so 0644 under a 022 umask,
// 0664 under 002); d8 sets 0644 directly. On an update the existing file's mode
// is preserved instead (see loadPasswdFile).
const defaultFileMode os.FileMode = 0o644

// passwdFile is an in-memory view of an htpasswd file. Lines that do not match
// the user being edited — other users, comments, blank lines — are preserved
// verbatim, so editing one entry never disturbs the rest of the file.
type passwdFile struct {
	path  string
	lines []string
	mode  os.FileMode
}

// loadPasswdFile reads an existing htpasswd file, preserving its permission bits.
func loadPasswdFile(path string) (*passwdFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	pf := &passwdFile{path: path, mode: defaultFileMode}
	if info, statErr := os.Stat(path); statErr == nil {
		pf.mode = info.Mode().Perm()
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for scanner.Scan() {
		pf.lines = append(pf.lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	return pf, nil
}

// newPasswdFile returns an empty file view for the '-c' create flow.
func newPasswdFile(path string) *passwdFile {
	return &passwdFile{path: path, mode: defaultFileMode}
}

// lineUser returns the username field (everything before the first colon).
func lineUser(line string) string {
	user, _, _ := strings.Cut(line, ":")

	return user
}

// get returns the stored hash for username.
func (pf *passwdFile) get(username string) (string, bool) {
	for _, line := range pf.lines {
		if lineUser(line) == username {
			_, hash, _ := strings.Cut(line, ":")

			return hash, true
		}
	}

	return "", false
}

// upsert sets username's hash, replacing every existing entry for that user in
// place (matching real htpasswd, which rewrites all duplicate lines) or
// appending a new one when there is none. It reports whether the user already
// existed.
func (pf *passwdFile) upsert(username, hash string) bool {
	newLine := username + ":" + hash
	found := false

	for i, line := range pf.lines {
		if lineUser(line) == username {
			pf.lines[i] = newLine
			found = true
		}
	}

	if !found {
		pf.lines = append(pf.lines, newLine)
	}

	return found
}

// remove deletes username's entry, reporting whether it existed.
func (pf *passwdFile) remove(username string) bool {
	kept := make([]string, 0, len(pf.lines))
	found := false

	for _, line := range pf.lines {
		if lineUser(line) == username {
			found = true

			continue
		}

		kept = append(kept, line)
	}

	pf.lines = kept

	return found
}

// save writes the file atomically: it renders to a temp file in the same
// directory and renames it into place, so a crash mid-write can never leave a
// truncated password file.
func (pf *passwdFile) save() error {
	var buf bytes.Buffer
	for _, line := range pf.lines {
		buf.WriteString(line)
		buf.WriteByte('\n')
	}

	dir := filepath.Dir(pf.path)

	tmp, err := os.CreateTemp(dir, ".htpasswd-*")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}

	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(buf.Bytes()); err != nil {
		tmp.Close()

		return fmt.Errorf("writing temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Chmod(tmpName, pf.mode); err != nil {
		return fmt.Errorf("setting file mode: %w", err)
	}

	if err := os.Rename(tmpName, pf.path); err != nil {
		return fmt.Errorf("replacing %s: %w", pf.path, err)
	}

	return nil
}
