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

package imagefs

import "io/fs"

type EntryType string

const (
	TypeFile     EntryType = "file"
	TypeDir      EntryType = "dir"
	TypeSymlink  EntryType = "symlink"
	TypeHardlink EntryType = "hardlink"
	TypeWhiteout EntryType = "whiteout"
	TypeOther    EntryType = "other"
)

type Entry struct {
	Path     string      `json:"path"`
	Type     EntryType   `json:"type"`
	Size     int64       `json:"size"`
	Mode     fs.FileMode `json:"-"`
	ModeStr  string      `json:"mode"`
	Linkname string      `json:"linkname,omitempty"`
}

func (e Entry) IsDir() bool { return e.Type == TypeDir }
