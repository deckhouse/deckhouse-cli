/*
Copyright 2024 Flant JSC

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

package domain

import (
	"errors"
	"fmt"
	"strings"
)

// VolumeKind represents the type of volume
type VolumeKind string

const (
	VolumeKindPVC         VolumeKind = "PersistentVolumeClaim"
	VolumeKindSnapshot    VolumeKind = "VolumeSnapshot"
	VolumeKindVirtualDisk VolumeKind = "VirtualDisk"
	VolumeKindVDSnapshot  VolumeKind = "VirtualDiskSnapshot"
)

// VolumeMode represents how the volume is accessed
type VolumeMode string

const (
	VolumeModeFilesystem VolumeMode = "Filesystem"
	VolumeModeBlock      VolumeMode = "Block"
)

// VolumeRef represents a reference to a volume
type VolumeRef struct {
	Kind VolumeKind
	Name string
}

// DefaultTTL is the default time-to-live for DataExport/DataImport
const DefaultTTL = "2m"

var (
	ErrUnsupportedVolumeMode = errors.New("unsupported volume mode")
	ErrInvalidVolumeFormat   = errors.New("invalid volume format, expect: <type>/<name>")
	ErrInvalidVolumeType     = errors.New("invalid volume type")
)

// ParseVolumeRef parses a string like "pvc/my-volume" into VolumeRef
func ParseVolumeRef(input string) (*VolumeRef, error) {
	parts := strings.Split(input, "/")
	if len(parts) != 2 {
		return nil, ErrInvalidVolumeFormat
	}

	kindStr := strings.ToLower(parts[0])
	name := parts[1]

	var kind VolumeKind
	switch kindStr {
	case "pvc", "persistentvolumeclaim":
		kind = VolumeKindPVC
	case "vs", "volumesnapshot":
		kind = VolumeKindSnapshot
	case "vd", "virtualdisk":
		kind = VolumeKindVirtualDisk
	case "vds", "virtualdisksnapshot":
		kind = VolumeKindVDSnapshot
	default:
		return nil, fmt.Errorf("%w: %s (valid: pvc, vs, vd, vds)", ErrInvalidVolumeType, kindStr)
	}

	return &VolumeRef{Kind: kind, Name: name}, nil
}

// ValidVolumeKinds returns list of valid volume kinds
func ValidVolumeKinds() []VolumeKind {
	return []VolumeKind{
		VolumeKindPVC,
		VolumeKindSnapshot,
		VolumeKindVirtualDisk,
		VolumeKindVDSnapshot,
	}
}

