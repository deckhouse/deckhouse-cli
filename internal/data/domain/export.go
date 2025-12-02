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
	"strings"
)

// DataExport represents a data export resource
type DataExport struct {
	Name      string
	Namespace string
	TTL       string
	Publish   bool
	TargetRef VolumeRef
	Status    DataExportStatus
}

// DataExportStatus represents the status of a DataExport
type DataExportStatus struct {
	URL        string
	PublicURL  string
	CA         string
	VolumeMode VolumeMode
	Ready      bool
	Expired    bool
}

// CreateExportParams contains parameters for creating a DataExport
type CreateExportParams struct {
	Name       string
	Namespace  string
	TTL        string
	VolumeKind VolumeKind
	VolumeName string
	Publish    bool
}

// DownloadParams contains parameters for downloading data
type DownloadParams struct {
	Name      string
	Namespace string
	SrcPath   string
	DstPath   string
	Publish   bool
	TTL       string
}

// ListParams contains parameters for listing data
type ListParams struct {
	Name      string
	Namespace string
	Path      string
	Publish   bool
	TTL       string
}

// GenerateExportName generates a DataExport name from volume reference
// Returns the generated name and whether a new export should be created
func GenerateExportName(input string) (exportName string, volumeRef *VolumeRef, needsCreate bool) {
	lowerInput := strings.ToLower(input)

	var prefix, kind string
	var volumeKind VolumeKind

	switch {
	case strings.HasPrefix(lowerInput, "pvc/"):
		prefix, kind = "de-pvc-", input[4:]
		volumeKind = VolumeKindPVC
	case strings.HasPrefix(lowerInput, "persistentvolumeclaim/"):
		prefix, kind = "de-pvc-", input[len("persistentvolumeclaim/"):]
		volumeKind = VolumeKindPVC
	case strings.HasPrefix(lowerInput, "vs/"):
		prefix, kind = "de-vs-", input[3:]
		volumeKind = VolumeKindSnapshot
	case strings.HasPrefix(lowerInput, "volumesnapshot/"):
		prefix, kind = "de-vs-", input[len("volumesnapshot/"):]
		volumeKind = VolumeKindSnapshot
	case strings.HasPrefix(lowerInput, "vd/"):
		prefix, kind = "de-vd-", input[3:]
		volumeKind = VolumeKindVirtualDisk
	case strings.HasPrefix(lowerInput, "virtualdisk/"):
		prefix, kind = "de-vd-", input[len("virtualdisk/"):]
		volumeKind = VolumeKindVirtualDisk
	case strings.HasPrefix(lowerInput, "vds/"):
		prefix, kind = "de-vds-", input[4:]
		volumeKind = VolumeKindVDSnapshot
	case strings.HasPrefix(lowerInput, "virtualdisksnapshot/"):
		prefix, kind = "de-vds-", input[len("virtualdisksnapshot/"):]
		volumeKind = VolumeKindVDSnapshot
	default:
		return input, nil, false
	}

	return prefix + kind, &VolumeRef{Kind: volumeKind, Name: kind}, true
}

