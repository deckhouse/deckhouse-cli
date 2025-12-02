/*
Copyright 2025 Flant JSC

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

package security

import (
	"github.com/deckhouse/deckhouse-cli/internal"
)

// SecurityDownloadList tracks images to be downloaded for security databases
type SecurityDownloadList struct {
	rootURL string

	// Databases holds image references per database name
	Databases map[string]map[string]struct{}
}

// NewSecurityDownloadList creates a new security download list
func NewSecurityDownloadList(rootURL string) *SecurityDownloadList {
	return &SecurityDownloadList{
		rootURL:   rootURL,
		Databases: make(map[string]map[string]struct{}),
	}
}

// Fill populates the download list with all security database images
func (dl *SecurityDownloadList) Fill() {
	// Define security databases and their tags
	databases := map[string][]string{
		internal.SecurityTrivyDBSegment:     {"2"},
		internal.SecurityTrivyBDUSegment:    {"1"},
		internal.SecurityTrivyJavaDBSegment: {"1"},
		internal.SecurityTrivyChecksSegment: {"1"},
	}

	for dbName, tags := range databases {
		dl.Databases[dbName] = make(map[string]struct{})
		for _, tag := range tags {
			ref := dl.rootURL + "/security/" + dbName + ":" + tag
			dl.Databases[dbName][ref] = struct{}{}
		}
	}
}

