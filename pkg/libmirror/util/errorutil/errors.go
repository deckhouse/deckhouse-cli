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

package errorutil

import "strings"

const CustomTrivyMediaTypesWarning = `` +
	"It looks like you are using Project Quay registry and it is not configured correctly for hosting Deckhouse.\n" +
	"See the docs at https://deckhouse.io/documentation/v1/supported_versions.html#container-registry for more details.\n\n" +
	"TL;DR: You should retry push after allowing some additional types of OCI artifacts in your config.yaml as follows:\n" +
	`FEATURE_GENERAL_OCI_SUPPORT: true
ALLOWED_OCI_ARTIFACT_TYPES:
  application/octet-stream:
    - application/deckhouse.io.bdu.layer.v1.tar+gzip
    - application/vnd.cncf.openpolicyagent.layer.v1.tar+gzip
  application/vnd.aquasec.trivy.config.v1+json:
    - application/vnd.aquasec.trivy.javadb.layer.v1.tar+gzip
    - application/vnd.aquasec.trivy.db.layer.v1.tar+gzip`

func IsImageNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_UNKNOWN") || strings.Contains(errMsg, "404 Not Found")
}

func IsRepoNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "NAME_UNKNOWN")
}

func IsTrivyMediaTypeNotAllowedError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_INVALID") && (strings.Contains(errMsg, "vnd.aquasec.trivy") || strings.Contains(errMsg, "application/octet-stream"))
}
