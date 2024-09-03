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

package contexts

import (
	"github.com/google/go-containerregistry/pkg/authn"
)

type Logger interface {
	DebugF(format string, a ...interface{})
	DebugLn(a ...interface{})

	InfoF(format string, a ...interface{})
	InfoLn(a ...interface{})

	WarnF(format string, a ...interface{})
	WarnLn(a ...interface{})

	Process(topic string, run func() error) error
}

// BaseContext hold data related to pending registry mirroring operation.
type BaseContext struct {
	// --registry-login + --registry-password (can be nil in this case) or --license depending on the operation requested
	RegistryAuth authn.Authenticator
	RegistryHost string // --registry (FQDN with port, if one is provided)
	RegistryPath string // --registry (path)

	DeckhouseRegistryRepo string // --source during pull, extracted from bundle data during push

	BundlePath         string // --images-bundle-path
	UnpackedImagesPath string

	Insecure            bool // --insecure
	SkipTLSVerification bool // --skip-tls-verify

	Logger Logger
}
