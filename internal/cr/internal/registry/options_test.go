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

package registry

import (
	"os"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func TestNew_HasDefaults(t *testing.T) {
	o := New()

	if o.Keychain == nil {
		t.Errorf("New() should seed the Docker keychain so commands authenticate without a prior login")
	}

	if o.Auth != nil {
		t.Errorf("New() should leave Auth unset; got %+v", o.Auth)
	}

	if o.Platform != nil {
		t.Errorf("New() should leave Platform nil; got %+v", o.Platform)
	}

	if o.PlainHTTP || o.TLSSkipVerify || o.Nondistributable || o.Verbose {
		t.Errorf("New() should leave every flag off; got %+v", o)
	}
}

func TestChainableBuilders(t *testing.T) {
	platform := &v1.Platform{OS: "linux", Architecture: "arm64"}
	auth := authn.FromConfig(authn.AuthConfig{Username: "u", Password: "p"})

	o := New().
		WithInsecure().
		WithNondistributable().
		WithVerbose().
		WithPlatform(platform).
		WithAuth(auth)

	if !o.PlainHTTP || !o.TLSSkipVerify {
		t.Errorf("WithInsecure must set both plain HTTP and TLS skip-verify; got %+v", o)
	}

	if !o.Nondistributable || !o.Verbose {
		t.Errorf("builders did not set their flags; got %+v", o)
	}

	if o.Platform != platform {
		t.Errorf("WithPlatform did not take; got %+v", o.Platform)
	}

	if o.Auth != auth {
		t.Errorf("WithAuth did not take")
	}
}

// A flag-driven caller passes the parsed result straight through, so a nil
// platform has to mean "unset" rather than clearing a previous value.
func TestWithPlatform_NilIsNoOp(t *testing.T) {
	platform := &v1.Platform{OS: "linux", Architecture: "amd64"}

	o := New().WithPlatform(platform).WithPlatform(nil)
	if o.Platform != platform {
		t.Errorf("WithPlatform(nil) must not clear the pinned platform; got %+v", o.Platform)
	}
}

// --insecure must reach reference parsing too: without name.Insecure a
// plain-HTTP reference is rejected before any request is made.
func TestNameOptions_TracksPlainHTTP(t *testing.T) {
	if got := len(New().nameOptions()); got != 0 {
		t.Errorf("secure options should add no name options; got %d", got)
	}

	if got := len(New().WithInsecure().nameOptions()); got != 1 {
		t.Errorf("--insecure should permit plain-HTTP references; got %d name options", got)
	}

	if _, err := name.ParseReference("localhost:5000/app:v1", New().WithInsecure().nameOptions()...); err != nil {
		t.Errorf("plain-HTTP reference should parse under --insecure: %v", err)
	}
}

func TestPushOptions_TrackNondistributable(t *testing.T) {
	if got := len(New().pushOptions()); got != 0 {
		t.Errorf("foreign layers are skipped by default; got %d push options", got)
	}

	if got := len(New().WithNondistributable().pushOptions()); got != 1 {
		t.Errorf("--allow-nondistributable-artifacts should reach the client; got %d push options", got)
	}
}

func TestGetOptions_TrackPlatform(t *testing.T) {
	o := New()
	if len(o.imageGetOptions()) != 0 || len(o.manifestGetOptions()) != 0 {
		t.Errorf("no platform pinned means no platform option")
	}

	o.WithPlatform(&v1.Platform{OS: "linux", Architecture: "arm64"})
	if len(o.imageGetOptions()) != 1 || len(o.manifestGetOptions()) != 1 {
		t.Errorf("--platform must reach both the image and the manifest call")
	}
}

// The registry client logs at debug on every operation and its own default
// logger writes to stdout. Several commands exist to put exact bytes there -
// "cr manifest ref | jq", "cr export ref - | tar tf -" - so anything landing on
// stdout corrupts the output the user asked for.
func TestLogger_NeverWritesToStdout(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		o := New()
		if verbose {
			o.WithVerbose()
		}

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("pipe: %v", err)
		}

		saved := os.Stdout
		os.Stdout = w

		logger := o.logger()
		logger.Error("error level")
		logger.Info("info level")
		logger.Debug("debug level")

		os.Stdout = saved

		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		buf := make([]byte, 1024)
		n, _ := r.Read(buf)

		if err := r.Close(); err != nil {
			t.Fatalf("close read end: %v", err)
		}

		if n > 0 {
			t.Errorf("verbose=%v: client logger wrote %d bytes to stdout: %q", verbose, n, buf[:n])
		}
	}
}
