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

package deckhousesettings

import (
	"strings"
	"testing"
)

func TestIsSensitiveKey(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{"password", true},
		{"Password", true},
		{"license", true},
		{"licenseKey", true},
		{"dockerCfg", true},
		{"registryDockerCfg", true},
		{"token", true},
		{"bearerToken", true},
		{"apiKey", true},
		{"accessKey", true},
		{"secretAccessKey", true},
		{"credentials", true},
		{"imagesRepo", false},
		{"username", false},
		{"mode", false},
		{"scheme", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := isSensitiveKey(tt.key); got != tt.want {
				t.Errorf("isSensitiveKey(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestConfigSettingsProcessingMasksCredentials(t *testing.T) {
	settings := map[string]interface{}{
		"registry": map[string]interface{}{
			"mode": "Direct",
			"direct": map[string]interface{}{
				"imagesRepo": "registry.example.com/deckhouse",
				"username":   "reg-user",
				"password":   "SuperSecret123",
				"license":    "LICENSE-KEY-42",
			},
		},
		"dockerCfg":      "eyJhdXRocyI6e319",
		"emptyPassword":  "",
		"nullToken":      nil,
		"tokens":         []interface{}{"tok-a", "tok-b"},
		"releaseChannel": "Stable",
	}

	result := configSettingsFromMapProcessing(settings)
	output := formatModuleConfigSettings(result)

	for _, secret := range []string{"SuperSecret123", "LICENSE-KEY-42", "eyJhdXRocyI6e319", "tok-a", "tok-b"} {
		if strings.Contains(output, secret) {
			t.Errorf("output leaks secret %q:\n%s", secret, output)
		}
	}

	// Scalar array items are printed without the "#N:" key, mask only.
	for _, masked := range []string{"password: ***", "license: ***", "dockerCfg: ***", "├ ***", "└ ***"} {
		if !strings.Contains(output, masked) {
			t.Errorf("output missing masked line %q:\n%s", masked, output)
		}
	}

	// Non-credential values stay readable.
	for _, plain := range []string{"imagesRepo: registry.example.com/deckhouse", "username: reg-user", "mode: Direct", "releaseChannel: Stable"} {
		if !strings.Contains(output, plain) {
			t.Errorf("output missing plain line %q:\n%s", plain, output)
		}
	}

	// Empty and null credential values are not replaced with the mask.
	if strings.Contains(output, "emptyPassword: ***") {
		t.Errorf("empty credential value must not be masked:\n%s", output)
	}

	if strings.Contains(output, "nullToken: ***") {
		t.Errorf("null credential value must not be masked:\n%s", output)
	}
}

func TestConfigSettingsProcessingMasksNestedUnderCredentialKey(t *testing.T) {
	// A map under a credential key: all nested scalars are masked.
	settings := map[string]interface{}{
		"registryCredentials": map[string]interface{}{
			"user": "admin",
			"pass": "qwerty",
		},
	}

	output := formatModuleConfigSettings(configSettingsFromMapProcessing(settings))

	for _, secret := range []string{"admin", "qwerty"} {
		if strings.Contains(output, secret) {
			t.Errorf("output leaks nested secret %q:\n%s", secret, output)
		}
	}

	for _, masked := range []string{"user: ***", "pass: ***"} {
		if !strings.Contains(output, masked) {
			t.Errorf("output missing masked line %q:\n%s", masked, output)
		}
	}
}
