//go:build dfrunnetwork

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

package errdiag

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DNSResolutionFailure(t *testing.T) {
	// .invalid TLD is reserved by RFC 2606 and guaranteed to never resolve.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := headImage(ctx, "nonexistent.invalid:443")
	require.Error(t, err)

	diag := Classify(err)
	require.NotNil(t, diag, "expected DNS error to be classified, got raw: %v", err)
	assert.Contains(t, diag.Category, CategoryDNS)
}
