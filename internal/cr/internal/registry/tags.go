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
	"context"
	"fmt"
)

// ListTags returns every tag of repoRef.
//
// The registry's page-by-page protocol is the client's business and has no
// console equivalent - there is no way for a user to ask for "the next range" -
// so callers get the complete list or an error, never a truncated one.
func ListTags(ctx context.Context, repoRef string, opts *Options) ([]string, error) {
	client, err := clientForRepoRef(repoRef, opts)
	if err != nil {
		return nil, err
	}

	tags, err := client.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("read tags for %s: %w", repoRef, err)
	}

	return tags, nil
}
