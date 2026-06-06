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

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const manifestAPIBase = "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1"

type ManifestFetcher func(ctx context.Context, sClient *safeClient.SafeClient, node *Node) ([][]byte, error)

var FetchManifests ManifestFetcher = fetchManifests

func fetchManifests(ctx context.Context, sClient *safeClient.SafeClient, node *Node) ([][]byte, error) {
	body, err := sClient.AggregatedGet(ctx, manifestPath(node))
	if err != nil {
		return nil, fmt.Errorf("aggregated GET for node %s: %w", node.ID, err)
	}

	defer body.Close()

	objects, err := decodeTopLevelArray(body)
	if err != nil {
		return nil, fmt.Errorf("decode manifests for node %s: %w", node.ID, err)
	}

	return objects, nil
}

func manifestPath(node *Node) string {
	return strings.Join([]string{
		manifestAPIBase,
		"namespaces", node.Namespace,
		node.Resource, node.Name,
		"manifests",
	}, "/")
}

type ObjectFilter func(rawJSON []byte) (bool, error)

func BuildObjectFilter(objectFlag string) (ObjectFilter, error) {
	if objectFlag == "" {
		return nil, nil
	}

	apiVersion, kind, name, err := parseObjectFlag(objectFlag)
	if err != nil {
		return nil, fmt.Errorf("--object flag: %w", err)
	}

	return func(rawJSON []byte) (bool, error) {
		var m map[string]any

		if err := json.Unmarshal(rawJSON, &m); err != nil {
			return false, err
		}

		gotAPI, _ := m["apiVersion"].(string)
		gotKind, _ := m["kind"].(string)
		meta, _ := m["metadata"].(map[string]any)

		gotName := ""
		if meta != nil {
			gotName, _ = meta["name"].(string)
		}

		return gotAPI == apiVersion && gotKind == kind && gotName == name, nil
	}, nil
}

// parseObjectFlag splits "<apiVersion>/<Kind>/<name>" into its three components.
// Name is always the last segment, Kind the second-to-last, the rest is apiVersion.
func parseObjectFlag(flag string) (string, string, string, error) {
	lastSlash := -1
	secondLastSlash := -1

	for i := len(flag) - 1; i >= 0; i-- {
		if flag[i] != '/' {
			continue
		}

		if lastSlash == -1 {
			lastSlash = i

			continue
		}

		secondLastSlash = i

		break
	}

	if lastSlash < 0 || secondLastSlash < 0 {
		return "", "", "", fmt.Errorf("expected format <apiVersion>/<Kind>/<name>, got %q", flag)
	}

	name := flag[lastSlash+1:]
	kind := flag[secondLastSlash+1 : lastSlash]
	apiVersion := flag[:secondLastSlash]

	if name == "" || kind == "" || apiVersion == "" {
		return "", "", "", fmt.Errorf("empty segment in --object flag %q", flag)
	}

	return apiVersion, kind, name, nil
}

// decodeTopLevelArray reads a JSON top-level array from r and returns each element
// as a raw JSON byte slice. Elements are decoded one at a time to avoid buffering the full response.
func decodeTopLevelArray(r io.Reader) ([][]byte, error) {
	dec := json.NewDecoder(r)

	if err := expectDelim(dec, '['); err != nil {
		return nil, fmt.Errorf("JSON array expected: %w", err)
	}

	var objects [][]byte

	for dec.More() {
		var raw json.RawMessage

		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("decode array element: %w", err)
		}

		// Copy to avoid decoder buffer reuse.
		cp := make([]byte, len(raw))
		copy(cp, raw)
		objects = append(objects, cp)
	}

	if err := expectDelim(dec, ']'); err != nil {
		return nil, fmt.Errorf("JSON array not closed: %w", err)
	}

	return objects, nil
}

// expectDelim reads the next token from dec and returns an error unless it is the expected delimiter.
func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}

	if delim, ok := tok.(json.Delim); !ok || delim != want {
		return fmt.Errorf("expected %v, got %v", want, tok)
	}

	return nil
}
