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

// Package apiclient talks to the runtime API of the Deckhouse controller (Module v2).
//
// The controller serves the API on two transports. Its TCP listener on the pod IP
// (the "self" port) publishes the routes that carry no package values: probes,
// metrics, pprof, queues, scheduler and requirements. Its Unix socket inside the
// container serves all of that plus /api/v1/packages, whose answers carry registry
// credentials, rendered Secrets and hook snapshots.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Output formats the API encodes dumps in, chosen with the output query parameter.
const (
	OutputJSON = "json"
	OutputYAML = "yaml"
)

const packagesPrefix = "/api/v1/packages"

// Transport sends a GET request for path with query to the API.
type Transport interface {
	Get(ctx context.Context, path string, query url.Values) (*Response, error)
}

// Response is an answer of the API, whatever its status.
type Response struct {
	StatusCode int
	Body       []byte
}

// StatusError is an answer of the API outside 2xx.
type StatusError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("GET %s: HTTP %d: %s", e.Path, e.StatusCode, e.Body)
}

// Client calls the API. Public reaches the TCP listener, Private the Unix socket;
// both may be the same transport, since the socket serves every route.
type Client struct {
	Public  Transport
	Private Transport
}

// Get returns the body of any route, reached through Private for the packages
// subtree and through Public otherwise.
func (c *Client) Get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	transport := c.Public
	if strings.HasPrefix(path, packagesPrefix) {
		transport = c.Private
	}

	return get(ctx, transport, path, query)
}

// Healthz reports that the controller process is up: GET /healthz.
func (c *Client) Healthz(ctx context.Context) (string, error) {
	body, err := get(ctx, c.Public, "/healthz", nil)

	return strings.TrimSpace(string(body)), err
}

// Readyz reports the readiness of the replica: GET /readyz, which answers 500 with
// the reason while the replica is not ready.
func (c *Client) Readyz(ctx context.Context) (Readiness, error) {
	resp, err := c.Public.Get(ctx, "/readyz", nil)
	if err != nil {
		return Readiness{}, err
	}

	message := strings.TrimSpace(string(resp.Body))

	switch resp.StatusCode {
	case http.StatusOK:
		return Readiness{Ready: true, Message: message}, nil
	case http.StatusInternalServerError:
		return Readiness{Message: message}, nil
	}

	return Readiness{}, &StatusError{Path: "/readyz", StatusCode: resp.StatusCode, Body: message}
}

// Endpoints lists the routes Public serves: GET /endpoints. The socket lists the
// packages subtree too.
func (c *Client) Endpoints(ctx context.Context) ([]Endpoint, error) {
	body, err := get(ctx, c.Public, "/endpoints", nil)
	if err != nil {
		return nil, err
	}

	var endpoints []Endpoint

	for line := range strings.Lines(string(body)) {
		method, path, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}

		endpoints = append(endpoints, Endpoint{Method: method, Path: path})
	}

	return endpoints, nil
}

// Metrics returns the Prometheus exposition of the controller: GET /metrics.
func (c *Client) Metrics(ctx context.Context) ([]byte, error) {
	return get(ctx, c.Public, "/metrics", nil)
}

// Pprof returns /debug/pprof/{name}: a named profile (heap, goroutine, allocs,
// block, mutex, threadcreate), profile or trace (both take seconds), cmdline or
// symbol. Query carries seconds, debug and the like as net/http/pprof reads them.
func (c *Client) Pprof(ctx context.Context, name string, query url.Values) ([]byte, error) {
	return get(ctx, c.Public, "/debug/pprof/"+url.PathEscape(name), query)
}

// Queues returns the task queues: GET /api/v1/queues/dump. A non-empty pkg narrows
// the dump to the queues of that package; the controller treats an unknown package,
// which selects no queue, as every queue.
func (c *Client) Queues(ctx context.Context, pkg string) (*QueuesDump, error) {
	dump := new(QueuesDump)
	if _, err := getJSON(ctx, c.Public, "/api/v1/queues/dump", nameQuery(pkg), dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Scheduler returns the scheduler state of every package: GET /api/v1/scheduler/dump.
func (c *Client) Scheduler(ctx context.Context) (*SchedulerDump, error) {
	dump := new(SchedulerDump)
	if _, err := getJSON(ctx, c.Public, "/api/v1/scheduler/dump", nil, dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// SchedulerNode returns the scheduler state of one package, nil for an unknown one:
// GET /api/v1/scheduler/dump?name=.
func (c *Client) SchedulerNode(ctx context.Context, pkg string) (*SchedulerNode, error) {
	node := new(SchedulerNode)

	found, err := getJSON(ctx, c.Public, "/api/v1/scheduler/dump", nameQuery(pkg), node)
	if err != nil || !found {
		return nil, err
	}

	return node, nil
}

// Requirements returns the values stored for the release requirement checks:
// GET /api/v1/requirements/dump.
func (c *Client) Requirements(ctx context.Context) (RequirementsDump, error) {
	var dump RequirementsDump
	if _, err := getJSON(ctx, c.Public, "/api/v1/requirements/dump", nil, &dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Packages returns every application and module: GET /api/v1/packages/dump.
func (c *Client) Packages(ctx context.Context) (*PackagesDump, error) {
	dump := new(PackagesDump)
	if _, err := getJSON(ctx, c.Private, packagesPrefix+"/dump", nil, dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Package returns one package, nil when there is none with that name:
// GET /api/v1/packages/dump?name=.
func (c *Client) Package(ctx context.Context, name string) (*NamedPackage, error) {
	pkg := new(NamedPackage)

	found, err := getJSON(ctx, c.Private, packagesPrefix+"/dump", nameQuery(name), pkg)
	if err != nil || !found {
		return nil, err
	}

	return pkg, nil
}

// Global returns the global module, nil before the runtime has initialized it:
// GET /api/v1/packages/global/dump.
func (c *Client) Global(ctx context.Context) (*GlobalModule, error) {
	global := new(GlobalModule)

	found, err := getJSON(ctx, c.Private, packagesPrefix+"/global/dump", nil, global)
	if err != nil || !found {
		return nil, err
	}

	return global, nil
}

// Render returns the Helm manifests rendered for a package as YAML:
// GET /api/v1/packages/render/{name}. A package without a chart is a 400, a
// render failure a 500, and so is an unknown package ("render failed: no package found").
func (c *Client) Render(ctx context.Context, name string) (string, error) {
	body, err := get(ctx, c.Private, packagesPrefix+"/render/"+url.PathEscape(name), nil)

	return string(body), err
}

// Snapshots returns the hook snapshots of a package; an unknown package is a 404:
// GET /api/v1/packages/snapshots/{name}.
func (c *Client) Snapshots(ctx context.Context, name string) (Snapshots, error) {
	var snapshots Snapshots
	if _, err := getJSON(ctx, c.Private, packagesPrefix+"/snapshots/"+url.PathEscape(name), nil, &snapshots); err != nil {
		return nil, err
	}

	return snapshots, nil
}

// get returns the body of a 2xx answer and a StatusError for any other.
func get(ctx context.Context, transport Transport, path string, query url.Values) ([]byte, error) {
	resp, err := transport.Get(ctx, path, query)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, &StatusError{Path: path, StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(resp.Body))}
	}

	return resp.Body, nil
}

// getJSON asks for JSON and decodes it into out. It reports false, leaving out
// untouched, when the API answered null: the dumps do that for an unknown name.
func getJSON(ctx context.Context, transport Transport, path string, query url.Values, out any) (bool, error) {
	query = withOutput(query, OutputJSON)

	body, err := get(ctx, transport, path, query)
	if err != nil {
		return false, err
	}

	if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		return false, nil
	}

	if err := json.Unmarshal(body, out); err != nil {
		return false, fmt.Errorf("decode %s: %w", path, err)
	}

	return true, nil
}

// withOutput returns a copy of query that asks for format.
func withOutput(query url.Values, format string) url.Values {
	result := url.Values{}
	for key, values := range query {
		result[key] = append([]string(nil), values...)
	}

	result.Set("output", format)

	return result
}

// nameQuery narrows a dump to one package; an empty name means every package.
func nameQuery(name string) url.Values {
	if name == "" {
		return nil
	}

	return url.Values{"name": {name}}
}
