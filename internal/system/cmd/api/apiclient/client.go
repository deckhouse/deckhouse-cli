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

// Package apiclient talks over HTTP to the runtime API of the Deckhouse controller
// (Module v2), served on the pod IP (the "self" port): probes, metrics, pprof,
// queues, scheduler, requirements and packages.
//
// The controller of deckhouse main at 8010976436 does not serve /api/v1/packages
// over HTTP yet, so there those routes answer 404.
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

// APIPrefix is where the controller mounts the versioned API (apiPrefix in
// deckhouse-controller/internal/packages/api/handlers/root).
const APIPrefix = "/api/v1"

// Output formats the API encodes dumps in, chosen with the output query parameter.
const (
	OutputJSON = "json"
	OutputYAML = "yaml"
)

// Transport sends a GET request for path with query to the API.
type Transport interface {
	Get(ctx context.Context, path string, query url.Values) (*Response, error)
}

// Response is an answer of the API, whatever its status.
type Response struct {
	StatusCode int
	Body       []byte
}

// Client calls the API through a transport.
type Client struct {
	transport Transport
}

// New returns a client that sends its requests through transport.
func New(transport Transport) *Client {
	return &Client{transport: transport}
}

// Get returns the body of any route.
func (c *Client) Get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	return c.get(ctx, path, query)
}

// Healthz reports that the controller process is up: GET /healthz.
func (c *Client) Healthz(ctx context.Context) (string, error) {
	body, err := c.get(ctx, "/healthz", nil)

	return strings.TrimSpace(string(body)), err
}

// Readyz reports the readiness of the replica: GET /readyz, which answers 500 with
// the reason while the replica is not ready.
func (c *Client) Readyz(ctx context.Context) (Readiness, error) {
	resp, err := c.transport.Get(ctx, "/readyz", nil)
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

// Endpoints lists the routes the TCP listener serves: GET /endpoints.
func (c *Client) Endpoints(ctx context.Context) ([]Endpoint, error) {
	body, err := c.get(ctx, "/endpoints", nil)
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
	return c.get(ctx, "/metrics", nil)
}

// Pprof returns /debug/pprof/{name}: a named profile (heap, goroutine, allocs,
// block, mutex, threadcreate), profile or trace (both take seconds), cmdline or
// symbol. Query carries seconds, debug and the like as net/http/pprof reads them.
func (c *Client) Pprof(ctx context.Context, name string, query url.Values) ([]byte, error) {
	return c.get(ctx, "/debug/pprof/"+url.PathEscape(name), query)
}

// Queues returns the task queues: GET /api/v1/queues/dump. A non-empty pkg narrows
// the dump to the queues of that package; the controller treats an unknown package,
// which selects no queue, as every queue.
func (c *Client) Queues(ctx context.Context, pkg string) (*QueuesDump, error) {
	dump := new(QueuesDump)
	if _, err := c.getJSON(ctx, APIPrefix+"/queues/dump", nameQuery(pkg), dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Scheduler returns the scheduler state of every package: GET /api/v1/scheduler/dump.
func (c *Client) Scheduler(ctx context.Context) (*SchedulerDump, error) {
	dump := new(SchedulerDump)
	if _, err := c.getJSON(ctx, APIPrefix+"/scheduler/dump", nil, dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// SchedulerNode returns the scheduler state of one package, ErrUnknownPackage for
// an unknown one: GET /api/v1/scheduler/dump?name=.
func (c *Client) SchedulerNode(ctx context.Context, pkg string) (*SchedulerNode, error) {
	node := new(SchedulerNode)

	found, err := c.getJSON(ctx, APIPrefix+"/scheduler/dump", nameQuery(pkg), node)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("scheduler node %q: %w", pkg, ErrUnknownPackage)
	}

	return node, nil
}

// Requirements returns the values stored for the release requirement checks:
// GET /api/v1/requirements/dump.
func (c *Client) Requirements(ctx context.Context) (RequirementsDump, error) {
	var dump RequirementsDump
	if _, err := c.getJSON(ctx, APIPrefix+"/requirements/dump", nil, &dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Packages returns every application and module: GET /api/v1/packages/dump.
func (c *Client) Packages(ctx context.Context) (*PackagesDump, error) {
	dump := new(PackagesDump)
	if _, err := c.getJSON(ctx, APIPrefix+"/packages/dump", nil, dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Package returns one package, ErrUnknownPackage when there is none with that
// name: GET /api/v1/packages/dump?name=.
func (c *Client) Package(ctx context.Context, name string) (*NamedPackage, error) {
	pkg := new(NamedPackage)

	found, err := c.getJSON(ctx, APIPrefix+"/packages/dump", nameQuery(name), pkg)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("package %q: %w", name, ErrUnknownPackage)
	}

	return pkg, nil
}

// Global returns the global module, ErrGlobalNotLoaded before the runtime has
// initialized it: GET /api/v1/packages/global/dump.
func (c *Client) Global(ctx context.Context) (*GlobalModule, error) {
	global := new(GlobalModule)

	found, err := c.getJSON(ctx, APIPrefix+"/packages/global/dump", nil, global)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, ErrGlobalNotLoaded
	}

	return global, nil
}

// Render returns the Helm manifests rendered for a package as YAML:
// GET /api/v1/packages/render/{name}. A package without a chart is a 400, a
// render failure a 500, and so is an unknown package ("render failed: no package found").
func (c *Client) Render(ctx context.Context, name string) (string, error) {
	body, err := c.get(ctx, APIPrefix+"/packages/render/"+url.PathEscape(name), nil)

	return string(body), err
}

// Snapshots returns the hook snapshots of a package; an unknown package is a 404:
// GET /api/v1/packages/snapshots/{name}.
func (c *Client) Snapshots(ctx context.Context, name string) (Snapshots, error) {
	var snapshots Snapshots
	if _, err := c.getJSON(ctx, APIPrefix+"/packages/snapshots/"+url.PathEscape(name), nil, &snapshots); err != nil {
		return nil, err
	}

	return snapshots, nil
}

// get returns the body of a 2xx answer and a StatusError for any other.
func (c *Client) get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	resp, err := c.transport.Get(ctx, path, query)
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
func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) (bool, error) {
	body, err := c.get(ctx, path, withOutput(query, OutputJSON))
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
