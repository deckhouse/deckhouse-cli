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

package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The testdata/*.json files were produced by the controller itself: values of its
// dump types (deckhouse-controller/internal/packages/{runtime,schedule}, queue,
// shell-operator snapshots) encoded by api.EncodeResponse at deckhouse main
// 8010976436, so they show the exact wire format.

func fixture(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)

	return data
}

// decodeStrict fails on any field the controller sends that the type lacks.
func decodeStrict(t *testing.T, name string, out any) {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(fixture(t, name)))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(out), name)
}

func TestTypesMatchControllerOutput(t *testing.T) {
	var queues QueuesDump
	decodeStrict(t, "queues.json", &queues)
	require.Len(t, queues.Queues, 3)
	require.Equal(t, "hook failed: exit status 1", *queues.Queues["prometheus"].Tasks[1].Error)
	require.Equal(t, "-500ms", queues.Queues["prometheus"].Tasks[0].NextRetry)
	require.Nil(t, queues.Queues["prometheus"].Tasks[0].Error)

	var scheduler SchedulerDump
	decodeStrict(t, "scheduler.json", &scheduler)
	require.Equal(t, DecisionForbid, scheduler.Nodes["istio"].Decision.Kind)
	require.Equal(t, NodeStateIdle, scheduler.Nodes["istio"].State)

	var node SchedulerNode
	decodeStrict(t, "scheduler_node.json", &node)
	require.Equal(t, uint(900), node.EffectiveOrder)
	require.Equal(t, Dependency{Constraint: ">=1.0"}, node.Dependencies["cert-manager"])
	require.Equal(t, Dependency{Optional: true}, node.Dependencies["istio"], "a null constraint decodes to empty")
	require.Equal(t, []string{"global"}, node.Subscriptions)

	var app Application
	decodeStrict(t, "package_app.json", &app)
	require.Equal(t, "my-app", app.Name)
	require.Equal(t, "team-a", app.Namespace)
	require.Equal(t, ">=1.29", app.Definition.Requirements.Kubernetes)
	require.Empty(t, app.Definition.Requirements.Deckhouse)
	require.Equal(t, map[string]string{"prometheus": ">=1.0", "cert-manager": ""}, app.Definition.Requirements.Modules.Mandatory)
	require.Equal(t, "e30=", app.Repository.DockerConfig)
	require.Equal(t, Operation{
		Group: "apps", Version: "v1", Kind: "Deployment", Name: "web", Namespace: "team-a",
		Type: "Apply", ID: "apply/apps/v1/Deployment/team-a/web", Category: "resource", Status: "Completed", DependsOn: []string{"meta/start"},
	}, app.Status.Tracking.Report.Operations[0])
	require.Equal(t, []URL{{URL: "https://app.example.com", Description: "UI"}}, app.Status.URLs)

	var module Module
	decodeStrict(t, "package_module.json", &module)
	require.True(t, module.Embedded)
	require.Equal(t, uint32(300), module.Definition.Weight)
	require.Equal(t, "monitoring", module.Definition.ExclusiveGroup)
	require.Equal(t, []ModuleGroup{{Name: "legacy", Members: map[string]string{"old-prometheus": "<2"}}}, module.Definition.Requirements.Modules.NoneOf)
	require.Equal(t, ConditionMaintenanceMode, module.Status.Conditions[0].Type)

	var packages PackagesDump
	decodeStrict(t, "packages.json", &packages)
	require.Equal(t, app, packages.Apps["team-a.my-app"])
	require.Equal(t, module, packages.Modules["prometheus"])

	var global GlobalModule
	decodeStrict(t, "global.json", &global)
	require.Equal(t, map[string]bool{"prometheus": true, "istio": false}, global.Dynamic)

	var snapshots Snapshots
	decodeStrict(t, "snapshots.json", &snapshots)
	require.Nil(t, snapshots["hooks/b.go"], "a hook without Kubernetes bindings maps to null")

	pods := snapshots["hooks/a.go"]["pods"]
	require.Len(t, pods.Snapshot, 2)
	require.Equal(t, "web-0", pods.Snapshot[0].Object["metadata"].(map[string]any)["name"])
	require.Equal(t, map[string]any{"name": "web-0"}, pods.Snapshot[0].FilterResult)
	require.Nil(t, pods.Snapshot[1].Object, "the object is dropped when only filter results are kept")
	require.Equal(t, "only-result", pods.Snapshot[1].FilterResult)
	require.Equal(t, CachedObjectsInfo{Count: 2, Added: 3, Deleted: 1}, pods.Operations.SinceStart)

	var requirements RequirementsDump
	decodeStrict(t, "requirements.json", &requirements)
	require.Equal(t, "1.31.4", requirements["global.discovery.kubernetesVersion"])
}

func TestNamedPackageTellsAppFromModule(t *testing.T) {
	var app NamedPackage
	require.NoError(t, json.Unmarshal(fixture(t, "package_app.json"), &app))
	require.NotNil(t, app.Application)
	require.Nil(t, app.Module)
	require.Equal(t, "team-a", app.Application.Namespace)

	var module NamedPackage
	require.NoError(t, json.Unmarshal(fixture(t, "package_module.json"), &module))
	require.Nil(t, module.Application)
	require.NotNil(t, module.Module)
	require.Equal(t, "prometheus", module.Module.Name)
}

type request struct {
	transport string
	path      string
	query     url.Values
}

// fakeTransport answers from a table keyed by path and records the requests.
type fakeTransport struct {
	name      string
	requests  *[]request
	responses map[string]*Response
}

func (f fakeTransport) Get(_ context.Context, path string, query url.Values) (*Response, error) {
	*f.requests = append(*f.requests, request{transport: f.name, path: path, query: query})

	if resp, ok := f.responses[path]; ok {
		return resp, nil
	}

	return &Response{StatusCode: http.StatusNotFound, Body: []byte("404 page not found\n")}, nil
}

func newFakeClient(responses map[string]*Response) (*Client, *[]request) {
	var requests []request

	return &Client{
		Public:  fakeTransport{name: "public", requests: &requests, responses: responses},
		Private: fakeTransport{name: "private", requests: &requests, responses: responses},
	}, &requests
}

func ok(body []byte) *Response { return &Response{StatusCode: http.StatusOK, Body: body} }

func TestClientRoutesEveryEndpoint(t *testing.T) {
	ctx := context.Background()
	null := []byte("null\n")

	client, requests := newFakeClient(map[string]*Response{
		"/healthz":                          ok([]byte("ok")),
		"/endpoints":                        ok([]byte("GET /healthz\nGET /api/v1/queues/dump\nGET /debug/pprof/*\n")),
		"/metrics":                          ok([]byte("deckhouse_tasks_queue_length 0\n")),
		"/debug/pprof/heap":                 ok([]byte("profile")),
		"/api/v1/queues/dump":               ok(fixture(t, "queues.json")),
		"/api/v1/scheduler/dump":            ok(null),
		"/api/v1/requirements/dump":         ok(fixture(t, "requirements.json")),
		"/api/v1/packages/dump":             ok(null),
		"/api/v1/packages/global/dump":      ok(null),
		"/api/v1/packages/render/prometheus": ok([]byte("---\nkind: ConfigMap\n")),
		"/api/v1/packages/render/no-chart":  {StatusCode: http.StatusBadRequest, Body: []byte("package has no Helm chart\n")},
		"/api/v1/packages/snapshots/prometheus": ok(fixture(t, "snapshots.json")),
	})

	healthz, err := client.Healthz(ctx)
	require.NoError(t, err)
	require.Equal(t, "ok", healthz)

	endpoints, err := client.Endpoints(ctx)
	require.NoError(t, err)
	require.Equal(t, []Endpoint{{"GET", "/healthz"}, {"GET", "/api/v1/queues/dump"}, {"GET", "/debug/pprof/*"}}, endpoints)

	metrics, err := client.Metrics(ctx)
	require.NoError(t, err)
	require.Equal(t, "deckhouse_tasks_queue_length 0\n", string(metrics))

	profile, err := client.Pprof(ctx, "heap", url.Values{"debug": {"1"}})
	require.NoError(t, err)
	require.Equal(t, "profile", string(profile))

	queues, err := client.Queues(ctx, "prometheus")
	require.NoError(t, err)
	require.Equal(t, 2, queues.Queues["prometheus"].Length)

	node, err := client.SchedulerNode(ctx, "unknown")
	require.NoError(t, err)
	require.Nil(t, node, "null is an unknown package")

	requirements, err := client.Requirements(ctx)
	require.NoError(t, err)
	require.Equal(t, "22.04", requirements["nodesMinimalOSVersionUbuntu"])

	pkg, err := client.Package(ctx, "unknown")
	require.NoError(t, err)
	require.Nil(t, pkg)

	global, err := client.Global(ctx)
	require.NoError(t, err)
	require.Nil(t, global, "null before the runtime initializes the global module")

	manifests, err := client.Render(ctx, "prometheus")
	require.NoError(t, err)
	require.Equal(t, "---\nkind: ConfigMap\n", manifests)

	_, err = client.Render(ctx, "no-chart")
	require.EqualError(t, err, "GET /api/v1/packages/render/no-chart: HTTP 400: package has no Helm chart")

	snapshots, err := client.Snapshots(ctx, "prometheus")
	require.NoError(t, err)
	require.Contains(t, snapshots, "hooks/a.go")

	_, err = client.Snapshots(ctx, "missing")

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, http.StatusNotFound, statusErr.StatusCode)

	_, err = client.Get(ctx, "/api/v1/packages/dump", url.Values{"output": {"yaml"}})
	require.NoError(t, err)

	require.Equal(t, []request{
		{"public", "/healthz", nil},
		{"public", "/endpoints", nil},
		{"public", "/metrics", nil},
		{"public", "/debug/pprof/heap", url.Values{"debug": {"1"}}},
		{"public", "/api/v1/queues/dump", url.Values{"output": {"json"}, "name": {"prometheus"}}},
		{"public", "/api/v1/scheduler/dump", url.Values{"output": {"json"}, "name": {"unknown"}}},
		{"public", "/api/v1/requirements/dump", url.Values{"output": {"json"}}},
		{"private", "/api/v1/packages/dump", url.Values{"output": {"json"}, "name": {"unknown"}}},
		{"private", "/api/v1/packages/global/dump", url.Values{"output": {"json"}}},
		{"private", "/api/v1/packages/render/prometheus", nil},
		{"private", "/api/v1/packages/render/no-chart", nil},
		{"private", "/api/v1/packages/snapshots/prometheus", url.Values{"output": {"json"}}},
		{"private", "/api/v1/packages/snapshots/missing", url.Values{"output": {"json"}}},
		{"private", "/api/v1/packages/dump", url.Values{"output": {"yaml"}}},
	}, *requests)
}

func TestReadyz(t *testing.T) {
	for _, tc := range []struct {
		name    string
		resp    *Response
		want    Readiness
		wantErr string
	}{
		{"ready", ok([]byte("Startup converge done.\n")), Readiness{Ready: true, Message: "Startup converge done."}, ""},
		{"converging", &Response{StatusCode: http.StatusInternalServerError, Body: []byte("Startup converge in progress\n")}, Readiness{Message: "Startup converge in progress"}, ""},
		{"unexpected", &Response{StatusCode: http.StatusBadGateway, Body: []byte("bad gateway")}, Readiness{}, "GET /readyz: HTTP 502: bad gateway"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newFakeClient(map[string]*Response{"/readyz": tc.resp})

			readiness, err := client.Readyz(context.Background())
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, readiness)
		})
	}
}
