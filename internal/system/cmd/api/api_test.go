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

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/api/apiclient"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

func readFixture(t *testing.T, name string, out any) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("apiclient", "testdata", name))
	require.NoError(t, err)

	if out != nil {
		require.NoError(t, json.Unmarshal(data, out))
	}

	return data
}

const queuesText = `Queue 'prometheus': 2 task(s)
  1. Run (enqueued 1.5s, next retry -500ms)
  2. HookRun:hooks/a.go (enqueued 300ms, next retry 2s)
     error: hook failed: exit status 1
Summary: 3 queue(s), 1 active, 2 task(s).
`

func TestTextViews(t *testing.T) {
	var queues apiclient.QueuesDump
	readFixture(t, "queues.json", &queues)

	var buf bytes.Buffer
	printQueues(&buf, &queues)
	require.Equal(t, queuesText, buf.String())

	var scheduler apiclient.SchedulerDump
	readFixture(t, "scheduler.json", &scheduler)

	buf.Reset()
	require.NoError(t, printSchedulerNodes(&buf, scheduler.Nodes))
	require.Equal(t, `NAME        VERSION  STATE      ORDER  EFFECTIVE  DECISION                  SCHEDULE REASON
istio       1.21.0   idle       900    900        Forbid (VersionMismatch)  -
prometheus  1.70.0   scheduled  300    900        Enable (Bundle)           dependencies satisfied
`, buf.String())

	var packages apiclient.PackagesDump
	readFixture(t, "packages.json", &packages)

	buf.Reset()
	require.NoError(t, printPackages(&buf, &packages))
	require.Equal(t, `KIND    NAME           VERSION  RUNNING  ISSUES
app     team-a.my-app  v1.2.3   true     Scaled=False (NotReady)
module  prometheus     v1.70.0  true     MaintenanceMode=True (NoResourceReconciliation)
`, buf.String(), "MaintenanceMode=False is fine, MaintenanceMode=True is an issue")
}

// requestLog records the request URIs the fake API server got.
type requestLog struct {
	mu   sync.Mutex
	uris []string
}

func (l *requestLog) add(uri string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.uris = append(l.uris, uri)
}

// take returns the recorded URIs and starts over.
func (l *requestLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	uris := l.uris
	l.uris = nil

	return uris
}

// fakeAPIServer stands in for the Kubernetes API: it lists the leader pod and
// answers pods/proxy requests to its "self" port with the controller's routes.
func fakeAPIServer(t *testing.T, log *requestLog) *httptest.Server {
	t.Helper()

	const proxy = "/api/v1/namespaces/d8-system/pods/deckhouse-0:4222/proxy"

	queues := readFixture(t, "queues.json", nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r.URL.RequestURI())

		switch r.URL.Path {
		case "/api/v1/namespaces/d8-system/pods":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"kind":"PodList","apiVersion":"v1","items":[{"metadata":{"name":"deckhouse-0","namespace":"d8-system"},`+
				`"spec":{"containers":[{"name":"deckhouse","ports":[{"name":"self","containerPort":4222}]}]}}]}`)
		case proxy + "/healthz":
			fmt.Fprint(w, "ok")
		case proxy + "/readyz":
			http.Error(w, "Startup converge in progress", http.StatusInternalServerError)
		case proxy + "/api/v1/queues/dump":
			if r.URL.Query().Get("output") == "yaml" {
				fmt.Fprint(w, "queues: {}\n")

				return
			}

			_, _ = w.Write(queues)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func runAPI(t *testing.T, server *httptest.Server, args ...string) (string, error) {
	t.Helper()

	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, fmt.Appendf(nil, `apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: %s
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
current-context: fake
users:
- name: fake
  user: {}
`, server.URL), 0o600))

	system := &cobra.Command{Use: "system"}
	flags.AddPersistentFlags(system)
	system.AddCommand(NewCommand())

	var out bytes.Buffer

	system.SetOut(&out)
	system.SetErr(&out)
	system.SetArgs(append([]string{"api", "--kubeconfig", kubeconfig}, args...))
	system.SilenceUsage = true
	system.SilenceErrors = true

	err := system.Execute()

	return out.String(), err
}

func TestCommandsGoThroughPodsProxy(t *testing.T) {
	var log requestLog

	server := fakeAPIServer(t, &log)

	out, err := runAPI(t, server, "queues", "dump", "-o", "text")
	require.NoError(t, err)
	require.Equal(t, queuesText, out)
	require.Equal(t, []string{
		"/api/v1/namespaces/d8-system/pods?labelSelector=app%3Ddeckhouse%2Cleader%3Dtrue",
		"/api/v1/namespaces/d8-system/pods/deckhouse-0:4222/proxy/api/v1/queues/dump?output=json",
	}, log.take())

	out, err = runAPI(t, server, "queues", "dump", "--name", "prometheus")
	require.NoError(t, err)
	require.Equal(t, "queues: {}\n", out, "yaml is passed through as the controller encodes it")
	require.Equal(t, "/api/v1/namespaces/d8-system/pods/deckhouse-0:4222/proxy/api/v1/queues/dump?name=prometheus&output=yaml", log.take()[1])

	out, err = runAPI(t, server, "healthz")
	require.NoError(t, err)
	require.Equal(t, "ok\n", out)

	out, err = runAPI(t, server, "readyz")
	require.ErrorIs(t, err, apiclient.ErrNotReady)
	require.EqualError(t, err, "not ready: Startup converge in progress")
	require.Empty(t, out)

	_, err = runAPI(t, server, "scheduler", "dump", "-o", "table")
	require.EqualError(t, err, `unknown output "table", want yaml, json or text`)

	_, err = runAPI(t, server, "requirements", "dump", "-o", "text")
	require.EqualError(t, err, `unknown output "text", want yaml or json`)

	_, err = runAPI(t, server, "get", "api/v1/queues/dump")
	require.ErrorContains(t, err, "PATH must be an absolute path")
}
