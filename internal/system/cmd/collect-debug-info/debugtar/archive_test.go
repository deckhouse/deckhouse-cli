package debugtar

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// TestWriteToTarConcatenatesChunks guards the contract that lets the
// "Defaulted container" notice be stored without copying the collected output:
// the entry header must declare the summed size of all chunks. A size that
// disagrees with the bytes written does not produce a wrong file, it produces a
// broken archive -- tar.Writer refuses the extra bytes or reports the missing
// ones on Close, and every entry after this one is lost.
func TestWriteToTarConcatenatesChunks(t *testing.T) {
	tests := []struct {
		name   string
		chunks [][]byte
		want   string
	}{
		{
			name:   "notice in front of the output",
			chunks: [][]byte{[]byte("Defaulted container \"virt-handler\" out of: virt-handler, kube-rbac-proxy\n"), []byte("log line\n")},
			want:   "Defaulted container \"virt-handler\" out of: virt-handler, kube-rbac-proxy\nlog line\n",
		},
		{
			name:   "no notice, the common case",
			chunks: [][]byte{[]byte(""), []byte("log line\n")},
			want:   "log line\n",
		},
		{
			name:   "command produced nothing",
			chunks: [][]byte{[]byte(""), nil},
			want:   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var archive bytes.Buffer

			tarWriter := tar.NewWriter(&archive)
			entry := command{File: "d8-virtualization-virt-handler-logs.txt"}

			if err := entry.writeToTar(tarWriter, test.chunks...); err != nil {
				t.Fatalf("writeToTar: %v", err)
			}

			// Close reports a header size that disagrees with the bytes written.
			if err := tarWriter.Close(); err != nil {
				t.Fatalf("finalize tar: %v", err)
			}

			reader := tar.NewReader(&archive)

			header, err := reader.Next()
			if err != nil {
				t.Fatalf("read header: %v", err)
			}

			if header.Name != entry.File {
				t.Errorf("entry name = %q, want %q", header.Name, entry.File)
			}

			if header.Size != int64(len(test.want)) {
				t.Errorf("header size = %d, want %d", header.Size, len(test.want))
			}

			content, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read content: %v", err)
			}

			if string(content) != test.want {
				t.Errorf("content = %q, want %q", content, test.want)
			}
		})
	}
}

// TestValidateCommandsRejectsBrokenTables covers the mistakes that a command
// table can carry into the archive: two entries under one name (tar stores
// both, extraction keeps only the last) and a file name still holding the
// {module-name} template. Neither is visible in the produced archive, which is
// why they are rejected before any command runs.
func TestValidateCommandsRejectsBrokenTables(t *testing.T) {
	tests := []struct {
		name     string
		commands []command
		wantErr  string
	}{
		{
			name:     "duplicate entry name",
			commands: []command{{File: "cluster-nodes.json"}, {File: "cluster-nodes.json"}},
			wantErr:  "duplicate archive entry",
		},
		{
			name:     "placeholder left in the file name",
			commands: []command{{File: "d8-{module-name}-ccm-logs.txt"}},
			wantErr:  "unresolved {module-name} placeholder",
		},
		{
			name:     "entry name reserved for the error report",
			commands: []command{{File: collectionErrorsFile}},
			wantErr:  "reserved",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCommands(test.commands)
			if err == nil {
				t.Fatalf("validateCommands accepted %v", test.commands)
			}

			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error = %v, want it to mention %q", err, test.wantErr)
			}
		})
	}
}

// TestBuiltinCommandTablesProduceValidArchives runs the real tables through the
// same validation the collection does, in the shapes they actually reach it:
// the cluster-wide table after module expansion, and the virtualization table
// after the per-pod log commands are generated.
func TestBuiltinCommandTablesProduceValidArchives(t *testing.T) {
	activeModules := map[string]bool{
		"cloud-provider-aws":    true,
		"cloud-provider-yandex": true,
		"cert-manager":          true,
		"istio":                 true,
		"cni-cilium":            true,
		"virtualization":        true,
	}

	expanded, _ := filterAndExpandCommands(debugCommands, activeModules, true, nil)
	if err := validateCommands(expanded); err != nil {
		t.Errorf("expanded cluster-wide commands: %v", err)
	}

	pods := []virtualizationPod{
		{Name: "virt-handler-abcde"},
		{Name: "virt-handler-fghij"},
		{Name: "virtualization-controller-0"},
		{Name: "dvcr-0"},
	}

	if err := validateCommands(buildVirtualizationCommands(pods, false)); err != nil {
		t.Errorf("virtualization commands: %v", err)
	}
}

// TestFormatCollectionErrorsNamesIncompleteEntries guards the only reason
// collection-errors.txt exists: the archive has to name the entries it could
// not fill. The warnings printed while collecting go to stderr, which is not
// part of the archive and is gone by the time anyone opens it, so a truncated
// entry would otherwise look exactly like a complete one.
func TestFormatCollectionErrorsNamesIncompleteEntries(t *testing.T) {
	report := string(formatCollectionErrors([]commandFailure{
		{
			file:     "cluster-crd.json",
			command:  "bash -c set -o pipefail; kubectl get customresourcedefinitions -o json | jq ...",
			err:      context.DeadlineExceeded,
			timedOut: true,
			timeout:  2 * time.Minute,
			kept:     4096,
		},
		{
			file:    "d8-istio-resources.json",
			command: "bash -c set -o pipefail; kubectl -n d8-istio get all -o json | jq '.items[]'",
			err:     errors.New("command terminated with exit code 1"),
			stderr:  "Error from server (NotFound): namespaces \"d8-istio\" not found",
		},
	}))

	for _, want := range []string{
		"cluster-crd.json",
		"TIMED OUT after 2m0s",
		"4096 bytes kept",
		"d8-istio-resources.json",
		"exit code 1",
		"namespaces \"d8-istio\" not found",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report does not mention %q:\n%s", want, report)
		}
	}
}

// TestFormatCollectionErrorsCapsQuotedStderr keeps one noisy command from
// turning the report into a second copy of its output.
func TestFormatCollectionErrorsCapsQuotedStderr(t *testing.T) {
	report := string(formatCollectionErrors([]commandFailure{
		{
			file:   "kube-system-etcd-logs.txt",
			err:    errors.New("command terminated with exit code 1"),
			stderr: strings.Repeat("unable to retrieve container logs\n", 10000),
		},
	}))

	if len(report) > 4*reportStderrLimit {
		t.Errorf("report length = %d bytes, want it capped near the %d byte stderr limit", len(report), reportStderrLimit)
	}

	if !strings.Contains(report, "truncated") {
		t.Errorf("report does not say the stderr was truncated:\n%s", report[:200])
	}
}
