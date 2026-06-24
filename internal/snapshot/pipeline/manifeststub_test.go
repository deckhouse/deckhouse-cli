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

package pipeline_test

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

// runPipeline runs the download pipeline with the shared in-memory manifest stub
// injected when the caller has not set its own ManifestSource. This replaces the
// production aggregated manifests-download surface in unit tests.
func runPipeline(ctx context.Context, cfg pipeline.Config) error {
	if cfg.ManifestSource == nil {
		cfg.ManifestSource = testManifestSource()
	}

	return pipeline.Run(ctx, cfg)
}

// manifestStub is an in-memory source.ManifestSource keyed by node ref. It replaces
// the live aggregated manifests-download surface in unit tests: each snapshot node's
// own manifests are seeded by ref and returned verbatim. Refs that were not seeded
// return an empty slice (the valid "node has no captured manifests" case).
type manifestStub struct {
	byRef map[string][]unstructured.Unstructured
	err   error
}

func newManifestStub() *manifestStub {
	return &manifestStub{byRef: map[string][]unstructured.Unstructured{}}
}

func stubKey(ref aggapi.NodeRef) string {
	return ref.APIVersion + "|" + ref.Kind + "|" + ref.Name + "|" + ref.Namespace
}

// add seeds objs as the own-node manifests for ref and returns the stub for chaining.
func (m *manifestStub) add(ref aggapi.NodeRef, objs ...unstructured.Unstructured) *manifestStub {
	m.byRef[stubKey(ref)] = append(m.byRef[stubKey(ref)], objs...)

	return m
}

// FetchNodeManifests implements source.ManifestSource.
func (m *manifestStub) FetchNodeManifests(_ context.Context, ref aggapi.NodeRef) ([]unstructured.Unstructured, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.byRef[stubKey(ref)], nil
}

// snapRef builds a core Snapshot node ref.
func snapRef(name, namespace string) aggapi.NodeRef {
	return aggapi.NodeRef{APIVersion: storageAPIVersion, Kind: "Snapshot", Name: name, Namespace: namespace}
}

// nodeRef builds a domain snapshot node ref.
func nodeRef(apiVersion, kind, name, namespace string) aggapi.NodeRef {
	return aggapi.NodeRef{APIVersion: apiVersion, Kind: kind, Name: name, Namespace: namespace}
}

// configMapManifest builds a ConfigMap manifest object.
func configMapManifest(name, namespace string) unstructured.Unstructured {
	meta := map[string]interface{}{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}

	return unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   meta,
	}}
}

// pvcManifest builds a PersistentVolumeClaim manifest object carrying storageClassName
// and volumeMode so shadow-meta resolution can read them.
func pvcManifest(name, namespace, uid, storageClass, volumeMode string) unstructured.Unstructured {
	meta := map[string]interface{}{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}

	if uid != "" {
		meta["uid"] = uid
	}

	return unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata":   meta,
		"spec": map[string]interface{}{
			"storageClassName": storageClass,
			"volumeMode":       volumeMode,
		},
	}}
}

// testManifestSource returns a single stub seeded with the own-node manifests for every
// node across all pipeline unit tests. Node refs are globally unique (distinct names /
// namespaces), so one shared stub serves every test without collisions.
func testManifestSource() *manifestStub {
	return newManifestStub().
		// buildFakeClient tree (pipeline_test.go): root carries one ConfigMap.
		add(snapRef(rootSnapshot, testNS), configMapManifest("test-cfg", testNS)).
		// TestPipeline_ShadowMetaFromManifest: disk-snap own manifests carry the captured PVC.
		add(nodeRef(childAPIVersion, childKind, diskSnapName, testNS),
			pvcManifest(sourcePVCName, testNS, "uid-disk", "csi-ceph-rbd-from-checkpoint", "Block")).
		// buildE2EFakeClient tree (e2e_test.go).
		add(snapRef(e2eRootSnap, e2eNS), configMapManifest(e2eRootCMName, e2eNS)).
		add(nodeRef(e2eVMAPIVersion, e2eVMKind, e2eVMSnap, e2eNS), configMapManifest(e2eVMCMName, e2eNS)).
		// buildDeletedPVCFakeClient tree: del-disk own manifests carry the (deleted-live) PVC.
		add(nodeRef(e2eVMAPIVersion, e2eDiskKind, e2eDelDisk, e2eNS),
			pvcManifest(e2eDelPVC, e2eNS, "uid-del", "csi-del-sc", "Block")).
		// buildOrphanLeafFakeClient tree: aggregator own manifests carry only the ConfigMap;
		// the orphan leaf's PVC manifest is keyed under the leaf's own ManifestScopeRef (VS ref).
		add(nodeRef(e2eVMAPIVersion, e2eDiskKind, "agg-snap", e2eNS),
			configMapManifest("agg-cm", e2eNS)).
		// Orphan leaf PVC manifest keyed by the leaf's ManifestScopeRef (VS CR ref).
		// WriteVolumeManifest fetches from ManifestScopeRef = VolumeSnapshot ref.
		add(nodeRef("snapshot.storage.k8s.io/v1", "VolumeSnapshot", "nss-vs-agg-pvc", e2eNS),
			pvcManifest("pvc-agg", e2eNS, "uid-agg-pvc", "csi-agg-sc", "Block"))
}
