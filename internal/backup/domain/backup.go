/*
Copyright 2024 Flant JSC

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

package domain

// BackupType represents the type of backup operation
type BackupType string

const (
	BackupTypeETCD          BackupType = "etcd"
	BackupTypeClusterConfig BackupType = "cluster-config"
	BackupTypeLoki          BackupType = "loki"
)

// ETCDBackupParams contains parameters for ETCD backup
type ETCDBackupParams struct {
	SnapshotPath string
	PodName      string // optional, specific pod to use
	Verbose      bool
}

// ClusterConfigBackupParams contains parameters for cluster config backup
type ClusterConfigBackupParams struct {
	TarballPath string
	Compress    bool
}

// LokiBackupParams contains parameters for Loki logs backup
type LokiBackupParams struct {
	StartTimestamp string
	EndTimestamp   string
	Limit          string
	ChunkDays      int
}

// BackupResult represents the result of a backup operation
type BackupResult struct {
	Type     BackupType
	Path     string
	Success  bool
	Error    error
	Warnings []string
}

// PodInfo contains pod information
type PodInfo struct {
	Name       string
	Namespace  string
	Ready      bool
	Containers []string
}

// K8sObject represents a Kubernetes object for backup operations
// This abstraction decouples usecase layer from k8s runtime.Object
type K8sObject interface {
	// GetName returns the object name
	GetName() string
	// GetNamespace returns the object namespace (empty for cluster-scoped)
	GetNamespace() string
	// GetKind returns the object kind
	GetKind() string
	// GetAPIVersion returns the API version
	GetAPIVersion() string
	// MarshalYAML serializes the object to YAML
	MarshalYAML() ([]byte, error)
}

