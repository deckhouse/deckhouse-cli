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

package usecase

import (
	"context"
	"fmt"
	"io"

	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
)

// ResourceFilter filters resources during backup
type ResourceFilter interface {
	Matches(obj domain.K8sObject) bool
}

// FileSystem provides file operations for backup
type FileSystem interface {
	// CreateTemp creates a temporary file
	CreateTemp(dir, pattern string) (WritableFile, error)
	// Rename moves a file
	Rename(oldpath, newpath string) error
	// Remove removes a file
	Remove(path string) error
}

// WritableFile represents a writable file
type WritableFile interface {
	io.Writer
	Name() string
	Sync() error
	Close() error
}

// ClusterConfigBackupUseCase handles cluster config backup operations
type ClusterConfigBackupUseCase struct {
	k8s           K8sClient
	fs            FileSystem
	tarballWriter func(path string, compress bool) (TarballWriter, error)
	filter        ResourceFilter
	logger        Logger
}

// NewClusterConfigBackupUseCase creates a new ClusterConfigBackupUseCase
func NewClusterConfigBackupUseCase(
	k8s K8sClient,
	fs FileSystem,
	tarballWriter func(path string, compress bool) (TarballWriter, error),
	filter ResourceFilter,
	logger Logger,
) *ClusterConfigBackupUseCase {
	return &ClusterConfigBackupUseCase{
		k8s:           k8s,
		fs:            fs,
		tarballWriter: tarballWriter,
		filter:        filter,
		logger:        logger,
	}
}

// Execute performs cluster config backup
func (uc *ClusterConfigBackupUseCase) Execute(ctx context.Context, params *domain.ClusterConfigBackupParams) (*domain.BackupResult, error) {
	result := &domain.BackupResult{
		Type: domain.BackupTypeClusterConfig,
		Path: params.TarballPath,
	}

	// Get namespaces
	namespaces, err := uc.k8s.ListNamespaces(ctx)
	if err != nil {
		result.Error = fmt.Errorf("list namespaces: %w", err)
		return result, result.Error
	}

	// Create temp file
	tarFile, err := uc.fs.CreateTemp(".", ".*.d8tmp")
	if err != nil {
		result.Error = fmt.Errorf("create temp file: %w", err)
		return result, result.Error
	}
	tempName := tarFile.Name()
	defer uc.fs.Remove(tempName)

	// Create tarball writer using the file directly
	backup, err := uc.tarballWriter(tempName, params.Compress)
	if err != nil {
		tarFile.Close()
		result.Error = fmt.Errorf("create tarball writer: %w", err)
		return result, result.Error
	}

	// Backup stages
	type backupStage struct {
		name   string
		fetch  func(ctx context.Context, namespaces []string) ([]domain.K8sObject, error)
		filter bool
	}

	stages := []backupStage{
		{"secrets", func(ctx context.Context, ns []string) ([]domain.K8sObject, error) { return uc.k8s.ListSecrets(ctx, ns) }, true},
		{"configmaps", func(ctx context.Context, ns []string) ([]domain.K8sObject, error) { return uc.k8s.ListConfigMaps(ctx, ns) }, true},
		{"custom-resources", func(ctx context.Context, _ []string) ([]domain.K8sObject, error) { return uc.k8s.ListCustomResources(ctx) }, false},
		{"cluster-roles", func(ctx context.Context, _ []string) ([]domain.K8sObject, error) { return uc.k8s.ListClusterRoles(ctx) }, false},
		{"cluster-role-bindings", func(ctx context.Context, _ []string) ([]domain.K8sObject, error) { return uc.k8s.ListClusterRoleBindings(ctx) }, false},
		{"storage-classes", func(ctx context.Context, _ []string) ([]domain.K8sObject, error) { return uc.k8s.ListStorageClasses(ctx) }, false},
	}

	for _, stage := range stages {
		objects, err := stage.fetch(ctx, namespaces)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s failed: %v", stage.name, err))
			continue
		}

		for _, obj := range objects {
			if stage.filter && uc.filter != nil && !uc.filter.Matches(obj) {
				continue
			}
			if err := backup.PutObject(obj); err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: put object failed: %v", stage.name, err))
			}
		}
	}

	if err := backup.Close(); err != nil {
		result.Error = fmt.Errorf("close tarball: %w", err)
		return result, result.Error
	}

	tarFile.Close()

	if err := uc.fs.Rename(tempName, params.TarballPath); err != nil {
		result.Error = fmt.Errorf("move tarball: %w", err)
		return result, result.Error
	}

	result.Success = true
	if len(result.Warnings) > 0 {
		uc.logger.Warn("Some backup procedures failed, only successfully backed-up resources will be available",
			"warnings", result.Warnings)
	}

	return result, nil
}

