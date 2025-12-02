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

package dataimport

import (
	"github.com/spf13/cobra"
)

// Config holds common configuration for import commands
type Config struct {
	Namespace string
	Publish   bool
	TTL       string
}

// CreateConfig holds configuration for create command
type CreateConfig struct {
	Config
	Name        string
	PVCFilePath string
	WFFC        bool
}

// DeleteConfig holds configuration for delete command
type DeleteConfig struct {
	Name      string
	Namespace string
}

// UploadConfig holds configuration for upload command
type UploadConfig struct {
	Name      string
	Namespace string
	FilePath  string
	DstPath   string
	Publish   bool
	Chunks    int
	Resume    bool
}

// BindFlags binds common flags to Config
func (c *Config) BindFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&c.Namespace, "namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().BoolVar(&c.Publish, "publish", false, "Provide access outside of cluster")
	cmd.Flags().StringVar(&c.TTL, "ttl", "2m", "Time to live")
}

// BindCreateFlags binds flags for create command
func (c *CreateConfig) BindCreateFlags(cmd *cobra.Command) {
	c.Config.BindFlags(cmd)
	cmd.Flags().StringVarP(&c.PVCFilePath, "file", "f", "", "PVC manifest file path")
	cmd.Flags().BoolVar(&c.WFFC, "wffc", false, "Wait for first consumer")
}

// BindDeleteFlags binds flags for delete command
func (c *DeleteConfig) BindFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&c.Namespace, "namespace", "n", "d8-data-exporter", "data volume namespace")
}

// BindUploadFlags binds flags for upload command
func (c *UploadConfig) BindUploadFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&c.Namespace, "namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().StringVarP(&c.FilePath, "file", "f", "", "file to upload")
	cmd.Flags().StringVarP(&c.DstPath, "dstPath", "d", "", "destination path of the uploaded file")
	cmd.Flags().IntVarP(&c.Chunks, "chunks", "c", 10, "number of chunks to upload")
	cmd.Flags().BoolVarP(&c.Publish, "publish", "P", false, "publish the uploaded file")
	cmd.Flags().BoolVar(&c.Resume, "resume", false, "resume upload if process was interrupted")
}

