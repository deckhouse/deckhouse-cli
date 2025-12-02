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

package export

import (
	"github.com/spf13/cobra"
)

// Config holds common configuration for export commands
type Config struct {
	Namespace string
	Publish   bool
	TTL       string
}

// CreateConfig holds configuration for create command
type CreateConfig struct {
	Config
	Name      string
	VolumeRef string
}

// DeleteConfig holds configuration for delete command
type DeleteConfig struct {
	Name      string
	Namespace string
}

// DownloadConfig holds configuration for download command
type DownloadConfig struct {
	Config
	DataName string
	SrcPath  string
	DstPath  string
}

// ListConfig holds configuration for list command
type ListConfig struct {
	Config
	DataName string
	Path     string
}

// BindFlags binds common flags to Config
func (c *Config) BindFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&c.Namespace, "namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().BoolVar(&c.Publish, "publish", false, "Provide access outside of cluster")
	cmd.Flags().StringVar(&c.TTL, "ttl", "2m", "Time to live")
}

// BindDeleteFlags binds flags for delete command
func (c *DeleteConfig) BindFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&c.Namespace, "namespace", "n", "d8-data-exporter", "data volume namespace")
}

// BindDownloadFlags binds flags for download command
func (c *DownloadConfig) BindDownloadFlags(cmd *cobra.Command) {
	c.Config.BindFlags(cmd)
	cmd.Flags().StringVarP(&c.DstPath, "output", "o", "", "file to save data (default: same as resource)")
}

