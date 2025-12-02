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

// StatusReport represents the complete cluster status
type StatusReport struct {
	Masters       StatusSection
	DeckhousePods StatusSection
	Releases      StatusSection
	Edition       StatusSection
	Settings      StatusSection
	Registry      StatusSection
	ClusterAlerts StatusSection
	CNIModules    StatusSection
	Queue         StatusSection
}

// StatusSection represents a single status section
type StatusSection struct {
	Title  string
	Output string
	Error  error
}

// GetAllSections returns all sections as a slice
func (r *StatusReport) GetAllSections() []StatusSection {
	return []StatusSection{
		r.Masters,
		r.DeckhousePods,
		r.Releases,
		r.Edition,
		r.Settings,
		r.Registry,
		r.ClusterAlerts,
		r.CNIModules,
		r.Queue,
	}
}
