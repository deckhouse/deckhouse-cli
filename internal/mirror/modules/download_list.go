/*
Copyright 2025 Flant JSC

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

package modules

// ModuleDownloadList tracks images to be downloaded for a single module
type ModuleDownloadList struct {
	// ReleaseChannels holds release channel image references
	ReleaseChannels map[string]struct{}
	// Images holds main module image references
	Images map[string]struct{}
	// ExtraImages holds extra image references
	ExtraImages map[string]struct{}
}

// NewModuleDownloadList creates a new download list for a module
func NewModuleDownloadList() *ModuleDownloadList {
	return &ModuleDownloadList{
		ReleaseChannels: make(map[string]struct{}),
		Images:          make(map[string]struct{}),
		ExtraImages:     make(map[string]struct{}),
	}
}

// ModulesDownloadListNew tracks images for all modules
type ModulesDownloadListNew struct {
	rootURL string
	modules map[string]*ModuleDownloadList
}

// NewModulesDownloadListNew creates a new modules download list
func NewModulesDownloadListNew(rootURL string) *ModulesDownloadListNew {
	return &ModulesDownloadListNew{
		rootURL: rootURL,
		modules: make(map[string]*ModuleDownloadList),
	}
}

// ForModule returns or creates a download list for a specific module
func (dl *ModulesDownloadListNew) ForModule(moduleName string) *ModuleDownloadList {
	if _, ok := dl.modules[moduleName]; !ok {
		dl.modules[moduleName] = NewModuleDownloadList()
	}
	return dl.modules[moduleName]
}

// AllModules returns all module download lists
func (dl *ModulesDownloadListNew) AllModules() map[string]*ModuleDownloadList {
	return dl.modules
}

