// This package is copied to this repository to use constants in functions
// Original - deckhouse/deckhouse-controller/pkg/apis/deckhouse.io/v1alpha1/module.go

package v1alpha1

const (
	ModuleResource = "modules"
	ModuleKind     = "Module"

	ModuleSourceEmbedded = "Embedded"

	ModuleAnnotationDescriptionRu = "ru.meta.deckhouse.io/description"
	ModuleAnnotationDescriptionEn = "en.meta.deckhouse.io/description"

	ModuleConditionEnabledByModuleConfig  = "EnabledByModuleConfig"
	ModuleConditionEnabledByModuleManager = "EnabledByModuleManager"
	ModuleConditionLastReleaseDeployed    = "LastReleaseDeployed"
	ModuleConditionIsReady                = "IsReady"
	ModuleConditionIsOverridden           = "IsOverridden"

	ModulePhaseAvailable        = "Available"
	ModulePhaseDownloading      = "Downloading"
	ModulePhaseDownloadingError = "DownloadingError"
	ModulePhaseReconciling      = "Reconciling"
	ModulePhaseInstalling       = "Installing"
	ModulePhaseDownloaded       = "Downloaded"
	ModulePhaseConflict         = "Conflict"
	ModulePhaseReady            = "Ready"
	ModulePhaseError            = "Error"

	ModuleReasonBundle                      = "Bundle"
	ModuleReasonModuleConfig                = "ModuleConfig"
	ModuleReasonDynamicGlobalHookExtender   = "DynamicGlobalHookExtender"
	ModuleReasonEnabledScriptExtender       = "EnabledScriptExtender"
	ModuleReasonDeckhouseVersionExtender    = "DeckhouseVersionExtender"
	ModuleReasonKubernetesVersionExtender   = "KubernetesVersionExtender"
	ModuleReasonClusterBootstrappedExtender = "ClusterBootstrappedExtender"
	ModuleReasonModuleDependencyExtender    = "ModuleDependencyExtender"
	ModuleReasonNotInstalled                = "NotInstalled"
	ModuleReasonDisabled                    = "Disabled"
	ModuleReasonConflict                    = "Conflict"
	ModuleReasonDownloading                 = "Downloading"
	ModuleReasonHookError                   = "HookError"
	ModuleReasonModuleError                 = "ModuleError"
	ModuleReasonReconciling                 = "Reconciling"
	ModuleReasonInstalling                  = "Installing"
	ModuleReasonError                       = "Error"

	ModuleMessageBundle                      = "turned off by bundle"
	ModuleMessageModuleConfig                = "turned off by module config"
	ModuleMessageDynamicGlobalHookExtender   = "turned off by global hook"
	ModuleMessageEnabledScriptExtender       = "turned off by enabled script"
	ModuleMessageDeckhouseVersionExtender    = "turned off by deckhouse version"
	ModuleMessageKubernetesVersionExtender   = "turned off by kubernetes version"
	ModuleMessageClusterBootstrappedExtender = "turned off because the cluster not bootstrapped yet"
	ModuleMessageModuleDependencyExtender    = "turned off because of unmet module dependencies"
	ModuleMessageNotInstalled                = "not installed"
	ModuleMessageDisabled                    = "disabled"
	ModuleMessageConflict                    = "several available sources"
	ModuleMessageDownloading                 = "downloading"
	ModuleMessageReconciling                 = "reconciling"
	ModuleMessageInstalling                  = "installing"
	ModuleMessageOnStartupHook               = "onStartup hooks done"

	DeckhouseRequirementFieldName        string = "deckhouse"
	KubernetesRequirementFieldName       string = "kubernetes"
	BootstrappedRequirementFieldName     string = "bootstrapped"
	ModuleDependencyRequirementFieldName string = "modules"
)
