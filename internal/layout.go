package internal

// deckhouse repo structure
// root-segment:<version>
// root-segment/install:<version>
// root-segment/install-standalone:<version>
// root-segment/release-channel:<version>
// root-segment/modules/<module-name>:<version>
// root-segment/modules/<module-name>/releases:<version>
// root-segment/modules/<module-name>/extra/<module-extra-name>:<version>
const (
	InstallSegment           = "install"
	InstallStandaloneSegment = "install-standalone"
	ReleaseChannelSegment    = "release-channel"
	ModulesSegment           = "modules"
	ModulesExtraSegment      = "extra"
	ModulesReleasesSegment   = "releases"
)
