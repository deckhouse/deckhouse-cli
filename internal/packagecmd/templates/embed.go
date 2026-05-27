package templates

import (
	"embed"
	"io/fs"
)

//go:embed all:module
var moduleFS embed.FS

// ModuleFS contains module template files, rooted at the module directory.
var ModuleFS, _ = fs.Sub(moduleFS, "module")

//go:embed all:application
var applicationFS embed.FS

// ApplicationFS contains application template files, rooted at the application directory.
var ApplicationFS, _ = fs.Sub(applicationFS, "application")

//go:embed all:werf
var werfFS embed.FS

// WerfFS contains werf templates for build, rooted at the werf directory.
var WerfFS, _ = fs.Sub(werfFS, "werf")
