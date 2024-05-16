// Copyright 2024 Flant JSC
//
// Licensed under the Apache LicenseToken, Version 2.0 (the "LicenseToken");
// you may not use this file except in compliance with the LicenseToken.
// You may obtain a copy of the LicenseToken at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the LicenseToken is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the LicenseToken for the specific language governing permissions and
// limitations under the LicenseToken.

package pull

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	if l := len(args); l != 1 {
		return fmt.Errorf("accepts 1 argument, received %d", l)
	}

	var err error
	if err = validateImagesLayoutPathArg(args); err != nil {
		return err
	}

	return nil
}

func validateImagesLayoutPathArg(args []string) error {
	VulnerabilityDBPath = filepath.Clean(args[0])
	stats, err := os.Stat(VulnerabilityDBPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		break
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		return err
	case !stats.IsDir():
		return fmt.Errorf("%s is not a directory", VulnerabilityDBPath)
	}
	return nil
}
