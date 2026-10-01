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

package pkg

// Edition is the last segment of a Deckhouse edition repo, e.g. the "ee" of
// registry.deckhouse.io/deckhouse/ee. The installer and deckhouse-cli are
// published once for all editions, in the root above it: pull reads them from
// there and push writes them there.
//
// The in-cluster registry-packages-proxy keeps its own list of editions to
// find deckhouse-cli above a cluster's edition repo; keep the two in sync.
type Edition string

const (
	EEEdition     Edition = "ee"
	FEEdition     Edition = "fe"
	SEEdition     Edition = "se"
	BEEdition     Edition = "be"
	SEPlusEdition Edition = "se-plus"
	CEEdition     Edition = "ce"
	CSEEdition    Edition = "cse"
	NoEdition     Edition = ""
)

func (e Edition) String() string {
	return string(e)
}

func (e Edition) IsValid() bool {
	switch e {
	case EEEdition, FEEdition, SEEdition, BEEdition, SEPlusEdition, CEEdition, CSEEdition:
		return true
	default:
		return false
	}
}
