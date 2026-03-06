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

type Edition string

const (
	EEEdition     Edition = "ee"
	FEEdition     Edition = "fe"
	SEEdition     Edition = "se"
	BEEdition     Edition = "be"
	SEPlusEdition Edition = "se-plus"
	CEEdition     Edition = "ce"
	NoEdition     Edition = ""
)

func (e Edition) String() string {
	return string(e)
}

func (e Edition) IsValid() bool {
	switch e {
	case EEEdition, FEEdition, SEEdition, BEEdition, SEPlusEdition, CEEdition:
		return true
	default:
		return false
	}
}
