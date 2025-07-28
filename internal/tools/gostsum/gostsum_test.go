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

package gostsum

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	streebog256 "go.cypherpunks.ru/gogost/v5/gost34112012256"
	streebog512 "go.cypherpunks.ru/gogost/v5/gost34112012512"
)

func Test_gostsums_HashCompatibility256(t *testing.T) {
	input := "012345678901234567890123456789012345678901234567890123456789012"
	gostsumHash := "9d151eefd8590b89daa6ba6cb74af9275dd051026bb149a452fd84e5e57b5500"

	gogostHash, err := digest(strings.NewReader(input), streebog256.New())
	require.NoError(t, err)
	require.Equal(t, gostsumHash, gogostHash)
}

func Test_gostsums_HashCompatibility512(t *testing.T) {
	input := "012345678901234567890123456789012345678901234567890123456789012"
	gostsumHash := "1b54d01a4af5b9d5cc3d86d68d285462b19abc2475222f35c085122be4ba1ffa00ad30f8767b3a82384c6574f024c311e2a481332b08ef7f41797891c1646f48"

	gogostHash, err := digest(strings.NewReader(input), streebog512.New())
	require.NoError(t, err)
	require.Equal(t, gostsumHash, gogostHash)
}
