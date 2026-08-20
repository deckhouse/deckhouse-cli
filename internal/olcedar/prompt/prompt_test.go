/*
Copyright 2026 Flant JSC

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

package prompt

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func asking(answers string) (*Prompt, *bytes.Buffer) {
	out := &bytes.Buffer{}

	return New(strings.NewReader(answers), out, false), out
}

func TestChooseTakesTheNumberTyped(t *testing.T) {
	p, out := asking("2\n")

	index, err := p.Choose("Disks:", []string{"nvme0n1", "sda"}, 0)
	require.NoError(t, err)
	require.Equal(t, 1, index)
	require.Contains(t, out.String(), "1) nvme0n1")
	require.Contains(t, out.String(), "2) sda")
}

func TestChooseAsksAgainAfterAnAnswerOutOfRange(t *testing.T) {
	p, out := asking("7\nx\n1\n")

	index, err := p.Choose("Disks:", []string{"nvme0n1", "sda"}, NoDefault)
	require.NoError(t, err)
	require.Equal(t, 0, index)
	require.Equal(t, 2, strings.Count(out.String(), "Answer with a number between 1 and 2."))
}

func TestChooseTakesTheDefaultOnAnEmptyLine(t *testing.T) {
	p, _ := asking("\n")

	index, err := p.Choose("Disks:", []string{"nvme0n1", "sda"}, 1)
	require.NoError(t, err)
	require.Equal(t, 1, index)
}

// Assuming an answer is only allowed where there is one to assume.
func TestChooseRefusesToAssumeWithoutADefault(t *testing.T) {
	p := New(strings.NewReader(""), &bytes.Buffer{}, true)

	_, err := p.Choose("Disks:", []string{"nvme0n1", "sda"}, NoDefault)
	require.ErrorIs(t, err, ErrNoDefault)

	index, err := p.Choose("Disks:", []string{"nvme0n1", "sda"}, 1)
	require.NoError(t, err)
	require.Equal(t, 1, index)
}

func TestConfirm(t *testing.T) {
	p, _ := asking("y\n")
	answer, err := p.Confirm("Erase it?", false)
	require.NoError(t, err)
	require.True(t, answer)

	p, _ = asking("\n")
	answer, err = p.Confirm("Erase it?", false)
	require.NoError(t, err)
	require.False(t, answer)

	p, out := asking("maybe\nno\n")
	answer, err = p.Confirm("Erase it?", true)
	require.NoError(t, err)
	require.False(t, answer)
	require.Contains(t, out.String(), "Answer y or n.")
}

func TestConfirmAssumesItsDefault(t *testing.T) {
	p := New(strings.NewReader(""), &bytes.Buffer{}, true)

	answer, err := p.Confirm("Erase it?", false)
	require.NoError(t, err)
	require.False(t, answer)
}

func TestLineFallsBackToTheDefault(t *testing.T) {
	p, out := asking("\n")

	answer, err := p.Line("Node name", "worker-1")
	require.NoError(t, err)
	require.Equal(t, "worker-1", answer)
	require.Contains(t, out.String(), "Node name [worker-1]: ")

	p, _ = asking("worker-7\n")
	answer, err = p.Line("Node name", "worker-1")
	require.NoError(t, err)
	require.Equal(t, "worker-7", answer)
}

func TestAskReportsInputThatEnded(t *testing.T) {
	p, _ := asking("")

	_, err := p.Line("Node name", "")
	require.ErrorContains(t, err, "the input ended")
}
