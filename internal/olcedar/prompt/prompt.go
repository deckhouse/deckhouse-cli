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

// Package prompt asks the questions a machine cannot answer for itself. Every
// question reads from an io.Reader and writes to an io.Writer, so the whole
// command is testable without a terminal.
package prompt

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrNoDefault means an answer was assumed but there is nothing to assume: the
// caller names the flag that would carry it.
var ErrNoDefault = errors.New("there is no default to assume")

// NoDefault is the index Choose takes when no option may be picked silently.
const NoDefault = -1

// Prompt asks on a stream. Assume answers every question with its default
// instead of asking, which is what --yes means.
type Prompt struct {
	in     *bufio.Reader
	out    io.Writer
	assume bool
}

func New(in io.Reader, out io.Writer, assume bool) *Prompt {
	return &Prompt{in: bufio.NewReader(in), out: out, assume: assume}
}

// Printf writes to the same stream the questions go to.
func (p *Prompt) Printf(format string, args ...any) {
	fmt.Fprintf(p.out, format, args...)
}

// Choose shows a numbered list and returns the index picked. defaultIndex of
// NoDefault means the answer has to be typed: nothing here may be picked for
// the operator.
func (p *Prompt) Choose(title string, options []string, defaultIndex int) (int, error) {
	if len(options) == 0 {
		return 0, fmt.Errorf("%s: there is nothing to choose from", title)
	}

	if p.assume {
		if defaultIndex == NoDefault {
			return 0, fmt.Errorf("%s: %w", title, ErrNoDefault)
		}

		return defaultIndex, nil
	}

	fmt.Fprintf(p.out, "\n%s\n\n", title)

	for i, option := range options {
		fmt.Fprintf(p.out, "  %d) %s\n", i+1, option)
	}

	for {
		answer, err := p.ask(question("Choice", defaultLabel(defaultIndex)))
		if err != nil {
			return 0, err
		}

		if answer == "" && defaultIndex != NoDefault {
			return defaultIndex, nil
		}

		number, err := strconv.Atoi(answer)
		if err != nil || number < 1 || number > len(options) {
			fmt.Fprintf(p.out, "Answer with a number between 1 and %d.\n", len(options))

			continue
		}

		return number - 1, nil
	}
}

// Confirm asks a yes/no question. defaultYes decides what an empty line means,
// and a question that may destroy data is asked with defaultYes false.
func (p *Prompt) Confirm(text string, defaultYes bool) (bool, error) {
	if p.assume {
		return defaultYes, nil
	}

	suffix := "y/N"
	if defaultYes {
		suffix = "Y/n"
	}

	for {
		answer, err := p.ask(fmt.Sprintf("%s [%s]: ", text, suffix))
		if err != nil {
			return false, err
		}

		switch strings.ToLower(answer) {
		case "":
			return defaultYes, nil
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		default:
			fmt.Fprintln(p.out, "Answer y or n.")
		}
	}
}

// Line reads one line of text, falling back to defaultValue on an empty answer.
func (p *Prompt) Line(text, defaultValue string) (string, error) {
	if p.assume {
		if defaultValue == "" {
			return "", fmt.Errorf("%s: %w", text, ErrNoDefault)
		}

		return defaultValue, nil
	}

	answer, err := p.ask(question(text, defaultValue))
	if err != nil {
		return "", err
	}

	if answer == "" {
		return defaultValue, nil
	}

	return answer, nil
}

func (p *Prompt) ask(text string) (string, error) {
	fmt.Fprint(p.out, text)

	answer, err := p.in.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read the answer: %w", err)
	}

	if errors.Is(err, io.EOF) && strings.TrimSpace(answer) == "" {
		return "", errors.New("read the answer: the input ended")
	}

	return strings.TrimSpace(answer), nil
}

func question(text, defaultValue string) string {
	if defaultValue == "" {
		return text + ": "
	}

	return fmt.Sprintf("%s [%s]: ", text, defaultValue)
}

func defaultLabel(defaultIndex int) string {
	if defaultIndex == NoDefault {
		return ""
	}

	return strconv.Itoa(defaultIndex + 1)
}
