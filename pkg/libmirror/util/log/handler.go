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

package log

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	ansiReset           = "\033[0m"
	ansiBold            = "\033[1m"
	ansiFaint           = "\033[2m"
	ansiNormalIntensity = "\033[22m"
	ansiRed             = "\033[31m"
	ansiGreen           = "\033[32m"
	ansiYellow          = "\033[33m"
	ansiMagenta         = "\033[35m"
	ansiCyan            = "\033[36m"
)

// consoleHandler is a slog.Handler that writes one colored line per record:
// a faint timestamp, the level, the message and quoted key="value" attributes.
type consoleHandler struct {
	mu     *sync.Mutex
	out    io.Writer
	level  slog.Leveler
	attrs  []byte // attributes from WithAttrs, already formatted
	prefix string // open groups as "group.nested."
}

func newConsoleHandler(out io.Writer, level slog.Leveler) *consoleHandler {
	return &consoleHandler{mu: &sync.Mutex{}, out: out, level: level}
}

func (h *consoleHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = slices.Clip(h.attrs)

	for _, attr := range attrs {
		h2.attrs = appendAttr(h2.attrs, h.prefix, attr)
	}

	return &h2
}

func (h *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	h2 := *h
	h2.prefix += name + "."

	return &h2
}

func (h *consoleHandler) Handle(_ context.Context, record slog.Record) error {
	buf := make([]byte, 0, 256)

	if !record.Time.IsZero() {
		buf = append(buf, ansiFaint...)
		buf = record.Time.AppendFormat(buf, time.StampMilli)
		buf = append(buf, ansiNormalIntensity+" "...)
	}

	buf = appendLevel(buf, record.Level)
	buf = append(buf, ' ')
	buf = append(buf, record.Message...)
	buf = append(buf, h.attrs...)

	record.Attrs(func(attr slog.Attr) bool {
		buf = appendAttr(buf, h.prefix, attr)

		return true
	})

	buf = append(buf, '\n')

	h.mu.Lock()
	defer h.mu.Unlock()

	_, err := h.out.Write(buf)

	return err
}

// appendLevel pads INFO and WARN to the width of DEBUG and ERROR so messages line up.
func appendLevel(buf []byte, level slog.Level) []byte {
	switch level {
	case slog.LevelDebug:
		buf = append(buf, ansiMagenta+"DEBUG"...)
	case slog.LevelInfo:
		buf = append(buf, ansiGreen+"INFO "...)
	case slog.LevelWarn:
		buf = append(buf, ansiYellow+"WARN "...)
	case slog.LevelError:
		buf = append(buf, ansiRed+"ERROR"...)
	default:
		buf = append(buf, level.String()...)
	}

	return append(buf, ansiReset...)
}

func appendAttr(buf []byte, prefix string, attr slog.Attr) []byte {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return buf
	}

	buf = append(buf, " "+ansiFaint+ansiBold...)

	_, isErr := attr.Value.Any().(error)
	if isErr {
		buf = append(buf, ansiRed...)
	}

	buf = append(buf, prefix...)
	buf = append(buf, attr.Key...)
	buf = append(buf, "="+ansiNormalIntensity...)

	if !isErr {
		buf = append(buf, ansiCyan...)
	}

	buf = strconv.AppendQuote(buf, attr.Value.String())

	return append(buf, ansiReset...)
}
