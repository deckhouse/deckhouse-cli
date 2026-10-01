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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsoleHandlerFormat(t *testing.T) {
	ts := time.Date(2026, 10, 1, 9, 5, 7, 123_000_000, time.UTC)

	tests := []struct {
		name  string
		wrap  func(slog.Handler) slog.Handler
		time  time.Time
		level slog.Level
		msg   string
		attrs []slog.Attr
		want  string
	}{
		{
			name:  "info line is padded to the error width",
			time:  ts,
			level: slog.LevelInfo,
			msg:   "╔ Pull images",
			want:  "\x1b[2mOct  1 09:05:07.123\x1b[22m \x1b[32mINFO \x1b[0m ╔ Pull images\n",
		},
		{
			name:  "error keys are red, other values are cyan and quoted",
			time:  ts,
			level: slog.LevelError,
			msg:   "Pull images failed",
			attrs: []slog.Attr{slog.Any("error", errors.New("boom")), slog.String("ref", `a "b"`)},
			want: "\x1b[2mOct  1 09:05:07.123\x1b[22m \x1b[31mERROR\x1b[0m Pull images failed" +
				" \x1b[2m\x1b[1m\x1b[31merror=\x1b[22m\"boom\"\x1b[0m" +
				" \x1b[2m\x1b[1mref=\x1b[22m\x1b[36m\"a \\\"b\\\"\"\x1b[0m\n",
		},
		{
			name: "groups prefix later keys, empty attributes and zero time are omitted",
			wrap: func(h slog.Handler) slog.Handler {
				return h.WithAttrs([]slog.Attr{slog.Int("a", 1)}).WithGroup("g").WithGroup("")
			},
			level: slog.LevelWarn,
			msg:   "warn",
			attrs: []slog.Attr{{}, slog.Bool("b", true)},
			want: "\x1b[33mWARN \x1b[0m warn" +
				" \x1b[2m\x1b[1ma=\x1b[22m\x1b[36m\"1\"\x1b[0m" +
				" \x1b[2m\x1b[1mg.b=\x1b[22m\x1b[36m\"true\"\x1b[0m\n",
		},
		{
			name:  "debug",
			time:  ts,
			level: slog.LevelDebug,
			want:  "\x1b[2mOct  1 09:05:07.123\x1b[22m \x1b[35mDEBUG\x1b[0m \n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer

			var h slog.Handler = newConsoleHandler(&out, slog.LevelDebug)
			if tt.wrap != nil {
				h = tt.wrap(h)
			}

			record := slog.NewRecord(tt.time, tt.level, tt.msg, 0)
			record.AddAttrs(tt.attrs...)

			require.NoError(t, h.Handle(context.Background(), record))
			assert.Equal(t, tt.want, out.String())
		})
	}
}

func TestConsoleHandlerLevel(t *testing.T) {
	var out bytes.Buffer

	logger := slog.New(newConsoleHandler(&out, slog.LevelWarn))
	logger.Info("hidden")
	logger.Warn("shown")

	assert.NotContains(t, out.String(), "hidden")
	assert.Contains(t, out.String(), "WARN \x1b[0m shown\n")
}
