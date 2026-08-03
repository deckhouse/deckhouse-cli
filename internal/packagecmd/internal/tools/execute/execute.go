package execute

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

type Command string

type Arg string

type options struct {
	path string
	args []string
	env  []string
}

type Env string

func NewEnv(key, value string) Env {
	return Env(fmt.Sprintf("%s=%s", key, value))
}

type Option func(*options)

func WithArgs(args ...Arg) Option {
	return func(o *options) {
		for _, arg := range args {
			o.args = append(o.args, string(arg))
		}
	}
}

func WithEnv(env ...Env) Option {
	return func(o *options) {
		for _, e := range env {
			o.env = append(o.env, string(e))
		}
	}
}

func WithPath(path string) Option {
	return func(o *options) {
		o.path = path
	}
}

func (c Command) Execute(ctx context.Context, opts ...Option) error {
	if len(c) == 0 {
		return fmt.Errorf("no command specified")
	}

	execOpts := new(options)
	for _, opt := range opts {
		opt(execOpts)
	}

	cmd := exec.CommandContext(ctx, string(c), execOpts.args...)

	cmd.Dir = execOpts.path
	cmd.Env = append(os.Environ(), execOpts.env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("execute command: %w", err)
	}

	return nil
}
