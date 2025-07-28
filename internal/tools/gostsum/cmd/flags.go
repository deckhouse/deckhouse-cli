package cmd

import (
	"github.com/spf13/pflag"
)

func addFlags(flags *pflag.FlagSet) {
	flags.IntP(
		"hash-length",
		"l",
		256,
		"Bit-length variant of the algorithm to use. Only 256 (default) and 512 bit lengths are supported.",
	)
}
