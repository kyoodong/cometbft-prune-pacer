package main

import "testing"

// Arguments left after the flags used to be ignored: "--max-step 5 run"
// dropped "run", and a mistyped subcommand stopped flag parsing silently.
func TestLeftoverArgsRejected(t *testing.T) {
	for _, args := range [][]string{
		{"--max-step", "5", "--min-retain", "5", "run"},
		{"rnu", "--max-step", "5", "--min-retain", "5"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("%q accepted", args)
		}
	}
}
