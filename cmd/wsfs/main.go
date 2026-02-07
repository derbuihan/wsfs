package main

import (
	"errors"
	"log"
	"os"
)

// Build information (set via ldflags)
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := run(os.Args, defaultDeps()); err != nil {
		var cliErr *cliError
		if errors.As(err, &cliErr) {
			if cliErr.msg != "" && !cliErr.printed {
				log.Print(cliErr.msg)
			}
			os.Exit(cliErr.exitCode)
		}
		log.Fatal(err)
	}
}
