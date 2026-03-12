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

type runErrorAction struct {
	logMessage string
	exitCode   int
	shouldExit bool
	useFatal   bool
}

func classifyRunError(err error) runErrorAction {
	if err == nil {
		return runErrorAction{}
	}

	var cliErr *cliError
	if errors.As(err, &cliErr) {
		action := runErrorAction{
			exitCode:   cliErr.exitCode,
			shouldExit: true,
		}
		if cliErr.msg != "" && !cliErr.printed {
			action.logMessage = cliErr.msg
		}
		return action
	}

	return runErrorAction{useFatal: true}
}

func main() {
	if err := run(os.Args, defaultDeps()); err != nil {
		action := classifyRunError(err)
		if action.useFatal {
			log.Fatal(err)
		}
		if action.logMessage != "" {
			log.Print(action.logMessage)
		}
		if action.shouldExit {
			os.Exit(action.exitCode)
		}
	}
}
