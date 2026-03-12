package main

import (
	"errors"
	"testing"
)

func TestClassifyRunErrorNil(t *testing.T) {
	action := classifyRunError(nil)
	if action.shouldExit || action.useFatal || action.logMessage != "" || action.exitCode != 0 {
		t.Fatalf("unexpected action for nil error: %+v", action)
	}
}

func TestClassifyRunErrorCLIError(t *testing.T) {
	action := classifyRunError(&cliError{
		exitCode: 2,
		msg:      "Usage: wsfs MOUNTPOINT",
	})
	if !action.shouldExit || action.exitCode != 2 {
		t.Fatalf("unexpected exit action: %+v", action)
	}
	if action.useFatal {
		t.Fatalf("did not expect fatal action: %+v", action)
	}
	if action.logMessage != "Usage: wsfs MOUNTPOINT" {
		t.Fatalf("unexpected log message: %q", action.logMessage)
	}
}

func TestClassifyRunErrorSuppressesPrintedCLIMessage(t *testing.T) {
	action := classifyRunError(&cliError{
		exitCode: 0,
		msg:      "already printed",
		printed:  true,
	})
	if !action.shouldExit || action.exitCode != 0 {
		t.Fatalf("unexpected exit action: %+v", action)
	}
	if action.logMessage != "" {
		t.Fatalf("expected no additional log message, got %q", action.logMessage)
	}
}

func TestClassifyRunErrorWrappedCLIError(t *testing.T) {
	err := errors.Join(errors.New("wrapper"), &cliError{
		exitCode: 1,
		msg:      "bad input",
	})

	action := classifyRunError(err)
	if !action.shouldExit || action.exitCode != 1 || action.logMessage != "bad input" {
		t.Fatalf("unexpected wrapped cli action: %+v", action)
	}
	if action.useFatal {
		t.Fatalf("did not expect fatal action: %+v", action)
	}
}

func TestClassifyRunErrorGenericErrorUsesFatal(t *testing.T) {
	action := classifyRunError(errors.New("boom"))
	if !action.useFatal {
		t.Fatalf("expected fatal action, got %+v", action)
	}
	if action.shouldExit || action.logMessage != "" || action.exitCode != 0 {
		t.Fatalf("unexpected non-fatal fields: %+v", action)
	}
}
