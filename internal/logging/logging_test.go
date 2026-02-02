package logging

import (
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"Debug", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"WARN", LevelWarn},
		{"warning", LevelWarn},
		{"WARNING", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"unknown", LevelInfo},
		{"", LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseLevel(tt.input)
			if result != tt.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLogLevelString(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarn, "warn"},
		{LevelError, "error"},
		{LogLevel(999), "info"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.level.String()
			if result != tt.expected {
				t.Errorf("%v.String() = %q, want %q", tt.level, result, tt.expected)
			}
		})
	}
}

func TestSetLevel(t *testing.T) {
	// Reset state after test
	originalLevel := Level
	originalDebugLogs := DebugLogs
	defer func() {
		Level = originalLevel
		DebugLogs = originalDebugLogs
	}()

	// Test setting debug level
	SetLevel(LevelDebug)
	if Level != LevelDebug {
		t.Errorf("SetLevel(LevelDebug): Level = %v, want %v", Level, LevelDebug)
	}
	if !DebugLogs {
		t.Error("SetLevel(LevelDebug): DebugLogs should be true")
	}

	// Test setting info level
	SetLevel(LevelInfo)
	if Level != LevelInfo {
		t.Errorf("SetLevel(LevelInfo): Level = %v, want %v", Level, LevelInfo)
	}
	if DebugLogs {
		t.Error("SetLevel(LevelInfo): DebugLogs should be false")
	}

	// Test setting warn level
	SetLevel(LevelWarn)
	if Level != LevelWarn {
		t.Errorf("SetLevel(LevelWarn): Level = %v, want %v", Level, LevelWarn)
	}

	// Test setting error level
	SetLevel(LevelError)
	if Level != LevelError {
		t.Errorf("SetLevel(LevelError): Level = %v, want %v", Level, LevelError)
	}
}

func TestLogLevelOrdering(t *testing.T) {
	// Verify log level ordering: DEBUG < INFO < WARN < ERROR
	if !(LevelDebug < LevelInfo) {
		t.Error("LevelDebug should be less than LevelInfo")
	}
	if !(LevelInfo < LevelWarn) {
		t.Error("LevelInfo should be less than LevelWarn")
	}
	if !(LevelWarn < LevelError) {
		t.Error("LevelWarn should be less than LevelError")
	}
}
