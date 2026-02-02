package logging

import (
	"log"
	"strings"
)

// LogLevel represents the logging verbosity level.
type LogLevel int

const (
	// LevelDebug logs all messages including debug information.
	LevelDebug LogLevel = iota
	// LevelInfo logs informational messages and above.
	LevelInfo
	// LevelWarn logs warning messages and above.
	LevelWarn
	// LevelError logs only error messages.
	LevelError
)

// Level is the current log level. Default is LevelInfo.
var Level LogLevel = LevelInfo

// DebugLogs is kept for backward compatibility.
// Deprecated: Use Level = LevelDebug instead.
var DebugLogs bool

// SetLevel sets the current log level.
func SetLevel(level LogLevel) {
	Level = level
	// Keep DebugLogs in sync for backward compatibility
	DebugLogs = (level <= LevelDebug)
}

// ParseLevel parses a string into a LogLevel.
// Supported values: "debug", "info", "warn", "error" (case-insensitive).
// Returns LevelInfo for unrecognized values.
func ParseLevel(s string) LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// String returns the string representation of a LogLevel.
func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

// Debugf logs a debug message if the current level is DEBUG.
func Debugf(format string, args ...any) {
	if Level <= LevelDebug || DebugLogs {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// Infof logs an informational message if the current level is INFO or below.
func Infof(format string, args ...any) {
	if Level <= LevelInfo {
		log.Printf("[INFO] "+format, args...)
	}
}

// Warnf logs a warning message if the current level is WARN or below.
func Warnf(format string, args ...any) {
	if Level <= LevelWarn {
		log.Printf("[WARN] "+format, args...)
	}
}

// Errorf logs an error message. Always logged regardless of level.
func Errorf(format string, args ...any) {
	log.Printf("[ERROR] "+format, args...)
}
