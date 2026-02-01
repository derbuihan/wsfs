package logging

import "log"

var DebugLogs bool

func Debugf(format string, args ...any) {
	if DebugLogs {
		log.Printf(format, args...)
	}
}
