package main

import "log"

var debugLogs bool

func debugf(format string, args ...any) {
	if debugLogs {
		log.Printf(format, args...)
	}
}
