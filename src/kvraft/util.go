package kvraft

import (
	"log"
	"os"
	"strings"
)

// Debugging
const Debug = false
const Log2File = false

var logFile *os.File

func init() {
	if Debug && Log2File {
		var err error
		logFile, err = os.OpenFile("debug.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("failed to open log file: %v", err)
		}
		log.SetOutput(logFile)
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		// log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
		log.Println(strings.Repeat("-", 100))
		log.Printf(format, a...)
	}
}
