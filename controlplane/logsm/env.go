package logsm

import "time"

// RealNow is the wall clock in nanoseconds. NewReplica uses it when Config.Now is nil.
func RealNow() int64 { return time.Now().UnixNano() }
