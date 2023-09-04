package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
	Command          string
	TaskName         string // optional for
	File             string
	ReduceTaskNumber int
}

type TaskReply struct {
	TaskName         string
	TaskId           int
	File             string
	ReduceTaskNumber int
	NReduce          int
	TotalFiles       int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
