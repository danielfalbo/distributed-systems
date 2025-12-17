package mr

// RPC definitions.
// Remember to capitalize all names.

import "os"
import "strconv"

type Empty struct{}

// Response to worker asking coordinator for a task.
type MapTaskAssignment struct {
	Id int
	Filename string
	NReduce int
}

// Worker notifying coordinator of completed task.
type MapTaskCompleted struct {
	Id int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
