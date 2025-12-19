package mr

// RPC definitions.
// Remember to capitalize all names.

import "os"
import "strconv"

type Empty struct{}

// Response to worker asking coordinator for a task.
type TaskReply struct {
	Id 						int

	// 0: Map, 1: Reduce
	Type 					int

	// Map gets 1 file; Reduce gets 'NReduce' files.
	FileNames  		[]string

	// When a Worker is acting as a Mapper,
	// it is responsible for processing 1 input file.
	// However, it must split its output into NReduce different buckets
	// so that each Reducer knows which part of the data to grab.
	// To avoid counts for the same keys to end up in different buckets,
	// Mappers uses ihash(key) % NReduce to decide which bucket a key belongs to.
	//
	// The ith Mapper will produce files mr-i-0, mr-i-1, ..., mr-i-(NReduce-1).
	NReduce 	int

	// When a Worker is acting as a Reducer, it is responsible for processing
	// its bucket values from every single Map task that ever ran. It needs
	// to know how many mappers processed the data.
	//
	// The Reducer for the ith bucket will process files
	// mr-0-i, mr-1-i, ..., mr-(NMap-1)-i.
	NMap int
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
