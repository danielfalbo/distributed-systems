package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Use 'ihash(key) % NReduce' to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 'main/mrworker.go' calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	task := AskForTask()
	fmt.Printf("task.Filename %v\n", task.Filename)

	// Read input file, pass it to Map, emit Map output.
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	result := MapTaskCompleted{
		Filename: task.Filename,
		Kva: kva,
	}
	SendMapTaskResult(&result)
}

// RPC to the coordinator asking for a task.
// Args and reply types are defined in 'rpc.go'.
func AskForTask() MapTaskAssignment {
	// Send the RPC request, wait for the reply.
	// The "Coordinator.Procedure" tells the
	// receiving server that we'd like to call
	// the Procedure() method of struct Coordinator.
	args  := Empty{}
	reply := MapTaskAssignment{}
	ok := call("Coordinator.MapTaskAssign", &args, &reply)
	if ok {
		fmt.Printf("reply.Filename %v\n", reply.Filename)
		fmt.Printf("reply.NReduce %v\n", reply.NReduce)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// RPC to the coordinator with the result of a task.
// Args and reply types are defined in 'rpc.go'.
func SendMapTaskResult(result *MapTaskCompleted) {
	args  := result
	reply := Empty{}
	ok := call("Coordinator.MapTaskCompleted", &args, &reply)
	if ok {
		fmt.Printf("ok\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Send an RPC request to the coordinator, wait for the response.
// Usually returns true. Returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
