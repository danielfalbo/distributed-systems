package mr

import "os"
import "fmt"
import "log"
import "time"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"

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

	for {
		task := AskForTask()

		if task.Id == -1 {
			// Coordinator gave no work (maybe waiting for others to finish)
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("task.Id %v\n", task.Id)
		fmt.Printf("task.Type %v\n", task.Type)
		fmt.Printf("len(task.FileNames) %v\n", len(task.FileNames))
		fmt.Printf("task.FileNames[0] %v\n", task.FileNames[0])
		fmt.Printf("task.NReduce %v\n", task.NReduce)
		fmt.Printf("task.NMap %v\n", task.NMap)

		// Naming convention for intermediate Map output files is mr-X-Y,
		// where X is the Map task number, and Y is the reduce task number.

		// We use temp files and rename then once the job is over,
		// so coordinator can check for finished work by looking for
		// files with expected filename format.

		// Create NReduce temp files and encoders attached to each file.
		encoders := make([]*json.Encoder, task.NReduce)
		files		 := make([]*os.File, 			task.NReduce)
		for i := 0; i < task.NReduce; i++ {
			file, err := os.CreateTemp("", "mr-tmp-*")
			if err != nil {
				log.Fatal("could not create temp file")
			}
			files[i] = file
			encoders[i] = json.NewEncoder(file)
		}

		// Read input file
		filename := task.FileNames[0]
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		// Process file content with given Map implementation from app plugin
		kva := mapf(filename, string(content))

		// Write result to files
		for _, kv := range kva {
			// Hash key to figure out where among the Nreduce outs to put given kv
			bucket := ihash(kv.Key) % task.NReduce
			encoders[bucket].Encode(&kv)
		}

		// Close files and rename them with naming convention
		for i, file := range files {
			tmpName := file.Name()
			file.Close()

			finalName := fmt.Sprintf("mr-%d-%d", task.Id, i)
			os.Rename(tmpName, finalName)
		}

		// Emit result
		result := MapTaskCompleted{
			Id: task.Id,
		}
		SendMapTaskResult(&result)
	}
}

// RPC to the coordinator asking for a task.
// Args and reply types are defined in 'rpc.go'.
func AskForTask() TaskReply {
	// Send the RPC request, wait for the reply.
	// The "Coordinator.Procedure" tells the
	// receiving server that we'd like to call
	// the Procedure() method of struct Coordinator.
	args  := Empty{}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Id %v\n", reply.Id)
		fmt.Printf("reply.Type %v\n", reply.Type)
		fmt.Printf("len(reply.FileNames) %v\n", len(reply.FileNames))
		fmt.Printf("reply.NMap %v\n", reply.NMap)
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
