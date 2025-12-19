package mr

import "os"
import "fmt"
import "log"
import "sort"
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

// Type.
//
// As this type is identical to []KeyValue,
// you can cast back and forth between []KeyValue and ByKey freely:
// they are identical in memory.

type ByKey []KeyValue

// Methods required for custom sorting.
//
// By writing 'func (a ByKey) ...' we're attaching these functions
// to 'ByKey' so that for any 'a' of type 'ByKey', we will be able
// to use our functions as 'a.Func(...)'.
//
// Implementing 'Len', 'Swap', and 'Less' makes our type satisfy the
// 'sort.Interface' interface, so that later we will be able to call
// 'sort.Sort(ByKey(data))' for any 'data' array of type '[]mr.KeyValue'.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

		if task.Type == 2 {
			fmt.Printf("Job is done!")
			os.Exit(0)
		}

		if task.Id == -1 {
			// Coordinator gave no work (waiting)
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("task.Id %v\n", task.Id)
		fmt.Printf("task.Type %v\n", task.Type)
		fmt.Printf("len(task.FileNames) %v\n", len(task.FileNames))
		if len(task.FileNames) > 0 {
			fmt.Printf("task.FileNames[0] %v\n", task.FileNames[0])
		}
		fmt.Printf("task.NReduce %v\n", task.NReduce)
		fmt.Printf("task.NMap %v\n", task.NMap)

		if task.Type == 0 { // map
			doMap(task, mapf)
		} else { // reduce
			doReduce(task, reducef)
		}
	}
}

func doMap(task TaskReply, mapf func(string, string) []KeyValue) {
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
	result := TaskResult{
		Id: task.Id,
		Type: 0, // map
	}
	SendTaskResult(&result)
}

func doReduce(task TaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.Id)
		file, err := os.Open(filename)
		if err != nil {
			// File doesn't exist. This is ok.
			// It means this mapper didn't produce any output for this bucket.
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort keys by name for efficient processing
	sort.Sort(ByKey(intermediate))

	// call reduce on each key
	tmpFile, _ := os.CreateTemp("", "mr-out-tmp-*")
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// find rightmost identical key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// Run reduce on all this key's values.
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// Write output to (tmp) file.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// Rename temp file to expected result filename.
	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(tmpFile.Name(), oname)

	// Emit result
	result := TaskResult{
		Id: task.Id,
		Type: 1, // reduce
	}
	SendTaskResult(&result)
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

		// Assuming coordinator is dead/done
		os.Exit(0)
	}
	return reply
}

// RPC to the coordinator with the result of a task.
// Args and reply types are defined in 'rpc.go'.
func SendTaskResult(result *TaskResult) {
	args  := result
	reply := Empty{}
	ok := call("Coordinator.TaskDone", &args, &reply)
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
