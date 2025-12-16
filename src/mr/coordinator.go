package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	files []string
	nReduce int

	done bool
}

// RPC handler. Argument and reply types are defined in 'rpc.go'.
func (c *Coordinator) MapTaskAssign(args *Empty,
																		reply *MapTaskAssignment) error {
	reply.Filename = c.files[0]
	reply.NReduce = c.nReduce
	return nil
}

// RPC handler. Argument and reply types are defined in 'rpc.go'.
func (c *Coordinator) MapTaskCompleted(args *Empty,
																				data *MapTaskCompleted) error {
	fmt.Printf("result arrived\n")
	return nil
}

// Start a thread that listens for RPCs from 'worker.go'.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 'main/mrcoordinator.go' calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done;
}

// Create a Coordinator.
// 'main/mrcoordinator.go' calls this function.
// 'nReduce' is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		done: false,
	}

	c.files = files
	c.nReduce = nReduce

	c.server()
	return &c
}
