package mr

import "os"
import "log"
import "net"
import "sync"
import "time"
import "net/rpc"
import "net/http"

type Task struct {
	ID 				int
	FileName 	string
	Status 		int  			// 0: idle, 1: in progress, 2: completed
	StartTime time.Time
}

type Coordinator struct {
	mapTasks 		[]Task
	reduceTasks []Task
	nReduce 		int
	phase 			int 				// 0: map phase, 1: reduce phase, 2: done
	mu 					sync.Mutex
}

// RPC handler. Argument and reply types are defined in 'rpc.go'.
func (c *Coordinator) GetTask(args *Empty, reply *TaskReply) error {

	// Grab mutex, find idle job, set reply to given idle task
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, task := range c.mapTasks {
		if task.Status == 0 { // 0: Idle
			// Mark task as assigned and started now in the coordinator state
			c.mapTasks[i].Status = 1
			c.mapTasks[i].StartTime = time.Now()

			// Set worker state to given task
			reply.Id = task.ID
			reply.FileNames = []string{task.FileName}
			reply.NReduce = c.nReduce
			reply.Type = 0 // map
			return nil
		}
	}

	// No idle map tasks found. We should now check if we're waiting for some
	// to finish or if we should more to the Reduce phase.
	reply.Id = -1

	return nil
}

// RPC handler. Argument and reply types are defined in 'rpc.go'.
func (c *Coordinator) MapTaskCompleted(args *MapTaskCompleted,
																			data *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTasks[args.Id].Status == 2 {
		// Task already marked as done, so return ealy to avoid double counting.
		// This is to safeguard from workers assumed dead who just finished late.
		return nil
	}

	c.mapTasks[args.Id].Status = 2 // completed

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

// Create a Coordinator.
// 'main/mrcoordinator.go' calls this function.
// 'nReduce' is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.phase = 0 // map phase

	for i, file := range files {
		task := Task{
				ID:  			i,
				FileName: file,
				Status: 	0, // idle
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	c.server()
	return &c
}

// 'main/mrcoordinator.go' calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
 	defer c.mu.Unlock()
	return c.phase == 2;
}
