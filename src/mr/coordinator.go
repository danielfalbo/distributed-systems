package mr

import "os"
import "log"
import "net"
import "sync"
import "time"
import "net/rpc"
import "net/http"

type Task struct {
	Id 				int
	FileName 	string
	Status 		int  			// 0: idle, 1: in progress, 2: completed
	StartTime time.Time
}

type Coordinator struct {
	mapTasks 		[]Task
	reduceTasks []Task
	nReduce 		int
	nMap      	int
	phase 			int 				// 0: map phase, 1: reduce phase, 2: done
	mu 					sync.Mutex
}

// RPC handler. Argument and reply types are defined in 'rpc.go'.
func (c *Coordinator) GetTask(args *Empty, reply *TaskReply) error {

	// Grab mutex, find idle job, set reply to given idle task
	c.mu.Lock()
	defer c.mu.Unlock()

	// Default to Wait task (-1).
	reply.Id = -1

	if c.phase == 0 { // map
		for i, task := range c.mapTasks {
			// [0: idle] or [1: in progress] but worker is assumed dead (10s timeout)
			if task.Status == 0 ||
				(task.Status == 1 && time.Since(task.StartTime) > time.Second * 10) {
				// Mark task as assigned and started now in the coordinator state
				c.mapTasks[i].Status = 1
				c.mapTasks[i].StartTime = time.Now()

				// Set worker state to given task
				reply.Id = task.Id
				reply.FileNames = []string{task.FileName}
				reply.NReduce = c.nReduce
				reply.Type = 0 // map
				return nil
			}
		}
	} else if c.phase == 1 { // reduce
		for i, task := range c.reduceTasks {
			if task.Status == 0 { // 0: Idle
				// Mark task as assigned and started now in the coordinator state
				c.reduceTasks[i].Status = 1
				c.reduceTasks[i].StartTime = time.Now()

				// Set worker state to given task
				// A Reduce worker doesn't need reply.FileNames since it can
				// infer them given its bucket index and NMap.
				reply.Id = i // reduce bucket
				reply.NMap = c.nMap
				reply.Type = 1 // reduce

				return nil
			}
		}
	} else { // done
		reply.Type = 2 // invite to exit
		return nil
	}

	return nil
}

// RPC handler. Argument and reply types are defined in 'rpc.go'.
func (c *Coordinator) TaskDone(args *TaskResult, data *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == 0 { // map
		if c.mapTasks[args.Id].Status == 2 {
			// Task already marked as done,
			// so return ealy to avoid double counting.
			// This is to safeguard from workers assumed dead
			// who just finished late.
			return nil
		}

		c.mapTasks[args.Id].Status = 2 // completed

		// If this was the last map task, let's switch the
		// coordinator to reduce phase.
		allMapsDone := true
		for _, t := range c.mapTasks {
			if t.Status != 2 {
				allMapsDone = false
				break
			}
		}
		if allMapsDone && c.phase == 0 {
			c.phase = 1 // Switch to Reduce phase.
		}
	} else { // reduce
		if c.reduceTasks[args.Id].Status == 2 {
			// Task already marked as done,
			// so return ealy to avoid double counting.
			// This is to safeguard from workers assumed dead
			// who just finished late.
			return nil
		}

		c.reduceTasks[args.Id].Status = 2 // completed

		// If this was the last reduce task, let's switch the
		// coordinator phase to done.
		allReduceDone := true
		for _, t := range c.reduceTasks {
			if t.Status != 2 {
				allReduceDone = false
				break
			}
		}
		if allReduceDone && c.phase == 1 {
			c.phase = 2 // Switch to done.
		}
	}

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
	c.phase = 0 // map phase

	// init map tasks: 1 per input file
	c.nMap = len(files)
	for i, file := range files {
		task := Task{
				Id:  			i,
				FileName: file,
				Status: 	0, // idle
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	// init nReduce reduce tasks
	c.nReduce = nReduce
	for i := 0; i < c.nReduce; i++ {
		task := Task{
				Id:  			i,
				Status: 	0, // idle
		}
		c.reduceTasks = append(c.reduceTasks, task)
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
