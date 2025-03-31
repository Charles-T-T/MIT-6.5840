package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
func (c *Coordinator) WorkerGetMapJob(workerID string, mapTask *MapTask) error {
	toMapTask, ok := <-c.toMapTasks
	if ok {
		mapTask.Filename = toMapTask.Filename
		mapTask.MapID = toMapTask.MapID
		mapTask.NReduce = toMapTask.NReduce
	} else {
		mapTask.Filename = "DONE" // no need to map
		return nil
	}

	// worker registers
	c.mu.Lock()
	c.workerRegistry[workerID] = workerSock(workerID)
	c.remainMapTask[toMapTask.Filename] = workerID
	go c.monitorMapTask(toMapTask.Filename, toMapTask.MapID)
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) WorkerGiveMapRes(mapTask MapTask, reply *string) error {
	// Coordinator only accepts results from worker in workerRegistry
	workerID := mapTask.WorkerID
	filename := mapTask.Filename
	kva := mapTask.Result
	_, exist := c.workerRegistry[workerID]
	if !exist {
		fmt.Println("Illegal map result: get from unknown worker:", workerID)
		return nil
	}

	c.mu.Lock()
	c.intermediate = append(c.intermediate, kva...)
	if DEBUG {
		fmt.Println("Successfully get map result from:", workerID)
		// fmt.Println("len of kva:", len(kva))
		// fmt.Println("len of intermediate:", len(c.intermediate))
	}
	delete(c.remainMapTask, filename)
	// c.WorkerQuit(workerID, reply)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WorkerGetReduceTask(workerID string, reduceTask *ReduceTask) error {
	toReduceTask, ok := <-c.toReduceTasks
	if ok {
		*reduceTask = toReduceTask
		reduceTask.WorkerID = workerID
		reduceTask.TempResFile = fmt.Sprintf("mr-tmp-%s", workerID)
	} else {
		reduceTask.ReduceID = "DONE"
		return nil
	}

	// worker registers
	c.mu.Lock()
	c.workerRegistry[workerID] = workerSock(workerID)
	c.remainReduceTask[toReduceTask.ReduceID] = workerID
	go c.monitorReduceTask(toReduceTask.ReduceID)
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) WorkerGiveReduceRes(reduceTask ReduceTask, reply *string) error {
	// Coordinator only accepts results from worker in workerRegistry
	workerID := reduceTask.WorkerID
	_, exist := c.workerRegistry[workerID]
	if !exist {
		fmt.Println("Illegal reduce result: get from unknown worker:", workerID)
		return nil
	}

	newname := fmt.Sprintf("mr-out-%s", reduceTask.ReduceID)
	*reply = newname
	err := os.Rename(reduceTask.TempResFile, newname)
	if err != nil {
		log.Fatal("Error when rename temp file:", err)
	}

	c.mu.Lock()
	if DEBUG {
		fmt.Println("Successfully get reduce result from:", workerID)
	}
	delete(c.remainReduceTask, reduceTask.ReduceID)
	// delete(c.workerRegistry, workerID)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WorkerQuit(workerID string, reply *string) error {
	_, exist := c.workerRegistry[workerID]
	if exist {
		delete(c.workerRegistry, workerID)
		if DEBUG {
			fmt.Printf("Worker quit: <%s>\n", workerID)
		}

	} else {
		if DEBUG {
			fmt.Printf("Unknown worker wants to quit: <%s>\n", workerID)
		}

	}
	return nil
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
