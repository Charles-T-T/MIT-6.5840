package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
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
func workerSock(workerID string) string {
	return "/var/tmp/5840-mr-worker-" + workerID
}

func (c *Coordinator) WorkerGetMapJob(workerID string, filename *string) error {
	todoFile, ok := <-c.todoFiles
	if ok {
		*filename = todoFile
	} else {
		*filename = "DONE" // no need to map
		return nil
	}

	// worker registers
	c.mu.Lock()
	c.workerRegistry[workerID] = workerSock(workerID)
	c.remainMapTask[*filename] = workerID
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) WorkerGiveMapRes(mapRes MapRes, reply *string) error {
	// Coordinator only accepts results from worker in workerRegistry
	workerID := mapRes.WorkerID
	filename := mapRes.Filename
	kva := mapRes.Kva
	_, exist := c.workerRegistry[workerID]
	if !exist {
		fmt.Println("Illegal result: get from unknown worker:", workerID)
		return nil
	}

	c.mu.Lock()
	c.intermediate = append(c.intermediate, kva...)
	fmt.Println("Successfully get result from:", workerID)
	fmt.Println("len of kva:", len(kva))
	fmt.Println("len of intermediate:", len(c.intermediate))
	delete(c.remainMapTask, filename)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WorkerQuit(workerID string, reply *string) error {
	_, exist := c.workerRegistry[workerID]
	if exist {
		delete(c.workerRegistry, workerID)
		fmt.Println("Worker quit:", workerID)
	} else {
		fmt.Println("Unknown worker wants to quit", workerID)
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
