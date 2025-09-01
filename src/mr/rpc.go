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
func (c *Coordinator) WorkerGetMapTask(workerID string, mapTask *MapTask) error {
	toMapTask, ok := <-c.toMapTasks
	if ok {
		mapTask.Filename = toMapTask.Filename
		mapTask.MapID = toMapTask.MapID
		mapTask.NReduce = toMapTask.NReduce
	} else {
		mapTask.AllMapDone = true // all Map tasks already done.
		mapTask.AllReduceDone = c.allReduceDone
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
	// Coordinator only accepts results from worker IN workerRegistry
	workerID := mapTask.WorkerID
	filename := mapTask.Filename
	_, exist := c.workerRegistry[workerID]
	if !exist {
		DPrintf("Illegal map result: get from unknown worker: %s\n", workerID)
		return nil
	}

	c.mu.Lock()
	DPrintf("Successfully get map result from: %s\n", workerID)
	delete(c.remainMapTask, filename)
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
		reduceTask.AllReduceDone = true // all Reduce tasks already done.
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
		DPrintf("Illegal reduce result: get from unknown worker: %s\n", workerID)
		return nil
	}

	newname := fmt.Sprintf("mr-out-%s", reduceTask.ReduceID)
	*reply = newname
	err := os.Rename(reduceTask.TempResFile, newname)
	if err != nil {
		DPrintf("Error when rename temp file: %v\n", err)
	}

	c.mu.Lock()
	DPrintf("Successfully get reduce result from: %s\n", workerID)
	delete(c.remainReduceTask, reduceTask.ReduceID)

	c.mu.Unlock()
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
