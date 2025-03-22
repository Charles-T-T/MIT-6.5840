package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// NOTE just for test
	mu             sync.RWMutex
	nReduce        int
	workerRegistry map[string]string // workerID -> wordAddr
	todoFiles      chan string
	remainMapTask  map[string]string // filename -> workerID
	intermediate   []KeyValue
	allMapDone     bool
	allReduceDone  bool
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Your code here -- RPC handlers for the worker to call.
// TODO: copy rpc function here.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Monitor a map task, reassign it if time out.
func (c *Coordinator) monitorMapTask(file string) {
	time.Sleep(time.Second * 10) // wait for 10s
	workerID, exist := c.remainMapTask[file]
	if exist {
		c.mu.Lock()
		delete(c.workerRegistry, workerID)
		c.mu.Unlock()
		c.todoFiles <- file
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	if !c.allMapDone || !c.allReduceDone {
		return false
	}
	if len(c.workerRegistry) > 0 {
		return false
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		todoFiles:      make(chan string, len(files)),
		remainMapTask:  make(map[string]string),
		workerRegistry: make(map[string]string),
		intermediate:   []KeyValue{},
		allMapDone:     false,
		allReduceDone:  false,
	}

	// Manage map tasks
	go func() {
		for _, file := range files {
			c.todoFiles <- file
			fmt.Println("Get todo-file:", file)
			c.remainMapTask[file] = "init"
		}
		for len(c.remainMapTask) > 0 {
			time.Sleep(time.Second)
		}
		close(c.todoFiles)
		c.allMapDone = true
		fmt.Println("All map tasks done.")
	}()

	// Manage reduce tasks
	go func() {
		for !c.allMapDone {
			time.Sleep(time.Second)
		}
		sort.Sort(ByKey(c.intermediate))
		oname := "mr-out-test0"
		fmt.Println("len of intermediate:", len(c.intermediate))
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(c.intermediate) {
			j := i + 1
			for j < len(c.intermediate) && c.intermediate[j].Key == c.intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, c.intermediate[k].Value)
			}
			output := len(values)
			fmt.Fprintf(ofile, "%v %v\n", c.intermediate[i].Key, output)
			i = j
		}
		ofile.Close()
		c.allReduceDone = true
		fmt.Println("All reduce tasks done")
	}()

	c.server()
	return &c
}
