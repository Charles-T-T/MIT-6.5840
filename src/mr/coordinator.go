package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	mu            sync.RWMutex
	nMap          int
	nReduce       int
	toMapTasks    chan MapTask
	toReduceTasks chan ReduceTask
	// NOTE: consider using map[string]struct{}
	remainMapTask    map[string]string // filename -> workerID
	remainReduceTask map[string]string // reduceID -> workerID
	workerRegistry   map[string]string // workerID -> workerAddr
	allMapDone       bool
	allReduceDone    bool
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Your code here -- RPC handlers for the worker to call.

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

// Monitor a Map task, reassign it if time out.
func (c *Coordinator) monitorMapTask(file string, mapID string) {
	time.Sleep(time.Second * 10) // wait for 10s
	workerID, exist := c.remainMapTask[file]
	if exist {
		c.mu.Lock()
		delete(c.workerRegistry, workerID)
		DPrintf("Map job by %s time out!\n", workerID)
		c.mu.Unlock()
		c.toMapTasks <- MapTask{Filename: file, MapID: mapID, NMap: c.nMap, NReduce: c.nReduce}
	}
}

// Monitor a Reduce task, reassign it if time out.
func (c *Coordinator) monitorReduceTask(reduceID string) {
	time.Sleep(time.Second * 10) // wait for 10s
	workerID, exist := c.remainReduceTask[reduceID]
	if exist {
		c.mu.Lock()
		delete(c.workerRegistry, workerID)
		DPrintf("Reduce job by %s time out!\n", workerID)
		c.mu.Unlock()
		c.toReduceTasks <- ReduceTask{ReduceID: reduceID}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if !c.allMapDone || !c.allReduceDone {
		return false
	}
	deleteJSONs()
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:             len(files),
		nReduce:          nReduce,
		toMapTasks:       make(chan MapTask, len(files)),
		toReduceTasks:    make(chan ReduceTask, nReduce),
		remainMapTask:    make(map[string]string),
		remainReduceTask: make(map[string]string),
		workerRegistry:   make(map[string]string),
		allMapDone:       false,
		allReduceDone:    false,
	}

	// Manage Map tasks
	go func() {
		// Init todo Map tasks
		for i, file := range files {
			mapTask := MapTask{
				Filename: file,
				MapID:    strconv.Itoa(i),
				NMap:     c.nMap,
				NReduce:  c.nReduce,
			}
			c.toMapTasks <- mapTask
			DPrintf("Get todo-file: %s\n", file)
			c.remainMapTask[file] = "init"
		}
		// Wait all Map tasks to be done
		for len(c.remainMapTask) > 0 {
			time.Sleep(time.Second)
		}
		close(c.toMapTasks)
		c.allMapDone = true
		DPrintf("All map tasks done.\n")
	}()

	// Manage Reduce tasks
	go func() {
		// output files for reduce results
		for i := 0; i < nReduce; i++ {
			c.toReduceTasks <- ReduceTask{ReduceID: strconv.Itoa(i)}
			c.remainReduceTask[strconv.Itoa(i)] = "init"
		}
		// Wait all Map tasks to be done
		for !c.allMapDone {
			time.Sleep(time.Second)
		}
		// Wait all Reduce tasks to be done
		for len(c.remainReduceTask) > 0 {
			time.Sleep(time.Second)
		}
		close(c.toReduceTasks)
		c.allReduceDone = true
		DPrintf("All reduce tasks done.\n")
	}()
	c.server()
	return &c
}

func deleteJSONs() {
	dir, err := os.Getwd()
	if err != nil {
		log.Println("Error getting current directory:", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		log.Println("Error finding JSON files:", err)
	}

	for _, file := range matches {
		if err := os.Remove(file); err != nil {
			log.Printf("Error deleting file %s: %v\n", file, err)
		}
	}
}
