package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MapRes struct {
	WorkerID string
	Filename string
	Kva      []KeyValue
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := strconv.Itoa(os.Getpid())
	// sockname := workerSock(workerID)

	// Periodically ask for a task
	mapDone := false // flag whether the map tasks have all finished
	for {
		// Ask for a map task
		if mapDone {
			break
		}
		var filename string
		fmt.Println("Ask for a map task...")
		call("Coordinator.WorkerGetMapJob", workerID, &filename)
		fmt.Println("Get file:", filename)

		if filename != "DONE" {
			// time.Sleep(time.Second) // just for stimulate
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			fmt.Println("len of kva:", len(kva))
			// IDandFilename := []string{workerID, filename}

			mapRes := MapRes{
				WorkerID: workerID,
				Filename: filename,
				Kva:      kva,
			}
			var reply string

			call("Coordinator.WorkerGiveMapRes", mapRes, &reply)
			time.Sleep(time.Second) // NOTE: let other workers have chance
		} else {
			mapDone = true
			fmt.Println("All map tasks done.")
		}

		time.Sleep(time.Second)
	}
	var reply string
	call("Coordinator.WorkerQuit", workerID, &reply)
	fmt.Println("Job done.")
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}

	s := "yes"
	done := true
	finish := call("Coordinator.FinishJob", s, &done)
	if finish {
		fmt.Println("Job finished.")
	} else {
		fmt.Println("Job not finished.")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
