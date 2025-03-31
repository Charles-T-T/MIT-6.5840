package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

var DEBUG bool = true

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MapTask struct {
	WorkerID string
	MapID    string
	NMap     int
	NReduce  int
	Filename string
	Result   []KeyValue
}

type ReduceTask struct {
	WorkerID    string
	ReduceID    string
	TempResFile string // created by worker
}

func workerSock(workerID string) string {
	return "/var/tmp/5840-mr-worker-" + workerID
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func saveMapRes(kva []KeyValue, mapID string, nReduce int) {
	reduceChunks := make(map[string][]KeyValue) // reduceID -> kvs
	for _, kv := range kva {
		reduceID := strconv.Itoa(ihash(kv.Key) % nReduce)
		reduceChunks[reduceID] = append(reduceChunks[reduceID], kv)
	}

	for reduceID, kvs := range reduceChunks {
		oname := fmt.Sprintf("mr-%s-%s.json", mapID, reduceID)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatal("Error when create file:", err)
		}
		defer ofile.Close()

		enc := json.NewEncoder(ofile)
		err = enc.Encode(&kvs)
		if err != nil {
			log.Fatal("Error when encoding kv:", err)
		}
	}
	if DEBUG {
		fmt.Println("Finish save map result.")
	}
}

func doReduce(toReduceFiles []string, reducef func(string, []string) string, oname string) {
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	intermediate := []KeyValue{}

	for _, toReduceFile := range toReduceFiles {
		file, _ := os.Open(toReduceFile)
		dec := json.NewDecoder(file)
		kva := []KeyValue{}
		if err := dec.Decode(&kva); err != nil {
			log.Fatal("Error when json decode:", err)
		}
		intermediate = append(intermediate, kva...)
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := strconv.Itoa(os.Getpid())

	// Periodically ask for a task
	mapDone := false    // flag whether all Map tasks have been finished
	reduceDone := false // flag whether all Reduce tasks have been finished
	// Do the map task
	for !mapDone{
		mapTask := MapTask{WorkerID: workerID}
		if DEBUG {
			fmt.Printf("<%s> ask for a map task...\n", workerID)
		}
		call("Coordinator.WorkerGetMapJob", workerID, &mapTask)
		if DEBUG {
			fmt.Printf("<%s> get task: %v\n", workerID, mapTask)
		}

		if mapTask.Filename != "DONE" {
			file, err := os.Open(mapTask.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", mapTask.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", mapTask.Filename)
			}
			file.Close()
			kva := mapf(mapTask.Filename, string(content))
			saveMapRes(kva, mapTask.MapID, mapTask.NReduce)

			mapTask.Result = kva
			var reply string
			call("Coordinator.WorkerGiveMapRes", mapTask, &reply)

			// if DEBUG {
			// 	time.Sleep(time.Second) // NOTE: this is to let other workers have chance
			// }
		} else {
			mapDone = true
			if DEBUG {
				fmt.Println("All map tasks done.")
			}
		}

		time.Sleep(time.Second)
	}

	// Do the Reduce task
	for !reduceDone{
		reduceTask := ReduceTask{WorkerID: workerID}
		if DEBUG {
			fmt.Printf("<%s> ask for a reduce task...\n", workerID)
		}
		call("Coordinator.WorkerGetReduceTask", workerID, &reduceTask)
		if DEBUG {
			fmt.Printf("<%s> get reduceID: %s\n", workerID, reduceTask.ReduceID)
		}
		if reduceTask.ReduceID != "DONE" {
			// Get Map result files to be Reduced
			pattern := fmt.Sprintf(`^mr-.*-%s.json$`, regexp.QuoteMeta(reduceTask.ReduceID))
			re := regexp.MustCompile(pattern)

			files, err := os.ReadDir(".")
			if err != nil {
				fmt.Println("Error reading directory:", err)
				return
			}

			var toReduceFiles []string
			for _, file := range files {
				if !file.IsDir() && re.MatchString(file.Name()) {
					toReduceFiles = append(toReduceFiles, file.Name())
				}
			}

			// Do the reduce job
			doReduce(toReduceFiles, reducef, reduceTask.TempResFile)
			if DEBUG {
				fmt.Printf("<%s> finish reduce job, res to %s.\n", workerID, reduceTask.TempResFile)
			}
			var reply string
			call("Coordinator.WorkerGiveReduceRes", reduceTask, &reply)
			if DEBUG {
				fmt.Printf("<%s> reduce res save to %s.\n", workerID, reply)
				// time.Sleep(time.Second) // NOTE: let another worker have chance
			}
		} else {
			reduceDone = true
			if DEBUG {
				fmt.Println("All reduce done.")
			}
		}
		
		time.Sleep(time.Second)
	}

	var reply string
	call("Coordinator.WorkerQuit", workerID, &reply)
	if DEBUG {
		fmt.Printf("<%s> job done.\n", workerID)
	}
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
