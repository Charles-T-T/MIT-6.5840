package mr

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.RWMutex
	nMap          int
	nReduce       int
	toMapTasks    chan MapTask
	toReduceTasks chan ReduceTask
	// NOTE: consider use map[string]struct{}
	remainMapTask    map[string]string // filename -> workerID
	remainReduceTask map[string]string // reduceID -> workerID
	workerRegistry   map[string]string // workerID -> wordAddr
	intermediate     []KeyValue
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

// Monitor a Map task, reassign it if time out.
func (c *Coordinator) monitorMapTask(file string, mapID string) {
	time.Sleep(time.Second * 10) // wait for 10s
	workerID, exist := c.remainMapTask[file]
	if exist {
		c.mu.Lock()
		delete(c.workerRegistry, workerID)
		if DEBUG {
			fmt.Printf("Map job by %s time out!\n", workerID)
		}
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
		if DEBUG {
			fmt.Printf("Reduce job by %s time out!\n", workerID)
		}
		c.mu.Unlock()
		c.toReduceTasks <- ReduceTask{ReduceID: reduceID}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if !c.allMapDone || !c.allReduceDone || len(c.workerRegistry) > 0 {
		return false
	}
	deleteJSONs()
	// mergeAndSortFiles("mr-out-final")
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
		intermediate:     []KeyValue{},
		allMapDone:       false,
		allReduceDone:    false,
	}

	// Manage map tasks
	go func() {
		for i, file := range files {
			mapTask := MapTask{
				Filename: file,
				MapID:    strconv.Itoa(i),
				NMap:     c.nMap,
				NReduce:  c.nReduce,
			}
			c.toMapTasks <- mapTask
			if DEBUG {
				fmt.Println("Get todo-file:", file)
			}
			c.remainMapTask[file] = "init"
		}
		for len(c.remainMapTask) > 0 {
			time.Sleep(time.Second)
		}
		close(c.toMapTasks)
		c.allMapDone = true
		if DEBUG {
			fmt.Println("All map tasks done.")
		}
	}()

	// Manage reduce tasks
	go func() {
		// output files for reduce results
		for i := 0; i < nReduce; i++ {
			// NOTE: maybe use the trick of renaming file?
			// os.Create(fmt.Sprintf("mr-out-%d", i))
			c.toReduceTasks <- ReduceTask{ReduceID: strconv.Itoa(i)}
			c.remainReduceTask[strconv.Itoa(i)] = "init"
		}
		// Wait map tasks to be done
		for !c.allMapDone {
			time.Sleep(time.Second)
		}
		// Wait reduce tasks to be done
		for len(c.remainReduceTask) > 0 {
			time.Sleep(time.Second)
		}
		close(c.toReduceTasks)
		c.allReduceDone = true
		if DEBUG {
			fmt.Println("All reduce tasks done.")
		}
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

func mergeAndSortFiles(outputFileName string) error {
	// 获取当前目录
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %w", err)
	}

	// 搜索当前目录下的所有"mr-out-*"文件
	files, err := filepath.Glob(filepath.Join(dir, "mr-out-*"))
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// 用来存储所有文件的内容
	var allLines []string

	// 遍历每个文件，读取其内容
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", file, err)
		}
		defer f.Close()

		// 使用Scanner逐行读取文件内容
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			allLines = append(allLines, scanner.Text()) // 将每行内容加入到 allLines 中
		}

		// 检查是否有读取错误
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading file %s: %w", file, err)
		}
	}

	// 对读取到的所有行进行字母序排序
	sort.Strings(allLines)

	// 创建输出文件
	outFile, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// 写入排序后的内容到输出文件
	for _, line := range allLines {
		_, err := outFile.WriteString(line + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to output file: %w", err)
		}
	}

	return nil
}
