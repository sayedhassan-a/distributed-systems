package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	UnProcessed = iota
	Pending
	Finished
)

type Coordinator struct {
	// Your definitions here.
	InputFiles          []string
	CurrentFile         int
	MapFinished         bool
	ReduceFinished      bool
	nReduce             int
	CurrentReduceTask   int
	MapTasksFinished    int
	ReduceTasksFinished int
	MapTasksStarted     int
	ReduceTasksStarted  int
	MapTasksStart       concurrentMap[time.Time]
	ReduceTasksStart    concurrentMap[time.Time]
	MapTasksState       concurrentMap[int]
	ReduceTasksState    concurrentMap[int]
}

type concurrentMap[T int | time.Time] struct {
	mutex sync.Mutex
	data  map[int]T
}

func (c *concurrentMap[T]) Get(key int) T {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	value, _ := c.data[key]
	return value
}

func (c *concurrentMap[T]) Set(key int, value T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.data[key] = value
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//fmt.Println("get task")
	//fmt.Println(c.MapTasksStarted, " ", c.ReduceTasksStarted, " ", c.MapTasksFinished, " ", c.ReduceTasksFinished)
	//fmt.Println(c.MapFinished, " ", c.ReduceFinished)
	//fmt.Println(c.CurrentFile, " ", c.CurrentReduceTask)
	for !c.MapFinished && c.MapTasksStarted == len(c.InputFiles) {
		reply.TaskType = WaitTaskType
		return nil
	}
	for c.MapFinished && !c.ReduceFinished && c.ReduceTasksStarted == c.nReduce {
		reply.TaskType = WaitTaskType
		return nil
	}
	if !c.MapFinished {
		for c.MapTasksState.Get(c.CurrentFile) != UnProcessed {
			c.CurrentFile++
			c.CurrentFile %= len(c.InputFiles)
		}
		reply.TaskType = MapTaskType
		reply.Filename = c.InputFiles[c.CurrentFile]
		reply.NReduce = c.nReduce
		reply.TaskNumber = c.CurrentFile
		c.MapTasksStart.Set(c.CurrentFile, time.Now())
		c.MapTasksState.Set(c.CurrentFile, Pending)
		c.MapTasksStarted++
		c.CurrentFile++
		c.CurrentFile %= len(c.InputFiles)
		return nil

	} else if !c.ReduceFinished {
		for c.ReduceTasksState.Get(c.CurrentReduceTask) != UnProcessed {
			c.CurrentReduceTask++
			c.CurrentReduceTask %= c.nReduce
		}
		reply.TaskType = ReduceTaskType
		reply.TaskNumber = c.CurrentReduceTask
		reply.NReduce = c.nReduce
		reply.NumberOfMapTasks = len(c.InputFiles)
		c.ReduceTasksState.Set(c.CurrentReduceTask, Pending)
		c.ReduceTasksStart.Set(c.CurrentReduceTask, time.Now())
		c.ReduceTasksStarted++
		c.CurrentReduceTask++
		c.CurrentReduceTask %= c.nReduce
		return nil
	}
	return &CallError{time.Now(), "call error"}
}

func (c *Coordinator) FinishedTask(args *FinishedTaskArgs, reply *FinishedTaskArgs) error {
	//println("finished task")
	//fmt.Println(args.TaskNumber, " ", args.TaskType)
	if args.TaskType == MapTaskType {
		if c.MapTasksState.Get(args.TaskNumber) != Pending {
			return nil
		}
		c.MapTasksState.Set(args.TaskNumber, Finished)
		c.MapTasksFinished++
		if c.MapTasksFinished == len(c.InputFiles) {
			c.MapFinished = true
		}
		return nil
	} else if args.TaskType == ReduceTaskType {
		if c.ReduceTasksState.Get(args.TaskNumber) != Pending {
			return nil
		}
		c.ReduceTasksState.Set(args.TaskNumber, Finished)
		c.ReduceTasksFinished++
		if c.ReduceTasksFinished == c.nReduce {
			c.ReduceFinished = true
		}
		return nil
	}
	return &CallError{time.Now(), "call error"}
}
func (c *Coordinator) CheckFailedTasks() {
	for !c.MapFinished {
		for TaskNumber := range len(c.InputFiles) {
			state := c.MapTasksState.Get(TaskNumber)
			if state == Pending {
				if time.Since(time.Time(c.MapTasksStart.Get(TaskNumber))) > time.Duration(10000)*time.Millisecond {
					c.MapTasksState.Set(TaskNumber, UnProcessed)
					c.MapTasksStarted--
				}
			}
		}
	}
	for !c.ReduceFinished {
		for TaskNumber := range c.nReduce {
			state := c.ReduceTasksState.Get(TaskNumber)
			if state == Pending {
				if time.Since(time.Time(c.ReduceTasksStart.Get(TaskNumber))) > time.Duration(10000)*time.Millisecond {
					c.ReduceTasksState.Set(TaskNumber, UnProcessed)
					c.ReduceTasksStarted--
				}
			}
		}
	}
}

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	return c.ReduceFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.InputFiles = files
	c.MapFinished = false
	c.ReduceFinished = false
	c.CurrentFile = 0
	c.nReduce = nReduce
	c.CurrentReduceTask = 0
	c.MapTasksState.data = make(map[int]int)
	c.MapTasksStart.data = make(map[int]time.Time)
	c.ReduceTasksState.data = make(map[int]int)
	c.ReduceTasksStart.data = make(map[int]time.Time)
	go c.CheckFailedTasks()
	c.server()
	return &c
}
