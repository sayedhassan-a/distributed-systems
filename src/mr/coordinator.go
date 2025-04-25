package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	InputFiles        []string
	CurrentFile       int
	MapFinished       bool
	ReduceFinished    bool
	nReduce           int
	CurrentReduceTask int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	if !c.MapFinished {
		reply.TaskType = MapTaskType
		reply.Filename = c.InputFiles[c.CurrentFile]
		reply.NReduce = c.nReduce
		reply.TaskNumber = c.CurrentFile
		c.CurrentFile++
		if len(c.InputFiles) == c.CurrentFile {
			c.MapFinished = true
		}
		fmt.Println(reply)
		return nil

	} else if !c.ReduceFinished {
		reply.TaskType = ReduceTaskType
		reply.TaskNumber = c.CurrentReduceTask
		reply.NReduce = c.nReduce
		reply.NumberOfMapTasks = len(c.InputFiles)
		c.CurrentReduceTask++
		if c.CurrentReduceTask == c.nReduce {
			c.ReduceFinished = true
		}
		return nil
	}
	return &CallError{time.Now(), "call error"}
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

	c.server()
	return &c
}
