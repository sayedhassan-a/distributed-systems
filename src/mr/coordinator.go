package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	InputFiles []string
	CurrentFile int
	MapFinished    bool
	ReduceFinished bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	if !c.MapFinished {
		reply.TaskType = MapTaskType
		reply.Filename = c.InputFiles[c.CurrentFile]
		reply.nReduce = c.nReduce
		c.CurrentFile++

	} else {
		reply.TaskType = ReduceTaskType
	}
	return nil
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
	fmt.Println(files)
	c := Coordinator{}
	// Your code here.
	c.InputFiles = files
	c.MapFinished = false
	c.ReduceFinished = false
	c.CurrentFile = 0
	c.nReduce = nReduce

	c.server()
	return &c
}
