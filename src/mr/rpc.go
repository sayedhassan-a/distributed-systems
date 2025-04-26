package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Define args and reply for RPC to decouple worker and coordinator
// as to not need to change coordinator after changing Task struct in worker

type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	WaitTaskType
)

type RequestTaskArgs struct {
	TaskType TaskType
}

type RequestTaskReply struct {
	TaskType         TaskType
	Filename         string
	NReduce          int
	TaskNumber       int
	NumberOfMapTasks int
}

type FinishedTaskArgs struct {
	TaskType   TaskType
	TaskNumber int
}

type FinishedTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
