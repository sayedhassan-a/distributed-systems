package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type CallError struct {
	timestamp time.Time
	message   string
}

func (e *CallError) Error() string {
	return fmt.Sprintf("%s at %v", e.message, e.timestamp)
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply, error := CallGetTask(3)
		if error != nil {
			break
		}
		if reply.TaskType == MapTaskType {
			RunMapTask(mapf, &reply)
			CallFinishedTask(3, &FinishedTaskArgs{MapTaskType, reply.TaskNumber})
		} else if reply.TaskType == ReduceTaskType {
			RunReduceTask(reducef, &reply)
			CallFinishedTask(3, &FinishedTaskArgs{ReduceTaskType, reply.TaskNumber})

		} else if reply.TaskType == WaitTaskType {
			time.Sleep(5000 * time.Millisecond)
			continue
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallGetTask(retry int) (RequestTaskReply, error) {

	args := RequestTaskArgs{}
	//TODO: fill args

	reply := RequestTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		return reply, nil
	} else {
		if retry > 0 {
			fmt.Println("call failed")
			return CallGetTask(retry - 1)
		}
		fmt.Println("server is down")
		return RequestTaskReply{}, &CallError{time.Now(), "call failed"}
	}
}

func CallFinishedTask(retry int, args *FinishedTaskArgs) error {
	reply := FinishedTaskReply{}

	ok := call("Coordinator.FinishedTask", &args, &reply)

	if ok {
		return nil
	} else {
		if retry > 0 {
			fmt.Println("call failed")
			return CallFinishedTask(retry-1, args)
		}
		fmt.Println("server is down")
		return &CallError{time.Now(), "call failed"}
	}
}

func RunMapTask(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	filename := reply.Filename
	nReduce := reply.NReduce
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	keyValueList := mapf(filename, string(content))

	keyValueBuckets := make([][]KeyValue, nReduce)
	for _, keyValue := range keyValueList {
		hash := ihash(keyValue.Key)
		key := hash % nReduce
		keyValueBuckets[key] = append(keyValueBuckets[key], keyValue)
	}
	for _, keyValueList := range keyValueBuckets {
		if len(keyValueList) != 0 {
			WriteToFile(keyValueList, reply, ihash(keyValueList[0].Key)%reply.NReduce)
		}
	}
}

func WriteToFile(keyValueList []KeyValue, reply *RequestTaskReply, reduceTask int) {
	filename := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, reduceTask)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.Encode(keyValueList)

}

func RunReduceTask(reducef func(string, []string) string, reply *RequestTaskReply) {

	intermediate := make(map[string][]string, 0)
	for i := range reply.NumberOfMapTasks {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)

		var keyValueList []KeyValue
		dec.Decode(&keyValueList)

		for _, keyValue := range keyValueList {
			intermediate[keyValue.Key] = append(intermediate[keyValue.Key], keyValue.Value)
		}
		file.Close()

	}
	outputFilename := fmt.Sprintf("mr-out-%d", reply.TaskNumber)
	outputFile, _ := os.Create(outputFilename)
	for key, valueList := range intermediate {
		output := reducef(key, valueList)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}

}

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
