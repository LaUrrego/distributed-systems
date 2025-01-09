package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// mapf is of form (filename, content) string
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task, availTask := GetTask()
		if !availTask {
			break
		}
		if task.WaitForTask {
			time.Sleep(5 * time.Second)
			continue
		}

		if task.Job == MAP {
			mapTaskHandler(mapf, task)
		} else if task.Job == REDUCE {
			reduceTaskHandler()
		}

	}
}

func mapTaskHandler(mapf func(string, string) []KeyValue, task *GetTaskReply) {

	fileName := task.FileName
	contents, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Failed to read file: %v", fileName)
		panic(err)
	}

	kva := mapf(fileName, string(contents))

	writeIntermediatefiles(kva, task.Index, task.ReduceCount)

}

// Function to write intermediate temp files and rename once successful
func writeIntermediatefiles(kva []KeyValue, mapTaskIndex int, reduceCount int) {

	// pointers to intermediate files and encoders for each
	encoders := make(map[int]*json.Encoder)
	files := make(map[int]*os.File)

	for _, kv := range kva {
		reducer := ihash(kv.Key) % reduceCount
		fileName := fmt.Sprintf("mr-%v-%v-tmp", mapTaskIndex, reducer)

		if _, exists := files[reducer]; !exists {
			file, err := os.Create(fileName)
			if err != nil {
				log.Fatalf("Error creating file: %v, error: %v", fileName, err)
			}
			files[reducer] = file
			encoders[reducer] = json.NewEncoder(file)
		}

		err := encoders[reducer].Encode(&kv)
		if err != nil {
			log.Fatalf("Error encoding to file: %v, error: %v", fileName, err)
		}
	}

	// atomically rename each file

	for i, file := range files {
		file.Close()
		finalName := fmt.Sprintf("mr-%v-%v", mapTaskIndex, i)
		tempName := fmt.Sprintf("mr-%v-%v-tmp", mapTaskIndex, i)

		err := os.Rename(tempName, finalName)
		if err != nil {
			log.Fatalf("Failed to rename: %v, Error: %v", tempName, err)
		}
	}
}

func reduceTaskHandler() {}

// RPC call returning a valid task from the coordinator if available
// If communication fails, assume job is completely done and send signal to exit
func GetTask() (*GetTaskReply, bool) {
	args := GetTaskArg{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply, true
	} else {
		return nil, false
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
