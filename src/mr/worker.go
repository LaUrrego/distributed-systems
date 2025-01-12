package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			fmt.Print("Waiting for task...\n")
			time.Sleep(5 * time.Second)
		} else {
			if task.Job == MAP {
				mapTaskHandler(mapf, task)
			} else if task.Job == REDUCE {
				reduceTaskHandler(reducef, task)
			}
		}

	}
}

func mapTaskHandler(mapf func(string, string) []KeyValue, task *GetTaskReply) {
	//fmt.Print("Starting mapTaskHandler\n")

	fileName := task.FileName
	//fmt.Printf("Reading file: [%v]\n", fileName)
	contents, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Printf("Failed to read file: %v, error: %v\n", fileName, err)
		panic(err)
	}

	kva := mapf(fileName, string(contents))

	err = writeIntermediatefiles(kva, task.Index, task.ReduceCount)
	if err != nil {
		fmt.Printf("Error completing writeIntermediateFiles on task[%v], error: %v\n", task.Index, err)
		return
	}

	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		if ok := MarkTaskComplete(task); ok {
			//fmt.Printf("Task %v marked complete.\n", task.Index)
			return
		}
		fmt.Printf("Retrying MarkTaskComplete RPC request for task[%v] of job[%v]. Attempt: %v\n", task.Index, task.Job, attempt)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("Unable to reach coordinator and mark task [%v] completed. Tried %v times.\n", task.Index, maxRetries)
}

// Function to write intermediate temp files and rename once successful
func writeIntermediatefiles(kva []KeyValue, mapTaskIndex int, reduceCount int) error {

	// pointers to intermediate files and encoders for each
	encoders := make(map[int]*json.Encoder)
	files := make(map[int]*os.File)
	createdFiles := []string{}

	for _, kv := range kva {
		reducer := ihash(kv.Key) % reduceCount
		fileName := fmt.Sprintf("mr-%v-%v-tmp", mapTaskIndex, reducer)

		if _, exists := files[reducer]; !exists {
			file, err := os.Create(fileName)
			if err != nil {
				return fmt.Errorf("error creating file: %v, error: %v", fileName, err)
			}
			files[reducer] = file
			encoders[reducer] = json.NewEncoder(file)
			createdFiles = append(createdFiles, fileName)
		}

		err := encoders[reducer].Encode(&kv)
		if err != nil {
			cleanUpTempFiles(createdFiles)
			return fmt.Errorf("error encoding to file: %v, error: %v", fileName, err)
		}
	}

	// atomically rename each file

	for i, file := range files {
		if err := file.Close(); err != nil {
			cleanUpTempFiles(createdFiles)
			return fmt.Errorf("failed to close file: [%v]", file)
		}
		finalName := fmt.Sprintf("mr-%v-%v", mapTaskIndex, i)
		tempName := fmt.Sprintf("mr-%v-%v-tmp", mapTaskIndex, i)

		err := os.Rename(tempName, finalName)
		if err != nil {
			cleanUpTempFiles(createdFiles)
			return fmt.Errorf("failed to rename: %v, Error: %v", tempName, err)
		}
	}
	return nil
}

// Function to cleanup created files on failure
func cleanUpTempFiles(createdFiles []string) {
	for _, file := range createdFiles {
		os.Remove(file)
	}
}

func reduceTaskHandler(reducef func(string, []string) string, task *GetTaskReply) {

	//fmt.Printf("Arrived at Reduce Handler: Task[%v]\n", task.Index)

	kvs, intermediateFiles := reduceIntermediateFileReader(task.Index, task.MapCount)
	sort.Sort(ByKey(kvs))

	outputFile := fmt.Sprintf("mr-out-%d", task.Index)
	outFile, _ := os.Create(outputFile)

	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	outFile.Close()

	// Cleanup intermediate files
	//fmt.Print("Cleaning up intermediate files...\n")
	cleanUpTempFiles(intermediateFiles)

	// Mark task complete once done, handle RPC communication failure
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		if ok := MarkTaskComplete(task); ok {
			//fmt.Printf("Task %v marked complete.\n", task.Index)
			return
		}
		//fmt.Printf("Retrying MarkTaskComplete RPC request for task[%v] of job[%v]. Attempt: %v\n", task.Index, task.Job, attempt)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("Unable to reach coordinator and mark task [%v] completed. Tried %v times.\n", task.Index, maxRetries)

}

func reduceIntermediateFileReader(reducerNum int, mapTaskCount int) ([]KeyValue, []string) {
	kva := []KeyValue{}
	intermediateFiles := []string{}

	for mapTask := 0; mapTask < mapTaskCount; mapTask++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapTask, reducerNum)
		file, err := os.Open(fileName)
		if err != nil {
			continue
		}
		defer file.Close()
		intermediateFiles = append(intermediateFiles, fileName)

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva, intermediateFiles

}

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

// RPC call to mark a task complete
func MarkTaskComplete(task *GetTaskReply) bool {
	args := TaskCompleteArg{
		Index: task.Index,
		Job:   task.Job,
	}
	reply := TaskCompleteReply{}
	ok := call("Coordinator.MarkTaskComplete", &args, &reply)
	if ok {
		//fmt.Printf("Task: %v[%v] marked complete.\n", task.Job, task.Index)
		return true
	} else {
		fmt.Printf("Error marking task %v of %v job complete, please try again.\n", task.Index, task.Job)
		return false
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
