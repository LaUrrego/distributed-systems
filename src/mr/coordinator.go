package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Task status state
type TaskStatus int

const (
	UNASSIGNED TaskStatus = iota
	ASSIGNED
	COMPLETED
)

// Type of job currently being performed
type JobType int

const (
	MAP JobType = iota
	REDUCE
)

// A task has a status state and initiated time to track whether it needs to be reassigned after 10 seconds
type Task struct {
	Status  TaskStatus
	Started time.Time
}

// Coordinator keeps track of a collection of input files, MapTasks, ReduceTasks, and
// uses a Completed channel to signal the end of it's period worker health checks. Mutex
// is used to keep the structure thread safe
type Coordinator struct {
	Files       []string
	MapTasks    []Task
	ReduceTasks []Task
	mu          sync.Mutex
	Completed   chan struct{}
}

//////////////////////////////////////////////////////////
// Your code here -- RPC handlers for the worker to call./
//////////////////////////////////////////////////////////

// Getting a task (MAP or REDUCE) doesn't require an arg,
type GetTaskArg struct{}

type GetTaskReply struct {
	Job         JobType // MAP || REDUCE
	FileName    string  // File the worker will be assigned
	WaitForTask bool    // Flag indicating whether there are still Map jobs left to complete.
	Index       int     // Index number of the current task
	MapCount    int     // Number of total Map tasks
	ReduceCount int     // Number of total Reduce tasks
}

// Completing a task
type TaskCompleteArg struct {
	Index int
	Job   JobType
}

type TaskCompleteReply struct{}

// Error return to signal job completion when a worker calls GetTask via RPC
var ErrJobDone = errors.New("job completed successfully")

// General task handler for Map and Reduce tasks
func (c *Coordinator) GetTask(args *GetTaskArg, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Map-phase
	if i, task := c.findAvailableTask(c.MapTasks); i >= 0 {
		c.assignTask(i, task, reply, MAP)
		return nil
	}

	if !c.allTasksComplete(c.MapTasks) {
		reply.WaitForTask = true
		fmt.Printf("Asked to wait[MAP], send reply: %+v\n", reply)
		return nil
	}

	// Reduce-phase
	if i, task := c.findAvailableTask(c.ReduceTasks); i >= 0 {
		c.assignTask(i, task, reply, REDUCE)
		return nil
	}

	// All reduce tasks completed means the whole MR job is done
	// ensure that the channel is closed only once
	var once sync.Once

	if c.allTasksComplete(c.ReduceTasks) {
		once.Do(func() { close(c.Completed) })
		return ErrJobDone
	}

	//fmt.Printf("Asked to wait[REDUCE], send reply: %+v\n", reply)
	reply.WaitForTask = true
	return nil
}

// General function to mark tasks complete
func (c *Coordinator) MarkTaskComplete(args *TaskCompleteArg, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Job == MAP {
		c.MapTasks[args.Index].Status = COMPLETED
		return nil
	}
	c.ReduceTasks[args.Index].Status = COMPLETED
	return nil
}

// General function to find the first task indicated as UNASSIGNED
func (c *Coordinator) findAvailableTask(tasks []Task) (index int, task *Task) {

	for i := range tasks {
		if tasks[i].Status == UNASSIGNED {
			return i, &tasks[i]
		}
	}
	return -1, nil
}

// General function to assign the current task struct
func (c *Coordinator) assignTask(index int, task *Task, reply *GetTaskReply, jobType JobType) {

	task.Started = time.Now()
	task.Status = ASSIGNED
	reply.Index = index
	reply.Job = jobType
	reply.MapCount = len(c.MapTasks)
	reply.ReduceCount = len(c.ReduceTasks)
	reply.WaitForTask = false

	if jobType == MAP {
		reply.FileName = c.Files[index]
	}
	//fmt.Printf("Assigned reply: %+v\n", *reply)
}

// General function to determine if all tasks in a phase are complete
func (c *Coordinator) allTasksComplete(tasks []Task) bool {

	for _, task := range tasks {
		if task.Status != COMPLETED {
			return false
		}
	}
	return true
}

// Checker for "crashed" workers using a 10 second interval. When the job is done, a close(c.Completed)
// command triggers a ticker stop and ends the periodic check.
func (c *Coordinator) periodicChecker() {
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				c.checkAndReassign()

			case <-c.Completed:
				fmt.Printf("Received the completed signal!\n")
				ticker.Stop()
				return
			}

		}
	}()
}

// Function to reassign tasks that have exceeded the 10 second threshold
// Since UNASSIGNED is our 0-value, all are automatically status UNASSIGNED
func (c *Coordinator) checkAndReassign() {
	c.mu.Lock()
	defer c.mu.Unlock()

	//fmt.Printf("Started checkAndReasign for [%v] map tasks\n", len(c.MapTasks))

	for i, mt := range c.MapTasks {
		if mt.Status == ASSIGNED &&
			time.Since(mt.Started) >= 10*time.Second {
			c.MapTasks[i] = Task{}
		}
	}

	//fmt.Printf("Started checkAndReasign for [%v] reduce tasks\n", len(c.ReduceTasks))

	for i, rt := range c.ReduceTasks {
		if rt.Status == ASSIGNED &&
			time.Since(rt.Started) >= 10*time.Second {
			c.ReduceTasks[i] = Task{}
		}
	}
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
	fmt.Print("Server started...\n")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	mapDone := c.allTasksComplete(c.MapTasks)
	reduceDone := c.allTasksComplete(c.ReduceTasks)
	if mapDone && reduceDone {
		fmt.Print("MR Job is done!\n\n")
	}
	return mapDone && reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:       files,
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
		Completed:   make(chan struct{}),
	}

	fmt.Printf("Starting coordinator: files:[%v], No. Map Tasks [%v], No. Reduce Tasks [%v]\n\n", files, len(c.MapTasks), len(c.ReduceTasks))

	c.periodicChecker()
	c.server()
	return &c
}
