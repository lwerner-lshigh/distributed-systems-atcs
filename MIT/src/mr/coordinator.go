package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	reduceTasks     int
	files           []string
	processingFiles []string
	completedFiles  []string
	fileMutex       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterMapTask(args *TaskArgs, reply *TaskReply) error {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	// pop file off stack
	file := c.files[len(c.files)-1]
	c.files = c.files[:len(c.files)-1]
	reply.File = file

	go c.WorkerTimeoutHandler(file, 10*time.Second)
	return nil
}

func (c *Coordinator) FinishedMapTask(args *FinishArgs, reply *FinishReply) error {
	// wait for maps to finish
	c.fileMutex.Lock()
	c.processingFiles = append(c.processingFiles, args.File)
	c.fileMutex.Unlock()

	return nil
}

func (c *Coordinator) TaskFailed(args *TaskFailedArgs, reply *TaskFailedReply) error {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	c.files = append(c.files, args.File)
	fmt.Println(args.Reason.Error())
	return nil
}

func (c *Coordinator) WorkerTimeoutHandler(file string, timeout time.Duration) {
	<-time.After(timeout) // wait for timeout
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	if !itemExists(c.processingFiles, file) {
		c.files = append(c.files, file)
	}

}

func itemExists(slice interface{}, item interface{}) bool {
	s := reflect.ValueOf(slice)

	if s.Kind() != reflect.Slice {
		panic("Invalid data-type")
	}

	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == item {
			return true
		}
	}

	return false
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.reduceTasks = nReduce

	c.server()
	return &c
}
