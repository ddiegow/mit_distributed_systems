// TODO: need to also implement the 10s timeout rule
package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

type mapTask struct {
	fileName   string
	fileNumber int
}
type Master struct {
	// Your definitions here.
	mapTasksPending       []mapTask  // file names of the mapping tasks that haven't been started yet
	mapTasksInProgress    []mapTask  // file names of the mapping tasks that are currently in progress
	reduceTasksPending    []int      // file names of the reduce tasks that haven't been started yet
	reduceTasksInProgress []int      // file names of the reduce tasks that are in currently progress
	varLock               sync.Mutex // lock to access struct variables
	nReduce               int
	totalFiles            int
	mu                    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Task(args *TaskArgs, reply *TaskReply) error {
	// send back a task to the worker
	m.varLock.Lock()
	defer m.varLock.Unlock()
	reply.NReduce = m.nReduce
	switch args.Command {
	case "TASK": // worker is asking for a task
		if len(m.mapTasksPending) > 0 { // if we still have map tasks pending
			// send a map task
			index := len(m.mapTasksPending) - 1
			reply.TaskName = "MAP"
			reply.File = m.mapTasksPending[index].fileName
			reply.TaskId = m.mapTasksPending[index].fileNumber
			m.mu.Lock()
			m.mapTasksInProgress = append(m.mapTasksInProgress, m.mapTasksPending[index]) // add to in progress
			m.mapTasksPending = m.mapTasksPending[:index]
			m.mu.Unlock() // remove last item
			timer := time.NewTimer(10 * time.Second)
			go func() {
				<-timer.C
				m.mu.Lock()
				task := mapTask{fileName: reply.File, fileNumber: reply.TaskId}
				if slices.Index(m.mapTasksInProgress, task) != -1 {
					m.mapTasksPending = append(m.mapTasksPending, task)
					slices.DeleteFunc(m.mapTasksInProgress, func(mt mapTask) bool { return mt.fileName == task.fileName && mt.fileNumber == task.fileNumber })
				}
				m.mu.Unlock()
			}()
			return nil // all went well, so no error
		} else if len(m.mapTasksInProgress) > 0 { // if we still have map tasks in progress but none pending
			// ask worker to wait
			reply.TaskName = "WAIT"
			return nil
		} else if len(m.reduceTasksPending) > 0 { // if all map tasks are done and we have reduce tasks pending
			// send a reduce task
			reply.TaskName = "REDUCE"
			reply.TotalFiles = m.totalFiles
			reply.ReduceTaskNumber = m.reduceTasksPending[len(m.reduceTasksPending)-1]
			m.mu.Lock()
			m.reduceTasksInProgress = append(m.reduceTasksInProgress, m.reduceTasksPending[len(m.reduceTasksPending)-1]) // add to in progress
			m.reduceTasksPending = m.reduceTasksPending[:len(m.reduceTasksPending)-1]                                    // remove last item
			m.mu.Unlock()
			timer := time.NewTimer(10 * time.Second)
			go func() {
				<-timer.C
				m.mu.Lock()
				if slices.Index(m.reduceTasksInProgress, reply.ReduceTaskNumber) != -1 {
					m.reduceTasksPending = append(m.reduceTasksPending, reply.ReduceTaskNumber)
					slices.DeleteFunc(m.reduceTasksInProgress, func(i int) bool { return reply.ReduceTaskNumber == i })
				}
				m.mu.Unlock()
			}()
			return nil
		} else if len(m.reduceTasksInProgress) > 0 { // if there are no reduce tasks pending but there are some in progress
			// ask worker to wait
			reply.TaskName = "WAIT"
			return nil
		} else { // everything is finished!
			// send termination signal

			reply.TaskName = "TERMINATE"
			return nil
		}
	case "RESULT": // worker is sending back confirmation of a completed task
		switch args.TaskName {
		case "MAP": // we are getting confirmation of a map task completed
			filename := args.File
			m.mu.Lock()                                                                                                              // get the name of the file that was processed
			m.mapTasksInProgress = slices.DeleteFunc(m.mapTasksInProgress, func(mt mapTask) bool { return mt.fileName == filename }) // remove it from the list
			m.mu.Unlock()
			return nil
		case "REDUCE": // we are getting the results of a reduce task
			taskNumber := args.ReduceTaskNumber
			m.mu.Lock()
			m.reduceTasksInProgress = slices.DeleteFunc(m.reduceTasksInProgress, func(i int) bool { return i == taskNumber })
			m.mu.Unlock()
			return nil
		}
	}

	return errors.New("Something went wrong in the RPC handler")
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {

	ret := len(m.mapTasksPending) == 0 && len(m.mapTasksInProgress) == 0 && len(m.reduceTasksInProgress) == 0 && len(m.reduceTasksPending) == 0
	if ret {
		// delete all intermediate files
		for i := 0; i < m.totalFiles; i++ {
			for j := 0; j < m.nReduce; j++ {
				os.Remove("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j))
			}
		}
	}
	// Your code here.
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.mapTasksPending = make([]mapTask, 0)
	m.reduceTasksPending = make([]int, 0)
	for i := 0; i < len(files); i++ {
		m.mapTasksPending = append(m.mapTasksPending, mapTask{fileName: files[i], fileNumber: i})
	}

	m.totalFiles = len(files)
	for i := 0; i < nReduce; i++ {
		m.reduceTasksPending = append(m.reduceTasksPending, i)
	}
	m.server()
	return &m
}
