// TODO: need to also implement the 10s timeout rule
package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	mapTasksPending       []string   // file names of the mapping tasks that haven't been started yet
	mapTasksInProgress    []string   // file names of the mapping tasks that are currently in progress
	reduceTasksPending    []string   // file names of the reduce tasks that haven't been started yet
	reduceTasksInProgress []string   // file names of the reduce tasks that are in currently progress
	varLock               sync.Mutex // lock to access struct variables
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Task(args *TaskArgs, reply *TaskReply) error {
	// send back a task to the worker
	m.varLock.Lock()
	defer m.varLock.Unlock()
	switch args.Command {
	case "TASK": // worker is asking for a task
		if len(m.mapTasksPending) > 0 { // if we still have map tasks pending
			// send a map task
			reply.TaskName = "MAP"
			reply.File = m.mapTasksPending[len(m.mapTasksPending)-1]
			m.mapTasksInProgress = append(m.mapTasksInProgress, m.mapTasksPending[len(m.mapTasksPending)-1]) // add to in progress
			m.mapTasksPending = m.mapTasksPending[:len(m.mapTasksPending)-1]                                 // remove last item
			return nil                                                                                       // all went well, so no error
		} else if len(m.mapTasksInProgress) > 0 { // if we still have map tasks in progress but none pending
			// ask worker to wait
			reply.TaskName = "WAIT"
			return nil
		} else if len(m.reduceTasksPending) > 0 { // if all map tasks are done and we have reduce tasks pending
			// send a reduce task
			reply.TaskName = "MAP"
			reply.File = m.reduceTasksPending[len(m.mapTasksPending)-1]
			m.mapTasksInProgress = append(m.reduceTasksInProgress, m.reduceTasksPending[len(m.reduceTasksPending)-1]) // add to in progress
			m.mapTasksPending = m.reduceTasksPending[:len(m.reduceTasksPending)-1]                                    // remove last item
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
	case "RESULT": // worker is sending back result of task
		switch args.TaskName {
		case "MAP": // we are getting the results of a map task
		case "REDUCE": // we are getting the results of a reduce task
		}
	}

	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapTasksPending = make([]string, 0)
	for _, fileName := range files {
		m.mapTasksPending = append(m.mapTasksPending, fileName)
	}
	m.server()
	return &m
}
