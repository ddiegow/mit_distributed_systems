package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// ask for a task
		taskArgs := TaskArgs{
			Command: "TASK",
		}
		taskReply := TaskReply{}
		err := !call("Master.Task", &taskArgs, &taskReply)
		if err {
			fmt.Println("Something went wrong. Coudln't fetch a task")
			time.Sleep(50 * time.Millisecond)
			continue
		}
		nReduce := taskReply.NReduce
		// process return
		switch taskReply.TaskName {
		case "WAIT":
			time.Sleep(50 * time.Millisecond)
			continue
		case "TERMINATE":
			return
		case "MAP":
			// Get intermediate key-value list
			filename := taskReply.File
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			// Create tmp bucket files
			tmpFiles := make([]*os.File, 0)
			for i := 0; i < taskReply.NReduce; i++ {
				name := "mr-" + strconv.Itoa(i)
				file, err = os.CreateTemp("tmp", name)
				if err != nil {
					log.Fatalf("cannot create temporary file %v. Error: %v", name, err)
				}
				tmpFiles = append(tmpFiles, file)
			}
			// Put each key-value pair in the correct tmp bucket
			for _, kv := range kva {
				n := ihash(kv.Key) % nReduce
				tmpFiles[n].WriteString(kv.Key + " " + kv.Value + "\n")
			}
			// Once we finish, rename tmp files
			for i, file := range tmpFiles {
				file.Close()
				oldpath := file.Name()
				newpath := "tmp/mr-" + strconv.Itoa(taskReply.TaskId) + "-" + strconv.Itoa(i)

				err := os.Rename(oldpath, newpath)
				if err != nil {
					log.Panic("Couldn't rename one of the files!")
				}

			}
		case "REDUCE":
		}

	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
