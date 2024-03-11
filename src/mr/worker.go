package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, t string) {

	for {
		req := GetTaskArgs{}
		resp := GetTaskReply{}
		call("Coordinator.GetTask", &req, &resp)
		switch resp.TaskType {
		case 0:
			log.Printf("processing %v", resp.IMap)
			if resp.TaskType == -1 {
				time.Sleep(10 * time.Second)
				continue
			}
			ti, _ := time.ParseDuration(t)
			time.Sleep(ti)
			file, err := os.Open(resp.FileName)
			if err != nil {
				log.Fatal("worker error: open named file failed ", resp.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatal("worker error: read named file failed ", err)
			}
			file.Close()
			intermediate := mapf(resp.FileName, string(content))
			sort.Sort(ByKey(intermediate))

			outEncs := make([]*json.Encoder, resp.NReduce)
			for i := 0; i < resp.NReduce; i++ {
				file, err := os.OpenFile(fmt.Sprintf("./inter/mr-%v-%v", resp.IMap, i), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					log.Fatal("worker error: create intermediate file failed ", err)
				}
				outEncs[i] = json.NewEncoder(file)
			}
			for _, kv := range intermediate {
				index := ihash(kv.Key) % resp.NReduce
				err := outEncs[index].Encode(kv)
				if err != nil {
					log.Fatal("worker error: write intermediate file failed ", err)
				}
			}

			call("Coordinator.FinishTask", &FinishTaskArgs{TaskType: resp.TaskType, ITask: resp.IMap}, &FinishTaskReply{})
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
