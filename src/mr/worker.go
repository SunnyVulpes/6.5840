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
	reducef func(string, []string) string) {

	for {
		req := GetTaskArgs{}
		resp := GetTaskReply{}
		call("Coordinator.GetTask", &req, &resp)
		switch resp.TaskType {
		case MapTask:
			//log.Printf("mapping %v", resp.IFile)
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
				file, err := os.OpenFile(fmt.Sprintf("./mr-%v-%v", resp.IFile, i), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
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

			call("Coordinator.FinishTask", &FinishTaskArgs{TaskType: resp.TaskType, ITask: resp.IFile}, &FinishTaskReply{})
		case ReduceTask:
			//log.Printf("reducing %v", resp.IFile)
			i := 0
			var kva []KeyValue
			for i < resp.NMap {
				file, err := os.Open(fmt.Sprintf("./mr-%v-%v", i, resp.IFile))
				if err != nil {
					log.Println("worker error: failed to open file ", i, err)
					break
				}

				dec := json.NewDecoder(file)
				for dec.More() {
					var kv KeyValue
					err := dec.Decode(&kv)
					if err != nil {
						log.Fatal("worker fatal: failed to decode file")
					}
					kva = append(kva, kv)
				}
				i++
			}

			res := map[string][]string{}
			i = 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				for k := i; k < j; k++ {
					res[kva[i].Key] = append(res[kva[i].Key], kva[k].Value)
				}
				i = j
			}

			sortedKey := []string{}
			for key := range res {
				sortedKey = append(sortedKey, key)
			}
			sort.Slice(sortedKey, func(i, j int) bool {
				return sortedKey[i] < sortedKey[j]
			})

			oname := fmt.Sprintf("mr-out-%v", resp.IFile)
			ofile, _ := os.Create(oname)

			for _, key := range sortedKey {
				values := res[key]
				output := reducef(key, values)
				fmt.Fprintf(ofile, "%v %v\n", key, output)
			}
			ofile.Close()
			call("Coordinator.FinishTask", &FinishTaskArgs{TaskType: resp.TaskType, ITask: resp.IFile}, &FinishTaskReply{})
		case Wait:
			time.Sleep(2 * time.Second)
			continue
		default:
			panic("unhandled default case")
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
