package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskNode struct {
	FileName   string
	IFile      int
	Processing bool
	Last       *TaskNode
	Next       *TaskNode
	ProcTime   time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	nMap    int
	nReduce int
	iMap    int

	mapLinkLen  int
	mapLinkHead *TaskNode
	mapLinkTail *TaskNode
	mapCache    map[int]*TaskNode

	rdcLinkLen  int
	rdcLinkHead *TaskNode
	rdcLinkTail *TaskNode
	rdcCache    map[int]*TaskNode
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

const (
	MapTask = iota
	ReduceTask
	Wait
	Done
)

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapLinkLen == 0 {
		if c.rdcLinkLen == 0 {
			reply.TaskType = Wait
			return nil
		}
		node := c.rdcLinkHead.Next
		if node.Processing {
			if node.ProcTime.Add(10 * time.Second).Before(time.Now()) {
				c.dispatchReduce(node, reply)
			} else {
				reply.TaskType = Wait
			}
			return nil
		}
		c.dispatchReduce(node, reply)
		return nil
	}
	node := c.mapLinkHead.Next
	if node.Processing {
		if node.ProcTime.Add(10 * time.Second).Before(time.Now()) {
			c.dispatchMap(node, reply)
		} else {
			reply.TaskType = Wait
		}
		return nil
	}
	c.dispatchMap(node, reply)
	return nil
}

func (c *Coordinator) dispatchMap(node *TaskNode, reply *GetTaskReply) {
	node.Processing = true
	node.ProcTime = time.Now()

	reply.NReduce = c.nReduce
	reply.FileName = node.FileName
	reply.IFile = node.IFile
	reply.TaskType = MapTask

	c.mapLinkHead.Next = node.Next
	node.Next.Last = c.mapLinkHead
	node.Last = c.mapLinkTail.Last
	node.Next = c.mapLinkTail
	node.Last.Next = node
	c.mapLinkTail.Last = node
}

func (c *Coordinator) dispatchReduce(node *TaskNode, reply *GetTaskReply) {
	node.Processing = true
	node.ProcTime = time.Now()

	reply.IFile = node.IFile
	reply.NMap = c.nMap
	reply.TaskType = ReduceTask

	c.rdcLinkHead.Next = node.Next
	node.Next.Last = c.rdcLinkHead
	node.Last = c.rdcLinkTail.Last
	node.Next = c.rdcLinkTail
	node.Last.Next = node
	c.rdcLinkTail.Last = node
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		node, ok := c.mapCache[args.ITask]
		if !ok {
			return fmt.Errorf("coordinator error: index of map task non-exist %v", args.ITask)
		}
		if !node.Processing {
			return fmt.Errorf("coordinator error: unknown error, check line 129")
		}

		node.Last.Next = node.Next
		node.Next.Last = node.Last
		//log.Println("finished ", node.IFile)
		delete(c.mapCache, node.IFile)
		c.mapLinkLen--

		if c.mapLinkLen == 0 {
			for i := 0; i < c.nReduce; i++ {
				node := &TaskNode{
					IFile:      i,
					ProcTime:   time.Now(),
					Processing: false,
					Last:       c.rdcLinkTail.Last,
					Next:       c.rdcLinkTail,
				}
				c.rdcLinkTail.Last.Next = node
				c.rdcLinkTail.Last = node
				c.rdcCache[node.IFile] = node
				c.rdcLinkLen++
			}
		}
	case ReduceTask:
		node, ok := c.rdcCache[args.ITask]
		if !ok {
			return fmt.Errorf("coordinator error: index of reduce task non-exist %v", args.ITask)
		}
		if !node.Processing {
			return fmt.Errorf("coordinator error: unknown error, check line 154")
		}
		node.Last.Next = node.Next
		node.Next.Last = node.Last
		delete(c.rdcCache, node.IFile)
		c.rdcLinkLen--
	default:
		panic("unhandled default case")
	}
	return nil
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	log.Println(c.rdcLinkLen)

	// Your code here.
	if c.mapLinkLen == 0 && c.rdcLinkLen == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.ß
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapCache = map[int]*TaskNode{}
	c.mapLinkLen = 0
	c.mapLinkHead = &TaskNode{FileName: "head"}
	c.mapLinkTail = &TaskNode{FileName: "tail", Last: c.mapLinkHead}
	c.mapLinkHead.Next = c.mapLinkTail
	for i, fileName := range files {
		node := &TaskNode{
			FileName:   fileName,
			IFile:      i,
			Last:       c.mapLinkTail.Last,
			Next:       c.mapLinkTail,
			Processing: false,
			ProcTime:   time.Now(),
		}
		c.mapLinkTail.Last.Next = node
		c.mapLinkTail.Last = node
		c.mapCache[i] = node
		c.mapLinkLen++
	}

	c.rdcCache = map[int]*TaskNode{}
	c.rdcLinkLen = 0
	c.rdcLinkHead = &TaskNode{FileName: "head"}
	c.rdcLinkTail = &TaskNode{FileName: "tail", Last: c.rdcLinkHead}
	c.rdcLinkHead.Next = c.rdcLinkTail

	c.nMap = len(files)
	c.nReduce = nReduce

	c.server()
	return &c
}
