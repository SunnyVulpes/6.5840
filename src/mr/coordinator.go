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

type MapTaskNode struct {
	FileName   string
	IMap       int
	Processing bool
	Last       *MapTaskNode
	Next       *MapTaskNode
	ProcTime   time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	nMap        int
	nReduce     int
	iMap        int
	mapLinkLen  int
	mapLinkHead *MapTaskNode
	mapLinkTail *MapTaskNode
	mapCache    map[int]*MapTaskNode
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node := c.mapLinkHead.Next
	if c.mapLinkLen == 0 {
		//todo
		reply.TaskType = -1
		return nil
	}
	if node.Processing {
		//todo
		reply.TaskType = -1
		return nil
	}
	node.Processing = true

	reply.NReduce = c.nReduce
	reply.FileName = node.FileName
	reply.IMap = node.IMap
	reply.TaskType = 0

	c.mapLinkHead.Next = node.Next
	node.Next.Last = c.mapLinkHead
	node.Last = c.mapLinkTail.Last
	node.Next = c.mapLinkTail
	node.Last.Next = node
	c.mapLinkTail.Last = node
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case 0:
		node, ok := c.mapCache[args.ITask]
		if !ok {
			return fmt.Errorf("coordinator error: index of task non-exist %v", args.ITask)
		}
		if !node.Processing {
			return fmt.Errorf("coordinator error: unknown error, check line 83")
		}

		node.Last.Next = node.Next
		node.Next.Last = node.Last
		log.Println("finished ", node.IMap)
		delete(c.mapCache, node.IMap)
		c.mapLinkLen--
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

	// Your code here.
	if c.mapLinkLen == 0 {
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
	c.mapCache = map[int]*MapTaskNode{}
	c.mapLinkLen = 0
	c.mapLinkHead = &MapTaskNode{FileName: "head"}
	c.mapLinkTail = &MapTaskNode{FileName: "tail", Last: c.mapLinkHead}
	c.mapLinkHead.Next = c.mapLinkTail
	for i, fileName := range files {
		node := &MapTaskNode{
			FileName:   fileName,
			IMap:       i,
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

	c.nReduce = nReduce

	c.server()
	return &c
}
