package mr

import (
	"fmt"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"errors"
)

// 放在队列里面，
type TaskNode struct {
	WorkerId int
	Filename string
}

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap int
	files []string
	finished bool

	// 等待Map的队列，存放文件名称
	queue1 []string
	// 等待Reduce的队列，存放中间文件名称
	queue2 []string
	
	// 当前正在进行的 worker， 用 Map 结构会好点
	MapWorkerList map[int]string
	ReduceWorkerList map[int]string

	// 锁
	mutex sync.Mutex
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

func (c *Coordinator) Task(args *WorkRequestArgs, reply *WorkReply) error {
	//runs the handler for each RPC in its own thread. There has data race with the `Done()` method.
	// the worker ask for a task to process
	// Assign map task
	if len(c.queue1) > 0 {
		c.mutex.Lock()

		reply.Code = 1
		// Map 任务
		reply.WorkType = 1
		
		// 给任务
		filename := c.queue1[0]
		// 分配
		reply.Filename = filename

		reply.nReduce = 10

		// 调整coordinator的状态
		// queue1 队列出队
		c.queue1 = c.queue1[1:len(c.queue1)]
		c.MapWorkerList[args.WorkerId] = filename

		c.mutex.Unlock()

	} else if len(c.MapWorkerList) > 0 {
		// 没有文件要读取，而 Map也还没有执行完成
		reply.Code = 0
	}else if len(c.MapWorkerList) == 0 && len(c.queue2) > 0{ // Map work完全结束
		reply.Code = 1
		// Reduce 任务
		reply.WorkType = 2
		c.mutex.Lock()
		// 给任务
		filename := c.queue2[0]
		// 分配
		reply.Filename = filename

		// 调整coordinator的状态
		// queue2 队列出队
		c.queue2 = c.queue2[1:len(c.queue2)]
		c.ReduceWorkerList[args.WorkerId] = filename

		c.mutex.Unlock()
	} else if len(c.ReduceWorkerList) > 0{
		// queue1 queue2 为空， 但是 reduce任务还没结束
		reply.Code = 0
	} else {
		// 全部结束了，告诉worker可以退出
		reply.Code = -1
	}

	return nil;
}

func (c *Coordinator) TaskFinish(args *NoticeCoorninatorArg, reply *NoticeCoorninatorReply ) error {

	workerId := args.WorkerId
	// Task Type
	workType := args.WorkType

	if workType == 0 {
		// Map 结束
		if _, ok := c.MapWorkerList[workerId] ; ok == false {
			// 不存在
			return errors.New(workeridNotFoundErrMsg(workerId))
		}
		if args.Filename != c.MapWorkerList[workerId] {
			return errors.New("Wrong file name")
		}
		// 存在
		c.mutex.Lock()
		delete(c.MapWorkerList, workerId)
		c.mutex.Unlock()
	} else if workType == 1{
		// Reduce 任务结束
		if _, ok := c.ReduceWorkerList[workerId] ; ok == false {
			// 不存在
			return errors.New(workeridNotFoundErrMsg(workerId))
		}
		// 存在
		if args.Filename != c.ReduceWorkerList[workerId] {
			return errors.New("Wrong file name")
		}
		c.mutex.Lock()
		delete(c.ReduceWorkerList, workerId)
		c.mutex.Unlock()
	}
	
	return nil
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
	// 这里有数据竞争 data race
	ret = c.finished

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.files = files
	c.finished = false
	// Your code here.

	// load queue1 and queue2, now queue2 is empty
	c.queue1 = append(c.queue1, c.files...)
	fmt.Printf("queue1: \n")
	for i:=0; i < len(c.queue1); i++ {
		fmt.Println(c.queue1[i])
	}
	c.MapWorkerList = map[int]string{}
	c.ReduceWorkerList = map[int]string{}
	fmt.Printf("queue2: \n")
	for i:=0; i < nReduce; i++ {
		c.queue2 = append(c.queue2, intermediateFileName(i))
		fmt.Println(intermediateFileName(i))
	}
	// 开启 rpc
	c.server()
	return &c
}
