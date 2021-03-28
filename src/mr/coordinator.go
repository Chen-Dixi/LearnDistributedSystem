package mr

import (
	"time"
	"fmt"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"errors"
)

// 定义接口
type TaskInterface interface{
	// 任务会不会超时
	isExpired() bool // check if the task is out of time
	setNow() // set the startTime now
	GenerateTaskInfo() TaskInfo
	setState(state TaskState)
	IndexNumber() int
}

type TaskState int32

const (
	TaskStateIdle TaskState = 1 // 未分配
	TaskStateRunning TaskState = 2 // 已经分配
	TaskStateFinished TaskState = 3 // 运行结束

	TaskExpiredTime time.Duration = time.Second*10 //
)

type Task struct{
	StartTime time.Time // start time
	InputFileName string// input file name
	TaskIndexNumber int // 
	State TaskState // task state: idle | running(assigned) | finished
	Type AssignedTaskType
	TaskId int // Identifier
	NReduce int
	NFile int

}

// 2 types of worker task: Map & Reduce
type MapTask struct{
	Task
}

type ReduceTask struct{
	Task
}

// Implement task interface
func (mt *MapTask) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		TaskType: TaskTypeMap,
		File: mt.InputFileName,
		Id: mt.TaskIndexNumber,
		NReduce: mt.NReduce,
	}
}

func (rt *ReduceTask) GenerateTaskInfo() TaskInfo{
	return TaskInfo{
		// 🤔
		TaskType: TaskTypeReduce,
		//File: rt.InputFileName, // 但是对应了很多y， 意思是肯定有这么多个y是吗		
		Id: rt.TaskIndexNumber,
		NFile: rt.NFile,
	}
}

func (t *Task) setNow() {
	t.StartTime = time.Now()
}

func (t *Task) isExpired() bool {
	since := time.Since(t.StartTime)
	return since > TaskExpiredTime
}

func (t *Task) setState(state TaskState) {
	t.State = state
}

func (t *Task) IndexNumber() int {
	return t.TaskIndexNumber
}

type TaskQueue struct{
	taskArray []TaskInterface
	mutex sync.Mutex
}

func (queue *TaskQueue) Lock() {
	queue.mutex.Lock()
}

func (queue *TaskQueue) Unlock() {
	queue.mutex.Unlock()
}

func (queue *TaskQueue) Pop() TaskInterface{
	queue.Lock()
	
	defer queue.Unlock()

	if len(queue.taskArray) == 0{
		return nil
	}

	var task = queue.taskArray[0]
	queue.taskArray = queue.taskArray[1:] // 出队
	return task
}

func (queue *TaskQueue) Push(task TaskInterface){
	queue.Lock()
	defer queue.Unlock()
	
	queue.taskArray = append(queue.taskArray, task) // 入队
}

func (queue *TaskQueue) RemoveTask(indexNumber int) (TaskInterface, error) {
	queue.Lock()
	defer queue.Unlock()

	for idx := 0; idx < len(queue.taskArray); idx++ {
		task := queue.taskArray[idx]
		if indexNumber == task.IndexNumber() {
			queue.taskArray = append(queue.taskArray[:idx], queue.taskArray[idx+1:]...)
			return task, nil
		}
	}
	return nil, errors.New("Task Not found")
}

func (queue *TaskQueue) Size() int {
	return len(queue.taskArray)
}

type CoordinatorPhase int32

const (
	Mapping CoordinatorPhase = 1
	Reducing CoordinatorPhase = 2
	Done CoordinatorPhase = 3
)

type Coordinator struct {
	// Your definitions here.
	NReduce int
	Files []string
	Phase CoordinatorPhase// 当前 Coordinator正在Map 还是 正在 Reduce

	// Map 任务队列
	MapWaitingQueue TaskQueue
	MapRunningQueue TaskQueue
	// Reduce 任务队列
	ReduceWaitingQueue TaskQueue
	ReduceRunningQueue TaskQueue

	mutex sync.Mutex
}

func (c *Coordinator) SetPhase(phase CoordinatorPhase) {
	c.mutex.Lock()
	c.Phase = phase
	c.mutex.Unlock()
}
// Your code here -- RPC handlers for the worker to call.

// the RPC argument and reply types are defined in rpc.go.
//

// Worker use the rpc method
func (c *Coordinator) AskForTask(args *EmptyArg, reply *TaskInfo) error {
	// TBD：控制台输出，分配了什么任务
	switch c.Phase{
	case Mapping:
		// 分配Map 任务，或者不分配任务。WaitingQueue 是空的，就分配一个TaskWait
		// c.MapWaitingQueue.Lock() 不在这里加锁，在函数内部加锁
		task := c.MapWaitingQueue.Pop()
		if task != nil { // 
			task.setNow()
			// task.setState(TaskStateRunning) // 目前这个没什么用
			c.MapRunningQueue.Push(task)
			*reply = task.GenerateTaskInfo() // 地址指向的对象 重新赋值
			fmt.Printf("Assign Map Task %v\n", reply.Id)
			return nil
		}
		// 没有mapping 任务
		*reply = TaskInfo{
			TaskType: TaskTypeWait,
		}
	case Reducing:
		task := c.ReduceWaitingQueue.Pop()
		if task != nil{
			task.setNow()
			c.ReduceRunningQueue.Push(task)
			*reply = task.GenerateTaskInfo() // 多态
			fmt.Printf("Assign Reduce Task %v\n", reply.Id)
			return nil
		}
		*reply = TaskInfo{
			TaskType: TaskTypeWait,
		}
	case Done:
		*reply = TaskInfo{
			TaskType: TaskTypeExit,
		}
		return nil
	default:
		return errors.New("Strange Coordinator Phase")
	}
	return nil
}

func (c *Coordinator) TaskFinishAck(info* TaskInfo, reply* EmptyArg) error{ // 也要返回error，查看是否有这个task，没有就丢弃这个消息
	// TBD 有可能在 Reducing 阶段 收到 Map Task的 完成消息
	// TBD 控制台输出：什么任务完成了
	switch info.TaskType{
	case TaskTypeMap:
		
		_, error := c.MapRunningQueue.RemoveTask(info.Id)
		if error != nil{
			return errors.New(MakeAddPrefix("Map ")(TaskNotFoundErrMsg(info.Id)))
		}
		fmt.Printf("Map Reduce Task %v\n", info.Id)
		if c.MapWaitingQueue.Size() == 0 && c.MapRunningQueue.Size() == 0 {
			c.RegisterReduceTasks()
			// 这里出现竞争
			c.SetPhase(Reducing)
		}
	case TaskTypeReduce:
		_, error := c.ReduceRunningQueue.RemoveTask(info.Id)
		if error != nil{
			return errors.New(MakeAddPrefix("Reduce ")(TaskNotFoundErrMsg(info.Id)))
		}
		fmt.Printf("Finish Reduce Task %v\n", info.Id)
		if c.ReduceWaitingQueue.Size() == 0 && c.ReduceRunningQueue.Size() == 0 {
			c.SetPhase(Done)
		}
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
	fmt.Printf("listen to socket: %v\n", sockname)
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
	// Your code here.
	// 这里有数据竞争 data race
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.Phase == Done
}

func (c *Coordinator) RegisterReduceTasks() {
	// if all the map tasks are done, start assigning reduce tasks
	taskTemp := ReduceTask{
		Task{
			NFile: len(c.Files),
			NReduce: c.NReduce,
		},
	}
	for idx := 0; idx < c.NReduce; idx++ {
		reduceTask := taskTemp
		reduceTask.TaskIndexNumber = idx
		c.ReduceWaitingQueue.Push(&reduceTask)
	}
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	// 根据files 生成队列
	mapTaskArray := make([]TaskInterface, 0) // 这里的初始化类型记住也许需要改动， 不知道应该写TaskInterface还是 Task, 决定应该写 TaskInterface
	for idx, fileName := range files {
		maptask := MapTask{
			Task{
				InputFileName: fileName,
				TaskIndexNumber: idx,
				State: TaskStateIdle,
				Type: TaskTypeMap,
				TaskId: 0, // taskid 在分配的时候再赋值？ 如何分配这个 taskId，uuid？
				NReduce: nReduce,
			},
		}
		mapTaskArray = append(mapTaskArray, &maptask)
	}
	
	c := Coordinator{
		// 指定Reduce Task 数量
		NReduce : nReduce,
		// 输入了哪些文件
		Files : files,
		// 初始化状态，正在执行Map阶段
		Phase : Mapping,
		// Map 任务等待分配队列
		MapWaitingQueue: TaskQueue{taskArray: mapTaskArray},
	}

	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Printf("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}

	// TBD: start a thread to requeue the out of time running task
	
	// 开启 rpc
	c.server()

	// 启动另一个线程 检查执行中的超时任务
	// TBD

	return &c
}