package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}



// Add your RPC definitions here.
type WorkRequestArgs struct{
	WorkerId int

}

type WorkReply struct{
	// 是否派遣任务 0: 无任务, 1: Map, or Reduce, 通过WorkType判断 -1: 全部运行结束，让worker退出
	Code int

	// 任务类型， 0:Map or 1:Reduce
	WorkType int
	// nReduce
	nReduce int
	// 传送要读取的文件名称 filename
	Filename string


}

type NoticeCoorninatorArg struct {
	Finished bool
	// Worker ID
	WorkerId int
	// Task Type
	WorkType int

	Filename string

}

type NoticeCoorninatorReply struct {
	Finished bool
	// Worker ID
	WorkerId int
	// Task Type
	WorkType int

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workeridNotFoundErrMsg(id int) string {
	s := "Can not find worker: "
	s += strconv.Itoa(id)
	return s
}

func intermediateFileName(reduce int) string {
	s := "mr_"
	s += strconv.Itoa(reduce)
	return s
}

func reduceOutFileName(reduce int) string {
	s := "mr-out-"
	s += strconv.Itoa(reduce)
	return s
}