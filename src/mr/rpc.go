package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "strings"
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

// 这里会定义很多状态
type AssignedTaskType int32

// 枚举就是const
// const 模拟枚举
// Don't use magic number
const (
	TaskTypeMap AssignedTaskType = 1
	TaskTypeReduce AssignedTaskType = 2
	TaskTypeWait	AssignedTaskType = 3
	TaskTypeExit AssignedTaskType = 4
)

// 请求不需要传递任何信息
type EmptyArg struct {
}

// TaskInfo is the message passed from coordinator to worker as "Reply"
type TaskInfo struct{
	// 本次Task的处理方式
	TaskType AssignedTaskType
	// 文本的名字
	File string
	Id int
	// 分成几个 Reduce Task
	NReduce int
	// 最开始有多少个输入
	NFile int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func TaskNotFoundErrMsg(id int) string {
	s := "Task not found, indexNumebr: "
	s += strconv.Itoa(id)
	return s
}

func MakeAddSuffix(suffix string) func(string) string {
    return func(name string) string {
        if !strings.HasSuffix(name, suffix) {
        		return name + suffix
        }
        return name
    }
}

func MakeAddPrefix(suffix string) func(string) string {
    return func(name string) string {
        if !strings.HasSuffix(name, suffix) {
        		return suffix + name
        }
        return name
    }
}