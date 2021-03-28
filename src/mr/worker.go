package mr

import (
	"sort"
	"time"
	"os"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"strconv"
	"io/ioutil"
	"encoding/json"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	// there is a loop inside the `CallTask` funciton
	
	failCount := 0
	for true{
		arg := EmptyArg{}
		taskInfo := TaskInfo{}

		success := call("Coordinator.AskForTask", &arg, &taskInfo)

		if !success {
			failCount++
			if(failCount > 10){
				fmt.Print("Coordinator seems dead, I will exit!\n")	
				break
			}
			fmt.Print("RPC failed, Sleep for 2 secondes!\n")
			time.Sleep(time.Second*2)
			continue
		}

		switch taskInfo.TaskType{
		case TaskTypeMap:
			ExecuteMapTask(mapf, &taskInfo)
		case TaskTypeReduce:
			ExecuteReduceTask(reducef, &taskInfo)
		case TaskTypeWait:
			fmt.Print("Task Wait!, Sleep for 2 seconds!\n")
			time.Sleep(time.Second*2)
		case TaskTypeExit:
			fmt.Print("Process Exit!\n")
			return
		default:
			fmt.Print("Unknown Task Type!\n")
		}
	}
}

func ExecuteMapTask(mapf func(string, string) []KeyValue, taskInfo *TaskInfo) {
	fmt.Printf("Get Map Task: %v\n", taskInfo.Id)
	nReduce := taskInfo.NReduce
	
	intermediate := []KeyValue{}
	// Read Input File
	filename := taskInfo.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))
	
	// output to intermediate file
	onames := make([]string, nReduce)
	oFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i:=0; i < nReduce; i++ {
		onames[i] = intermediateFileName(taskInfo.Id, i)
		oFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-temp-*")
		encoders[i] = json.NewEncoder(oFiles[i])
	}

	for i:=0; i<len(intermediate); i++ {
		key := intermediate[i].Key
		if err := encoders[ihash(key)%nReduce].Encode(intermediate[i]); err != nil{
			fmt.Printf("Encode file %v failed: %v", onames[ihash(key)%nReduce], err)
			panic("Encode error")
		}
	}

	for idx, file := range oFiles {

		newpath := onames[idx]
		oldpath := file.Name()
		os.Rename(oldpath, newpath)

		file.Close()
	}
	
	call("Coordinator.TaskFinishAck", taskInfo, &EmptyArg{})
}

func ExecuteReduceTask(reducef func(string, []string) string, taskInfo *TaskInfo) {
	// * Reduce  Reduce 不需要 Filename ，读取所有 mr-X-<ID | ReduceIndex> 文件
		// 	* TaskType : TaskTypeReduce
		// 	* ReduceIndex ｜ ID 
		// 	* NFile ： 知道最开始有几个文件，就能知道 有多少X的编号都有哪些，然后遍历
		// 	* NRudece 暂时没有用
	fmt.Printf("Get Reduce Task: %v\n", taskInfo.Id)
	nFile := taskInfo.NFile
	
	reduceTaskId := taskInfo.Id
	oname := "mr-out-"+strconv.Itoa(reduceTaskId)

	intermediate := make([]KeyValue,0) 
	for i := 0; i < nFile; i++ {
		iname :=intermediateFileName(i, reduceTaskId)
		file, err := os.Open(iname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", iname, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile("mr-tmp", "mr-tmp-*")
	if err != nil{
		fmt.Printf("Create output file %v failed: %v\n", oname, err)
		panic("Create file error")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = os.Rename(ofile.Name(), oname)
	if err != nil{
		fmt.Printf("Ranme tmp file failed%v\n", oname)
		panic("Rename file error")
	}
	ofile.Close()
	call("Coordinator.TaskFinishAck", taskInfo, &EmptyArg{})
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func intermediateFileName(X int, Y int) string{
	s := "mr-tmp/mr-"
	s += strconv.Itoa(X) + "-" + strconv.Itoa(Y)
	return s
}