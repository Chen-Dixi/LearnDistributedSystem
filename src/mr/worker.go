package mr

import (
	"os"
	"time"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"io/ioutil"
	"sort"
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
	
	// 
	workerId := os.Getuid()
	// there is a loop inside the `CallTask` funciton
	CallTask(workerId, mapf, reducef)

}

func CallTask(workerId int, mapf func(string, string) []KeyValue,
reducef func(string, []string) string) {

	for true {
		args := WorkRequestArgs{}
		args.WorkerId = workerId

		reply := WorkReply{}

		response := call("Coordinator.Task", &args, &reply) 
		
		if response == false || reply.Code == 0{
			// sleep 一下
			time.Sleep(time.Second)
			// 开始下一侧 request
			continue
		}

		// response == true
		if(reply.Code == -1){
			// All task have finished, exit the worker process
			break;
		}

		// Check if it is Map or Reduce Task
		if reply.WorkType == 1 {
			// Map Task
			MapTask(workerId, &reply, mapf)
		}else if reply.WorkType == 2{
			// Reduce Task
			ReduceTask(workerId, &reply, reducef)
		}
	}
}

func MapTask(workerId int, reply *WorkReply, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	filename := reply.Filename
	fmt.Printf("workerId: %d\n", workerId)
	fmt.Printf("file: %v\n", filename)


	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		key := intermediate[i].Key
		fmt.Println("key: "+key)
		fmt.Printf("hash: %d\n ", ihash(key))
		fmt.Printf("nReduce: %d\n ", reply.nReduce)
		reduceNo := ihash(key) % 10
		oname := intermediateFileName(reduceNo)
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(ofile)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		for k := i; k<j; k++{
			enc.Encode(intermediate[k])
		}
		ofile.Close()
		i = j
	}
	// 写完了
	noticeArg := NoticeCoorninatorArg{}
	noticeArg.Filename = reply.Filename
	noticeArg.WorkerId = workerId
	noticeArg.WorkType = 0

	noticeReply := NoticeCoorninatorReply{}

	call("Coordinator.TaskFinish", noticeArg, noticeReply)
}

func ReduceTask(workerId int, reply *WorkReply, reducef func(string, []string) string) {
	intermediateFileName := reply.Filename
	ifile, _ := os.OpenFile(intermediateFileName, os.O_RDONLY, 0755)
	intermediate := []KeyValue{}

	dec := json.NewDecoder(ifile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	sort.Sort(ByKey(intermediate))
	
	oname := intermediateFileName + "-out"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	ofile.Close()
	// 写完了
	noticeArg := NoticeCoorninatorArg{}
	noticeArg.Filename = reply.Filename
	noticeArg.WorkerId = workerId
	noticeArg.WorkType = 1

	noticeReply := NoticeCoorninatorReply{}

	call("Coordinator.TaskFinish", noticeArg, noticeReply)
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
