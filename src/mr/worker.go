package mr

import (
    "encoding/json"
    "fmt"
    "log"
    "os"
    "sort"
    "time"
	"hash/fnv"
	"net/rpc"
)
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


// 发送rpc请求给Coordinator，然后根据任务类型执行对应的任务
func Worker(mapf func(string, string) []KeyValue,reducef func(string, []string) string) {
	for{
		args := RequestArgs{}
		reply := RequestReply{}
		ok := call("Coordinator.RequestTask",&args,&reply)
		if !ok {
			return 
		}

		switch reply.TaskType{
		case MapTask:
			// 执行map任务
			doMapTask(mapf,reply.TaskID,reply.InputFile,reply.NReduce)
			// 发送报告
			reportDone(MapTask,reply.TaskID)
		case ReduceTask:
            doReduceTask(reducef,reply.TaskID, reply.NMap, reply.NReduce)
            reportDone(ReduceTask, reply.TaskID)
        case NoTask:
            time.Sleep(time.Second)
        case DoneTask:
            return
		}
		
	}
}

// map
// 参数：map函数、map任务ID、文件名、nReduce
func doMapTask(mapf func(string,string)[] KeyValue,taskID int,file string,nReduce int){
	// 读取文件
	content,err := os.ReadFile(file)
	if err!=nil{
		log.Printf("Map task %d error: %v", taskID, err)
		return 
	}
	// 调用map函数，生成中间键值对
	kvs:=mapf(file,string(content))

	// 用来存储中间键值对和nReduce的对应关系
	// 切片，每个元素都是 []KeyValue（即 KeyValue 切片）。
	partitions := make([][]KeyValue, nReduce)

	// 利用哈希函数，将中间键值对放到不同的“桶”中
	for _,kv :=range kvs{
		idx:=ihash(kv.Key)%nReduce
		partitions[idx]=append(partitions[idx],kv)
	}
	// 生成中间文件
	// 一个合理的中间文件命名规范是`mr-X-Y`，其中`X`是Map任务编号，`Y`是Reduce桶编号。

	// 遍历切片，读取每个nReduce的所有kvs
	for idx,kvs:= range partitions{
		// 创造一个临时文件，文件名为mr-mapID-nReduceID
		tmpFile,err:=os.CreateTemp("",fmt.Sprintf("mr-%d-%d-*",taskID,idx))
		if err!=nil{
			log.Printf("Create temp file error: %v", err)
			continue
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range kvs {
    		enc.Encode(&kv)
		}
		tmpFile.Close()
        os.Rename(tmpFile.Name(), fmt.Sprintf("mr-%d-%d", taskID, idx))
	}
}

// reducef 任务、reduceID、map数量、reduce数量
func doReduceTask(reducef func(string, []string) string,taskID int,nMap int,nReduce int){
	//创建一个map，用于存储 键（string）与多个值（[]string） 的映射关系。
	kvs:=make(map[string][]string)

	// 读取文件，很明显，中间文件的首个编号，是map任务id
	for i:=0;i<nMap;i++{
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, taskID))
        if err != nil {
            continue
        }
		dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break
            }
			// 将相同 Key 的 Value 归并到一个切片中
            kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
        }
        file.Close()
	}
	// 将kvs中的所有键值存入keys中，为了后续进行排序
	keys:=make([]string,0,len(kvs))
	for k:= range kvs{
		keys= append(keys,k)
	}
	// 对key进行排序
	sort.Strings(keys)

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-*", taskID))
    if err != nil {
        log.Printf("Create out file error: %v", err)
        return
    }

	// 利用reducef将所有的键值对，按照键依次进行处理
	for _, key := range keys {
        output := reducef(key, kvs[key])
        fmt.Fprintf(tmpFile, "%v %v\n", key, output)
    }
	tmpFile.Close()
    os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", taskID))
}

// 向coordinator发送报告
func reportDone(taskType TaskType,taskID int){
	args := ReportArgs{TaskType:taskType,TaskID:taskID}
	reply := ReportReply{}

	call("Coordinator.ReportTask",&args,&reply)

}











// 样例，不用管
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

//
// 向协调器发送RPC请求，等待响应。通常返回false
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
