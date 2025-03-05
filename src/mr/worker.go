package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


// Map 函数返回 KeyValue 的片段。
type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 为 Map 输出的每个 KeyValue 选择 reduce 任务编号。
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


// main/mrworker.go 调用该函数。
// 接受两个参数，分别是mapf函数和reducef函数
// mapf(string string)[]KeyValue，mapf接收string、string类型形参，并返回一个KeyValue结构体数组
// reducef(string []string) string 接收string、[]string类型，返回最终生成结果string。  其中string是key，而[]string是该key对应的所有value

func Worker(mapf func(string, string) []KeyValue,reducef func(string, []string) string) {	
	// Worker 实现。
	// 取消注释，向协调器发送示例 RPC。
	CallExample()

}

/*
示例函数展示如何向协调器发出 RPC 调用。RPC 参数和回复类型在 rpc.go 中定义。
*/
func CallExample() {

	// 声明参数结构。
	args := ExampleArgs{}

	// 填写参数。
	args.X = 99

	// 声明回复结构。
	reply := ExampleReply{}

	// 发送 RPC 请求，等待回复。“Coordinator.Example ”告诉接收服务器，我们要调用 struct Coordinator 的 Example() 方法。
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// 向协调器发送 RPC 请求，等待响应。
// 通常返回 true。
// 如果出错，则返回 false。
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
