package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

}

// 您在此处的代码 -- 供 Worker 调用的 RPC 处理程序。

//
// RPC 处理程序示例。
//
// RPC 参数和回复类型在 rpc.go 中定义。
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// 启动一个线程，监听来自 worker.go 的 RPCs
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

// main/mrcoordinator.go 会定期调用 Done() 来确定整个工作是否已完成。
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// 创建一个协调员。
// main/mrcoordinator.go 调用此函数。
// nReduce 是要使用的还原任务数。
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
