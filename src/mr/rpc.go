package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"
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

// 定义任务类型
type TaskType int

const (
    MapTask TaskType = iota   // 0：表示 Map 任务
    ReduceTask                // 1：表示 Reduce 任务
    NoTask                    // 2：表示当前没有可执行的任务
    DoneTask                  // 3：表示任务已经完成
)

// 任务状态枚举
type TaskStatus int

const (
	Idle TaskStatus = iota	// 0:表示还没有被执行
	InProgress				// 1:表示任务正在被执行
	Completed				// 2:表示任务已经完成
)

// 任务结构体定义
type Task struct{
	Type TaskType		// 任务枚举类型
	ID int				// 任务ID
	InputFile string	// 输入文件
	Status	TaskStatus	// 当前状态
	StartTime time.Time	// 开始时间，用来判断是否超时
	
}

// RPC定义

// Worker向Master发送任务请求，来获取要执行的任务的信息
type RequestArgs struct{}

// Master向Worker回复任务信息
type RequestReply struct{
	TaskType TaskType	// 任务枚举类型
	TaskID	int			// 任务ID
	InputFile string	// 输入文件
	NReduce    int		// Reduce数量
    NMap       int		// map数量
}

// Worker向Master发送任务完成报告
type ReportArgs struct{
	TaskType TaskType	// 任务类型
	TaskID	int			// 任务ID
}

// 报告的回复信息
type ReportReply struct{}



// 不用管
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
