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

// 在此添加 RPC 定义。


// 在 /var/tmp 中为协调器创建一个唯一的 UNIX 域套接字名称。
// 在 /var/tmp 目录中，为协调器命名。
// 不能使用当前目录，因为
// Athena AFS 不支持 UNIX 域套接字。
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
