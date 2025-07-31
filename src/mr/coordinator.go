package mr

import (
    "log"
    "net"
    "net/http"
    "net/rpc"
    "sync"
    "time"
	"os"
)

// MapReduce阶段枚举
type Phase int

const (
	MapPhase Phase = iota		// 处于Map阶段
	ReducePhase					// 处于Reduce阶段
	DonePhase					// 处于完成阶段 
)



// 任务管理结构体定义
type Coordinator struct {
	mapTasks []*Task			// map任务队列
	redTasks []*Task			// reduce任务队列
	mapChan	chan* Task		// map管道，用于分发map任务
	redChan	chan* Task		// reduce管道，用于分发reduce任务
	phase   Phase			// mapReduce当前的阶段
	nReduce int				// Reduce数量
	mu		sync.Mutex		// 锁
}

// 是否已经完成了所有的任务
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

// 初始化相关信息
// 函数参数：文件数组、Reduce任务
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:	make([]*Task,len(files)),
		redTasks:	make([]*Task,nReduce),
		mapChan:	make(chan*Task,len(files)),		// 由于管道最多可能存储所有的任务，因此将长度设置的和mapTask一样
		redChan:	make(chan*Task,nReduce),
		nReduce:	nReduce,
		phase:		MapPhase,		//刚开始，肯定是MapPhase阶段		
	}

	// 初始化mapTasks
	for i,file :=range files{
		c.mapTasks[i] = &Task{
			Type:		MapTask,
			ID:			i,
			InputFile: 	file,
			Status:		Idle,
		}
		// 将map任务放入管道
		c.mapChan <- c.mapTasks[i]
	}


	// 初始化redTasks
	for i:=0;i<nReduce;i++{
		c.redTasks[i] = &Task{
			Type:		ReduceTask,
			ID:			i,
			Status:		Idle,
		}
	}
	// 使用一个独立的协程来进行超时检查
	go c.checkTimeouts()
	c.server()
	return &c
}

// (c *Coordinator) 表示这是结构体Coordinator的一个方法
func (c *Coordinator) checkTimeouts(){
	for{
		// 每隔1秒执行一次检查
		time.Sleep(time.Second)		
		c.mu.Lock()
		now := time.Now()
		switch c.phase{
		case MapPhase:
			for _,task := range c.mapTasks{
				// now.Sub(task.StartTime)表示现在的时间-运行开始时间
				if task.Status == InProgress && now.Sub(task.StartTime)>10*time.Second{
					task.Status=Idle
					c.mapChan <- task
				}
			}
		case ReducePhase:
			for _,task := range c.redTasks{
				if task.Status == InProgress && now.Sub(task.StartTime)>10*time.Second{
					task.Status=Idle
					c.redChan <- task
				}
			}
		}
		c.mu.Unlock()
	}
}

// Worker 向 Coordinator 发送任务请求时，会调用 RequestTask。
func (c* Coordinator) RequestTask(args *RequestArgs,reply *RequestReply) error{
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase==DonePhase{
		reply.TaskType =DoneTask
		return nil
	}

	var task *Task
	if c.phase == MapPhase{
		// 非阻塞的从管道中读取数据，如果没有，则设置为NoTask
		select {
		case task = <-c.mapChan:
		default:
			reply.TaskType=NoTask
			return nil
		}
	}else {
        select {
        case task = <-c.redChan:
        default:
            reply.TaskType = NoTask
            return nil
        }
    }

	task.Status = InProgress
	task.StartTime = time.Now()

	reply.TaskType = task.Type
	reply.TaskID = task.ID
	reply.InputFile = task.InputFile
	reply.NReduce = c.nReduce
	if task.Type == ReduceTask{
		reply.NMap = len(c.mapTasks)
	}
	return nil
}

// Worker 任务完成后，会调用 ReportTaskDone 通知 Coordinator 任务已完成。
func (c *Coordinator) ReportTask(args *ReportArgs,reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []*Task
	// 找到对应的任务集合
	if args.TaskType == MapTask{
		tasks = c.mapTasks
	}else{
		tasks= c.redTasks
	}
	// 检查 TaskID 是否有效
	if args.TaskID<0 || args.TaskID >=len(tasks){
		return nil
	}
	task := tasks[args.TaskID]
	// 检查任务状态
	if task.Status == InProgress{
		task.Status=Completed
		allDone :=true
		// 检查所有任务是否完成
		if(args.TaskType == MapTask){
			for _,t :=range c.mapTasks{
				if t.Status != Completed{
					allDone= false
					break
				}
			}
			// 如果所有的map任务都已经完成，则进入reducePhase阶段
			if allDone{
				c.phase=ReducePhase
				for _,rt := range c.redTasks{
					rt.Status =Idle
					// 所有的reduce任务加入red管道中
					c.redChan <- rt
				}
			}
		} else {
			for _,t :=range c.redTasks{
				if t.Status !=Completed{
					allDone=false
					break
				}
			}
			if allDone{
				c.phase =DonePhase
			}
		}
	}
	return nil


}

// 示例
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// 启动一个线程，从worker.go中监听rpc
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
