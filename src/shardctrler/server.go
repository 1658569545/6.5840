package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "sort"
import "time"
import "sync/atomic"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	

	// Your data here.
	dead    int32 // set by Kill()
	configs []Config // indexed by config num
	// 通知通道，用于通知对应的客户端，请求是否已经执行
	notifyChan map[int]chan Op
	// 记录每个客户端的最大已经处理的请求序列号，防止重复执行
	seqMap	map[int64]int
}

type Op struct {
	// Your data here.
	ClientId int64
	SeqId int
	// Raft的Index
	Index int
	// Join、Leave、Move、Query
	OpType string
	// 要加入的Group  
	Servers map[int][]string	// new GID -> servers mappings

	// 要删除的Group的ID编号
	GIDs []int

	// 将Shard迁移到GID group中
	Shard int 	
	GID int
	
	// 查询第Num个版本的配置信息
	Num int
	// 存储查询结果
	Cig Config
	
	Err Err
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.notifyChan = make(map[int]chan Op)
	sc.seqMap = make(map[int64]int)

	go sc.applyMsgLoop()
	return sc
}

// ================== RPC Function ==================
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sc.killed(){
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	_,isLeader := sc.rf.GetState()
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	op:=Op{
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		OpType: "Join",
		Servers: args.Servers,
	}
	// 将操作写入raft
	lastIndex,_,_ := sc.rf.Start(op)
	op.Index = lastIndex
	// 获取通道
	ch := sc.getNotifyChan(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChan,op.Index)
		sc.mu.Unlock()
	}()
	// 超时定时器
	timer := time.NewTicker(100*time.Millisecond)
	defer timer.Stop()

	// 监测通道
	select{
	case res:= <- ch:
		// 先判断是否重复
		if res.ClientId != op.ClientId || res.SeqId != op.SeqId{
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		}else{
			reply.Err = OK
			reply.WrongLeader = false
			return 
		}
	case <- timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sc.killed(){
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	_,isLeader := sc.rf.GetState()
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	op:=Op{
		OpType:"Leave",
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		GIDs: args.GIDs,
	}
	// 将操作写入raft
	lastIndex,_,_ := sc.rf.Start(op)
	op.Index = lastIndex
	// 获取通道
	ch := sc.getNotifyChan(lastIndex)

	defer func(){
		sc.mu.Lock()
		delete(sc.notifyChan,op.Index)
		sc.mu.Unlock()
	}()
	// 超时定时器
	timer := time.NewTicker(100*time.Millisecond)
	defer timer.Stop()

	// 监测通道
	select{
	case res:= <- ch:
		// 先判断是否重复
		if res.ClientId != op.ClientId || res.SeqId != op.SeqId{
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		}else{
			reply.Err = OK
			reply.WrongLeader = false
			return 
		}
	case <- timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sc.killed(){
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	_,isLeader := sc.rf.GetState()
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	op:=Op{
		OpType:"Move",
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		Shard: args.Shard,
		GID: args.GID,
	}
	// 将操作写入raft
	lastIndex,_,_ := sc.rf.Start(op)
	op.Index = lastIndex
	// 获取通道
	ch := sc.getNotifyChan(lastIndex)

	defer func(){
		sc.mu.Lock()
		delete(sc.notifyChan,op.Index)
		sc.mu.Unlock()
	}()
	// 超时定时器
	timer := time.NewTicker(100*time.Millisecond)
	defer timer.Stop()

	// 监测通道
	select{
	case res:= <- ch:
		// 先判断是否重复
		if res.ClientId != op.ClientId || res.SeqId != op.SeqId{
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		}else{
			reply.Err = OK
			reply.WrongLeader = false
			return 
		}
	case <- timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sc.killed(){
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	_,isLeader := sc.rf.GetState()
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return 
	}
	op:=Op{
		OpType:"Query",
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		Num: args.Num,
	}
	// 将操作写入raft
	lastIndex,_,_ := sc.rf.Start(op)
	op.Index = lastIndex
	// 获取通道
	ch := sc.getNotifyChan(lastIndex)

	defer func(){
		sc.mu.Lock()
		delete(sc.notifyChan,op.Index)
		sc.mu.Unlock()
	}()
	// 超时定时器
	timer := time.NewTicker(100*time.Millisecond)
	defer timer.Stop()

	// 监测通道
	select{
	case res:= <- ch:
		// 先判断是否重复
		if res.ClientId != op.ClientId || res.SeqId != op.SeqId{
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		}else{
			reply.Err = OK
			reply.WrongLeader = false
			reply.Config = res.Cig
			return 
		}
	case <- timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}

}


// ================== Lib Function ==================
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// 判断请求是否重复
func (sc *ShardCtrler) isRepeat(clientId int64,seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastIndex,exist := sc.seqMap[clientId]
	if !exist{
		return false
	}
	return seqId<=lastIndex
}

// 获取index对应的通道
func (sc *ShardCtrler) getNotifyChan(index int) chan Op{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _,exist := sc.notifyChan[index];!exist{
		sc.notifyChan[index] = make(chan Op,1)
	}
	return sc.notifyChan[index]
}

// 执行Join操作
func (sc *ShardCtrler) Join_op(op *Op) {
    join_Servers := op.Servers
    oldConfig := sc.configs[len(sc.configs)-1]
    newConfig := Config{
        Num:    oldConfig.Num + 1,
        Shards: oldConfig.Shards,
        Groups: make(map[int][]string),
    }
    // 复制旧的 Groups
    for gid, servers := range oldConfig.Groups {
        copyServers := make([]string, len(servers))
        copy(copyServers, servers)
        newConfig.Groups[gid] = copyServers
    }

    // 添加新的 Groups
    for gid, servers := range join_Servers {
        newConfig.Groups[gid] = servers
    }

    // 将新配置追加到配置列表
    sc.configs = append(sc.configs, newConfig)

    // 重新分配分片
    sc.rebalance()
}

// 执行Leave操作
func (sc *ShardCtrler) Leave_op(op *Op){
	// op.GIDS[] int是要删除的group的编号
	// 获取旧的配置，然后复制的时候，跳过该group，对于该组的分片，直接将该组的分片则置为别的
	removedGids := op.GIDs
	oldConfig := sc.configs[len(sc.configs)-1]
	// 转化成Map，用来快速查找
	removedGidMap := make(map[int]bool)
	// 将要删除的gid设置为true
	for _, gid := range removedGids{
		removedGidMap[gid] = true
	}
	// 创建新配置，直接跳过gid = true的
	newConfig := Config{
		Num:oldConfig.Num+1,
		Shards:oldConfig.Shards,
		Groups:make(map[int][]string),
	}
	// 复制Group
	for gid,servers := range oldConfig.Groups{
		if !removedGidMap[gid]{
			copyServers:= make([]string,len(servers))
			copy(copyServers,servers)
			newConfig.Groups[gid]=copyServers
		}
	}
	sc.configs = append(sc.configs, newConfig)
	sc.rebalance()
}
// 执行Move操作
func (sc *ShardCtrler) Move_op(op *Op){
	aimshard := op.Shard
	aimgid := op.GID
	// 先获取当前的配置
	oldConfig := sc.configs[len(sc.configs)-1]
	if aimgid == oldConfig.Shards[aimshard]{
		return
	}
	// 创建新配置
	newConfig := Config{
		Num:oldConfig.Num+1,
		Shards:oldConfig.Shards,
		Groups:make(map[int][]string),
	}
	// 复制
	for gid,servers := range oldConfig.Groups{
		copyServers:= make([]string,len(servers))
		copy(copyServers,servers)
		newConfig.Groups[gid]=copyServers
	}
	//迁移，先判断gid是否存在，再迁移
	if _,exist := newConfig.Groups[aimgid];!exist{
		return
	}
	newConfig.Shards[aimshard] = aimgid
	// 追加新配置
    sc.configs = append(sc.configs, newConfig)
}
// 执行Query操作
func (sc *ShardCtrler) Query_op(op *Op){
	num := len(sc.configs)
	if op.Num == -1  || op.Num >= num{
		op.Cig = sc.configs[num-1]
	}
	op.Cig = sc.configs[op.Num]
}

//Join和Leave之后需要进行负载均衡
func (sc *ShardCtrler) rebalance() {
    // 获取最新的配置
    lastConfig := sc.configs[len(sc.configs)-1]
    tempShard := lastConfig.Shards
    groups := lastConfig.Groups

    totalShards := len(tempShard)
    totalGroups := len(groups)
    if totalGroups == 0 {
        // 没有 group，全部置为 0
        for i := range tempShard {
            tempShard[i] = 0
        }
        return
    }

    // 1. 统计每个 group 当前拥有的 shard 数
    gid2shards := make(map[int][]int) // gid -> shard indices
    for i, gid := range tempShard {
        if gid != 0 {
            gid2shards[gid] = append(gid2shards[gid], i)
        }
    }

    // 2. 计算目标分配
    everyNum := totalShards / totalGroups
    extraNum := totalShards % totalGroups

    // 3. 按照 gid 排序，保证分配顺序一致
    gids := make([]int, 0, len(groups))
    for gid := range groups {
        gids = append(gids, gid)
    }
    sort.Ints(gids)

    // 4. 生成目标分配表
    target := make(map[int]int) // gid -> 目标分片数
    for i, gid := range gids {
        if i < extraNum {
            target[gid] = everyNum + 1
        } else {
            target[gid] = everyNum
        }
    }

    // 5. 收集多余的 shard
    var toMove []int // 需要重新分配的 shard 索引
    for gid, shards := range gid2shards {
        if len(shards) > target[gid] {
            toMove = append(toMove, shards[target[gid]:]...)
            gid2shards[gid] = shards[:target[gid]]
        }
    }

    // 6. 分配不足的 group
    idx := 0
    for _, gid := range gids {
        need := target[gid] - len(gid2shards[gid])
        for i := 0; i < need; i++ {
            shard := toMove[idx]
            tempShard[shard] = gid
            gid2shards[gid] = append(gid2shards[gid], shard)
            idx++
        }
    }

    // 7. 更新配置
    sc.configs[len(sc.configs)-1].Shards = tempShard
}

// ================== 后台协程 ==================
// 读取ApplyCh中的数据，然后解析，执行
func (sc* ShardCtrler) applyMsgLoop(){
	for{
		if sc.killed(){
			return 
		}
		select{
		case msg:= <-sc.applyCh:
			if msg.CommandValid{
				op:=msg.Command.(Op)
				index:=msg.CommandIndex
				if !sc.isRepeat(op.ClientId,op.SeqId){
					sc.mu.Lock()
					switch op.OpType{
					case "Join":
						// 执行join操作，需要传入引用
						sc.Join_op(&op)
					case "Leave":
						// 执行leave操作
						sc.Leave_op(&op)
					case "Move":
						// 执行move操作
						sc.Move_op(&op)
					case "Query":
						// 执行query操作
						sc.Query_op(&op)
					}
					sc.mu.Unlock()
				}
				// 只有当前的raft是leader才能将结果压入通道，因为只能和leader进行通信
				if _,isLeader := sc.rf.GetState();isLeader{
					sc.getNotifyChan(index)<-op
				}
			}
		}
	}
}
