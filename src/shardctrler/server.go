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
	notifyChan map[int]chan Op
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
        newConfig.Groups[gid] = servers
    }
    // 添加新的 Groups
    for gid, servers := range join_Servers {
        newConfig.Groups[gid] = servers
    }

	// 负载均衡
	newConfig.Shards=sc.rebalanceShards(newConfig.Groups,newConfig.Shards)
	sc.configs = append(sc.configs, newConfig)
}

// 执行Leave操作，op.GIDS[] int是要删除的group的编号
func (sc *ShardCtrler) Leave_op(op *Op){
	// 获取旧的配置，然后复制的时候，跳过该group，对于该组的分片，直接将该组的分片则置为别的
	oldConfig := sc.configs[len(sc.configs)-1]

	removedGids := op.GIDs
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
			newConfig.Groups[gid] = servers
		}
	}
	if len(newConfig.Groups) == 0 {
		for shard, _ := range newConfig.Shards {
			newConfig.Shards[shard] = 0
		}
		// 将新配置添加到配置列表
		sc.configs = append(sc.configs, newConfig)
		return
	}
	// 负载均衡
	newConfig.Shards=sc.rebalanceShards(newConfig.Groups,newConfig.Shards)
	// 将新配置添加到配置列表
	sc.configs = append(sc.configs, newConfig)
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
		op.Cig = sc.configs[len(sc.configs)-1]
		return 
	}
	op.Cig = sc.configs[op.Num]
}

func (sc *ShardCtrler) rebalanceShards(groups map[int][]string, shards [NShards]int) [NShards]int {
	/*
	总体思路：
		1. 先把group按照id从小到大排序，因为我们限制将多余的shard分配给id小的group。
		2. 使用一个map来记录当前各个group已经分配了多少shard。即counts(gid---->shard数目)
		3. 计算每个group处理shard的平均值，即shards/groups
		4. 计算多余的shard，即shards%groups
		5. 使用一个map来存储应该各个group应该分配的shard数目，即quote(gid---->目标shard数目)
		6. 开始移动shard
			6.1 遍历shards,
			6.2 如果shards的gid=0,则需要分配  or  quote[gid]==0,则代表该shards的当前group已经满了，需要分配，加入toMove切片
			6.3 如果不需要移动，则quote[gid]--，
		7. 开始分配
			7.1 将需要分配的切片，分配给还有剩余的group，即quote[gid]>0的group
	*/ 
	if len(groups) == 0 {
		return [NShards]int{}
	}

	groupIDs := make([]int, 0, len(groups))
	for gid := range groups {
		groupIDs = append(groupIDs, gid)
	}
	
	sort.Ints(groupIDs)

	// 统计每个 group 拥有的 shard
	counts := make(map[int]int)
	for _, gid := range shards {
		if gid != 0 {
			counts[gid]++
		}
	}

	// 计算目标数量
	target := NShards / len(groups)
	extra := NShards % len(groups)

	// 每个group要分配的目标分片数量
	// 多余的分片优先分配给 GID 小的 group
	quota := make(map[int]int)
	for _, gid := range groupIDs {
		quota[gid] = target
		if extra > 0 {
			quota[gid]++
			extra--
		}
	}

	// 遍历shards(shard-->gid)，寻找需要移动的分片
	toMove := []int{}
	for shard, gid := range shards {
		// 如果shards[shard]=0,则代表这个分片还没被分配
		// 如果quota[gid]=0，则代表这个分片当前所在的group已经满了，因此需要分配
		if gid == 0 || quota[gid] == 0 { 
			toMove = append(toMove, shard)
		} else {
			quota[gid]-- // 这个 shard 保留
		}
	}

	// 把需要移动的 shard 分配给 quota 还有剩余的 group
	for _, shard := range toMove {
		for _, gid := range groupIDs {
			if quota[gid] > 0 {
				shards[shard] = gid
				quota[gid]--
				break
			}
		}
	}

	return shards
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
