package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "time"
import "sync/atomic"

const (
	UpConfigLoopInterval = 100 * time.Millisecond 

	GetTimeout          = 500 * time.Millisecond
	AppOrPutTimeout     = 500 * time.Millisecond
	UpConfigTimeout     = 500 * time.Millisecond
	AddShardsTimeout    = 500 * time.Millisecond
	RemoveShardsTimeout = 500 * time.Millisecond
)


type Shard struct{
	// 该分片的存储的数据
	KVMap map[string]string
	// 使用配置号作为时间戳，可以防止旧的数据覆盖新的数据
	ConfigNum int 
}

type Op struct {
	Key 	string 
	Value 	string
	OpType 	string
	ClientId int64
	SeqId	int
	UpConfig shardctrler.Config 	// 携带最新的分片配置
	ShardId int  // 分片编号，用来区分分片
	Shard Shard 	// 分片的具体数据
	SeqMap map[int64]int
}

type OpReply struct{
	ClientId int64
	SeqId	int
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32

	notifyChan map[int]chan Op
	SeqMap map[int64]int

	Config shardctrler.Config // 最新配置
	LastConfig shardctrler.Config // 倒数第二个配置

	shardPersist []Shard 	// 存储Shard数据

	sck		*ShardCtrler.Clerk 	// 用来与Shard Controller进行通信，可以理解为Shard Controller的客户端

	persister *raft.Persister // 判断当前是否需要快照
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] 包含了该组中所有服务器的端口号。
//
// me 是当前服务器在 servers[] 中的索引。
//
// k/v 服务器应该通过底层的 Raft 实现来存储快照，
// 它应该调用 persister.SaveStateAndSnapshot() 来原子地保存 Raft 状态以及快照。
//
// k/v 服务器应当在 Raft 保存的状态超过 maxraftstate 字节时进行快照，
// 以便 Raft 能够垃圾回收日志。如果 maxraftstate 为 -1，则无需进行快照。
//
// gid 是该服务器所在group的 GID，用于与 shardctrler 进行交互。
//
// 将 ctrlers[] 传递给 shardctrler.MakeClerk()， 这样你就可以向 shardctrler 发送 RPC 请求。
//
// make_end(servername) 将服务器名称从 Config.Groups[gid][i] 转换为 labrpc.ClientEnd，
// 你可以通过它发送 RPC 请求。你需要这个来向其他组发送 RPC 请求。
//
// 查看 client.go 文件，了解如何使用 ctrlers[] 和 make_end() 来向拥有特定分片的组发送 RPC 请求。
//
// StartServer() 必须快速返回，因此它应该启动 goroutine 来处理任何长时间运行的工作。

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.shardsPersist = make([]Shard, shardctrler.NShards)
	kv.notifyChan = make(map[int]chan OpReply)
	kv.SeqMap = make(map[int64]int)
	kv.sck = shardctrler.MakeClerk(kv.masters)

	// 快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}

	go kv.applyMsgHandlerLoop()

	return kv
}

//====================== 后台协程 ======================

// 处理ApplyCh中的ApplyMsg
func(kv *ShardKV) applyMsgHandlerLoop(){
	for{
		if kv.killed(){
			return 
		}
		select{
		case:msg <- kv.applyCh:
			if msg.CommandValid{
				kv.mu.Lock()
				op := msga.CommandValid.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}
				// 处理Put、Append、Get 
				if op.OpType == PutType || op.OpType == AppendType || op.OpType == GetType{
					shardId := key2shard(op.Key)
					if kv.Config.Shards[shardId] != kv.gid{
						reply.Err = ErrWrongGroup
					}else if kv.shardPersist[shardId].KVMap== nil{
						reply.Err = ShardNotArrived
					}else{
						if !kv.isRepeat(op.ClientId,op.SeqId){
							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardPersist[shardId].KVMap[op.Key] = op.Value
							case AppendType:
								kv.shardPersist[shardId].KVMap[op.Key] += op.Value
							case GetType:
								
							}
						}
					}
				}else{
					// 处理其他RPC
					switch op.OpType {
					case UpConfigType:
						// 配置更新
						kv.upConfigHandler(op)
					case AddShardType:
						// 对于分片发送，我们使用op.SeqId来存储请求序号，其数值一般为配置版本号
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						kv.removeShardHandler(op)
					}
				}
				// 进行快照压缩
				if kv.maxraftstate!=-1 && kv.persister.RaftStateSize() > kv.maxraftstate{
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				// 将结果压到通道里面
				kv.getWaitCh(msg.CommandIndex) <- reply
				kv.mu.Unlock()
			}else if msg.SnapshotValid{
				kv.mu.Lock()
				// 读取快照中的数据
				kv.ReadSnapShot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// ====================== RPC 部分 ======================
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 获取所在分片id
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid{
		// 这个分片不属于我们group
		reply.Err = ErrWrongGroup
	}else if kv.shardPersist[shardId].KVMap == nil{
		// 这个分片的数据还未到达
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived{
		return 
	}
	command := Op{
		OnType:GetType,
		ClientId:args.ClientId,
		SeqId:args.SeqId,
		Key:args.Key,
	}
	// 写入Raft
	err := kv.startCommand(command,GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	// 再次加锁检查分片归属和数据是否到达，防止在等待共识期间配置发生变化
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid{
		reply.Err = ErrWrongGroup
	}else if kv.shardPersist[shardId].KVMap == nil{
		reply.Err = ShardNotArrived
	}else{
		reply.Err = OK
		reply.Value = kv.shardPersist[shardId].KVMap[args.Key]
	}
	kv.mu.Unlock()
	return 
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 获取所在分片id
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid{
		reply.Err = ErrWrongGroup
	}else if kv.shardPersist[shardId].KVMap== nil{
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived{
		return 
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	// 写入Raft
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}

// 各个group之间的RPC，用来传输分片
func (kv *ShardKV) AddShard(args* SendShardArg,reply *AddShardReply){
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,	// 请求序号（通常是配置版本号）
		ShardId:  args.ShardId,		
		Shard:    args.Shard,		// 分片数据
		SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
}

//================= 持久化快照部分 =================

// 将快照中的数据持久化
func (kv *ShardKV) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)


	e.Encode(kv.Config)
	e.Encode(kv.SeqMap)
	e.Encode(kv.shardPersist)
	e.Encode(kv.LastConfig)
	//e.Encode(kv.maxraftstate)
	data := w.Bytes()
	return data
}

// 读取快照中的数据
func (kv *ShardKV) ReadSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist []Shard
	var SeqMap map[int64]int
	//var MaxRaftState int
	var Config shardctrler.Config
	var LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) == nil && d.Decode(&SeqMap) == nil {
		kv.shardPersist = shardsPersist
		kv.SeqMap = SeqMap
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}

// ================= lib Functions =================

// 更新配置
func (kv *ShardKV) upConfigHandler(op Op){
	curConfig := kv.Config
	upConfig := op.UpConfig
	// 我的配置较新，因此不需要更新
	if curConfig.Num >= upConfig.Num{
		return 
	}
	// 更新
	for shard,gid := range curConfig.Shards{
		
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardPersist[shard] = upConfig.Shards[shard]
			kv.shardPersist[shard].ConfigNum = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
}

// 写入Raft
func (kv *ShardKV) startCommand(command Op,timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index,_,isLeader := kv.rf.Start(command)
	if !isLeader{
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	// 获取通道,并从该通道获取对应执行结果
	ch := getWaitCh(index)
	kv.mu.Unlock()

	// 定时器
	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select{
	case:res <- ch:
		kv.mu.Lock()
		delete(kv.notifyChan,index)
		if res.SeqId != command.SeqId || res.ClientId != command.ClientId{
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return res.Err
	case:<-timer.C:
		return ErrOverTime
	}
}

// 获取通道
func (kv *ShardKV) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _,ok := kv.notifyChan[index];!ok{
		kv.notifyChan[index] = make(chan OpReply,1)
	}
	return kv.notifyChan[index]
}

// 判断请求是否重复
func (kv *ShardKV) isRepeat(clientId int64,seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId,exist := kv.seqMap[clientId]
	if !exist{
		return false
	}
	return seqId<=lastSeqId
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
