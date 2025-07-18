package shardkv


import (
	"bytes"
	"sync/atomic"
	"time"
	"sync"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string 
	Value string 
	ClientId int64 
	SequenceNum int64
	CmdType int8 	// Get、Put、Append、
}

type OpReply struct {
	CmdType int8
	ClientId int64
	SequenceNum int64
	Err string
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int

	dead    int32 // set by Kill()

	ctrlers      []*labrpc.ClientEnd 	// 所有shardctrler的RPC连接
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	kv map[string]string 	// 本地的kv存储
	clientSequences map[int64]int64 // 客户端去重表（clientId ----> SeqId）
	waitChannels map[int64]chan OpReply // 用来等待Raft日志的执行结果（log index ----> chan）

	lastConfig shardctrler.Config 	// 要更新的配置的前一个配置
	curConfig shardctrler.Config 	// 要更新的配置

	shardStates map[int]int8 // 每个分片的状态 Serving/Pulling/Offering

	persister *raft.Persister // raft持久化对象
	snapshotIndex int 	// 快照的index

	mck          *shardctrler.Clerk  // 与shardctrler通信的客户端，用于拉取最新配置
}


//---------------------------------------------------- RPC 处理 ----------------------------------------------------
func (kv *ShardKV) waitCmd(index int64, cmd Op) OpReply {
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	kv.waitChannels[index] = ch
	kv.mu.Unlock()
	
	select {
	case res := <-ch:
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SequenceNum != cmd.SequenceNum {
			res.Err = ErrCmd
		}
		kv.mu.Lock()
		delete(kv.waitChannels, index)
		kv.mu.Unlock()
		return res
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitChannels, index)
		kv.mu.Unlock()
		res := OpReply{
			Err: ErrTimeout,
		}
		return res
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	valid,ready := kv.checkShard(args.Key)
	if !valid{
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if !ready{
		reply.Err = ErrShardNotReady
		kv.mu.Unlock()
		return
	}
	// 重复请求
	if args.SequenceNum <= kv.clientSequences[args.ClientId]{
		reply.Err = OK
		reply.Value = kv.kv[args.Key]
		kv.mu.Unlock()
		return
	}
	cmd := Op{
		CmdType:    GetCmd,
		Key:        args.Key,
		ClientId:   args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index,_,isleader := kv.rf.Start(cmd)
	if !isleader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	res := kv.waitCmd(int64(index),cmd)
	reply.Err,reply.Value = res.Err,res.Value 
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	valid,ready := kv.checkShard(args.Key)
	if !valid{
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if !ready{
		reply.Err = ErrShardNotReady
		kv.mu.Unlock()
		return
	}
	// 重复请求
	if args.SequenceNum <= kv.clientSequences[args.ClientId]{
		reply.Err = OK
		
		kv.mu.Unlock()
		return
	}
	cmd := Op{
		Key:        args.Key,
		ClientId:   args.ClientId,
		SequenceNum: args.SequenceNum,
		Value:       args.Value,
	}
	if args.Op == "Put"{
		cmd.CmdType = PutCmd
	}else if args.Op == "Append"{
		cmd.CmdType = AppendCmd
	}else{
		return 
	}
	index,_,isleader := kv.rf.Start(cmd)
	if !isleader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	res := kv.waitCmd(int64(index),cmd)
	reply.Err = res.Err
}

// 接收迁移的分片
func (kv *ShardKV) InstallShard(args* InstallShardArgs, reply *InstallShardReply){
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Num < args.Num {
		// 本地配置落后，不接收分片，因为可能在旧配置中，这些分片还不是我的
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Num > args.Num{
		// 对方发来的配置落后，通知对方删除掉这个分片数据，因为分片迁移已经完成（必须要完成上个配置的迁移之后，才能整体完成迁移）
		kv.mu.Unlock()
		deleteArgs := kv.generateDeleteShardArgs(args)
		go kv.sendDeleteShard(&deleteArgs)
	}
	
	for _,shardId := range args.Shards{
		if kv.shardStates[shardId]!=Pulling{
			reply.Err = OK
			kv.mu.Unlock()
			// 删除数据
			deleteArgs := kv.generateDeleteShardArgs(args)
			go kv.sendDeleteShard(&deleteArgs)
			return
		}
	}
	kv.mu.Unlock()
	// 写入Raft日志
	index, _, isleader := kv.rf.Start(*args)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	kv.waitChannels[int64(index)] = ch
	kv.mu.Unlock()
	select{
	case res:= <- ch:
		if res.Err == OK{
			reply.Err = OK
			deleteArgs := kv.generateDeleteShardArgs(args)
			go kv.sendDeleteShard(&deleteArgs)
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		}else{
			reply.Err = res.Err
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		}
	case <- time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		return 
	}
}

// 删除分片数据
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs,reply* DeleteShardReply){
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Num > args.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	for _, shard := range args.Shards {
		if kv.shardStates[shard] != Offering {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	index, _, isleader := kv.rf.Start(*args)
	if !isleader{
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	kv.waitChannels[int64(index)] = ch
	kv.mu.Unlock()
	select{
	case res:= <- ch:
		if res.Err == OK{
			reply.Err = OK
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		}else{
			reply.Err = res.Err 
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}
}

//---------------------------------------------------- RPC 发送 ----------------------------------------------------
func (kv *ShardKV) sendInstallShard(args InstallShardArgs){
	for _,server := range args.Dest{
		src:= kv.make_end(server)
		var reply InstallShardReply
		ok:= src.Call("ShardKV.InstallShard", &args, &reply)
		if ok&& reply.Err == OK{
			return 
		}
	}
}

func (kv *ShardKV) generateDeleteShardArgs(args* InstallShardArgs) DeleteShardArgs{
	arg:= DeleteShardArgs{
		Num:args.Num,
		Dest:args.Src,
		Src:args.Dest,
		Shards:args.Shards,
		Keys:make([]string,0),
	}
	for k:= range args.Data{
		arg.Keys = append(arg.Keys,k)
	}
	return arg
}

func (kv *ShardKV) sendDeleteShard(args *DeleteShardArgs){
	for _,server := range args.Dest{
		src:= kv.make_end(server)
		var reply DeleteShardReply
		ok:= src.Call("ShardKV.DeleteShard", args, &reply)
		if ok&& reply.Err == OK{
			return 
		}
	}
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

//---------------------------------------------------- lib functions ----------------------------------------------------

func (kv *ShardKV) isLeader() bool {
	return kv.rf.IsLeader()
}

// 等待raft日志的应用结果
func (kv *ShardKV) watiCmd(index int64,cmd Op) OpReply{
	ch := make(chan OpReply,1)
	kv.mu.Lock()
	kv.waitChannels[index] = ch
	kv.mu.Unlock()
	select{
	case res := <-ch:
		// 判断是否合法
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SequenceNum != cmd.SequenceNum {
			res.Err = ErrCmd
		}
		kv.mu.Lock()
		delete(kv.waitChannels,index)
		kv.mu.Unlock()
		return res
	case <-time.After(100 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitChannels,index)
		kv.mu.Unlock()
		res:=OpReply{
			Err: ErrTimeout,
		}
		return res
	}
}

// 检查分片状态
// return (valid, ready)，本组是否拥有、分片是否处于正常状态
func (kv *ShardKV) checkShard(key string) (bool,bool){
	shardId :=key2shard(key)
	if kv.curConfig.Shards[shardId] !=kv.gid{
		return false ,true
	}
	if kv.shardStates[shardId]!=Serving{
		return true,false
	}
	return true,true
}

// 检查请求是否重复
func (kv *ShardKV) isRepeated(clientId,sequenceNum int64) bool{
	seq,ok:=kv.clientSequences[clientId]
	if ok && seq>=sequenceNum{
		return true
	}
	return false
}

// 生成快照
func (kv *ShardKV) getSnapShot()[]byte{
	w := new (bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.kv)
	enc.Encode(kv.clientSequences)
	enc.Encode(kv.shardStates)
	enc.Encode(kv.curConfig)
	enc.Encode(kv.lastConfig)
	snapshot := w.Bytes()
	return snapshot
}

// 读取快照中的数据
func (kv *ShardKV) readSnapShot(data []byte){
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kv)
	d.Decode(&kv.clientSequences)
	d.Decode(&kv.shardStates)
	d.Decode(&kv.curConfig)
	d.Decode(&kv.lastConfig)
	return
}

//---------------------------------------------------- handle部分 ----------------------------------------------------

// ------------ Put/Append/Get处理
func (kv *ShardKV) handleOps (op Op) OpReply{
	clientId := op.ClientId 
	vaild,ready:=kv.checkShard(op.Key)
	if !vaild{
		return OpReply{
			CmdType: op.CmdType,
			ClientId: op.ClientId,
			SequenceNum: op.SequenceNum,
			
			Err : ErrWrongGroup,
		}
	}
	if !ready{
		return OpReply{
			CmdType: op.CmdType,
			ClientId: op.ClientId,
			SequenceNum: op.SequenceNum,
			Err: ErrShardNotReady,
		}
	}
	if kv.isRepeated(op.ClientId,op.SequenceNum){
		return OpReply{
			CmdType: op.CmdType,
			ClientId: op.ClientId,
			SequenceNum: op.SequenceNum,
			Err: OK,
			Value: kv.kv[op.Key],
		}
	}
	if op.CmdType == PutCmd || op.CmdType == AppendCmd {
		if op.CmdType == PutCmd {
			kv.kv[op.Key] = op.Value
		} else if op.CmdType == AppendCmd {
			kv.kv[op.Key] += op.Value
		}
	}
	if op.SequenceNum > kv.clientSequences[clientId] {
		kv.clientSequences[clientId] = op.SequenceNum
	}
	res := OpReply {
		CmdType: op.CmdType,
		ClientId: op.ClientId,
		SequenceNum: op.SequenceNum,
		Err: OK,
		Value: kv.kv[op.Key],
	}
	return res
}

func (kv *ShardKV) allServing(shardIds []int) bool{
	for _,shardId := range shardIds{
		if kv.shardStates[shardId] != Serving{
			return false
		}
	}
	return true
}


// ------------ 分片迁移处理
func (kv *ShardKV) handleInstallShard(args InstallShardArgs)OpReply{
	if args.Num < kv.lastConfig.Num{
		return OpReply{
			Err : OK,
		}
	}
	if args.Num > kv.curConfig.Num{
		return OpReply{
			Err : ErrShardNotReady,
		}
	}
	if kv.allServing(args.Shards){
		return OpReply{
			Err : OK,
		}
	}
	// 分片数据迁移
	for k,v := range args.Data{
		kv.kv[k]=v
	}
	// 客户去重表更新，需要更新全部的，因为是新的配置
	for k,v := range args.ClientSequences{
		if v > kv.clientSequences[k]{
			kv.clientSequences[k] = v
		}
	}
	// 最后需要更新状态
	for _,shardId := range args.Shards{
		kv.shardStates[shardId] = Serving
	}
	return OpReply{
		Err : OK,
	}
}

func (kv *ShardKV)hasServingState(shardIds []int) bool{
	for _,shardId := range shardIds{
		if kv.shardStates[shardId] == Serving{
			return true
		}
	}
	return false
}

// ------------ 分片删除处理
func (kv *ShardKV) handleDeleteShard(args DeleteShardArgs)OpReply{
	if args.Num < kv.lastConfig.Num{
		return OpReply{
			Err : OK,
		}
	}
	if args.Num > kv.curConfig.Num{
		return OpReply{
			Err : ErrShardNotReady,
		}
	}
	if kv.hasServingState(args.Shards){
		// still need these shards
		return OpReply{
			Err : OK,
		}
	}
	for _,k := range args.Keys{
		delete(kv.kv,k)
	}
	// 最后需要更新状态
	for _,shard := range args.Shards{
		kv.shardStates[shard] = Serving
	}
	return OpReply {
		Err: OK,
	}
}

// ------------ 分片状态更新
func (kv *ShardKV) updateShardsState(){
	if kv.lastConfig.Num == 0{
		return 
	}
	for shard,gid := range kv.curConfig.Shards{
		lastGid := kv.lastConfig.Shards[shard]
		if lastGid == kv.gid && gid != kv.gid {
			// 上次属于自己，这次不属于，说明这个分片需要被删除
			kv.shardStates[shard] = Offering
		} else if lastGid != kv.gid && gid == kv.gid {
			// 上次不属于自己，这次属于，说明这个分片需要被安装
			kv.shardStates[shard] = Pulling
		}
	}
}

// ------------ 配置更新处理
func (kv *ShardKV) handleConfig(config shardctrler.Config){
	if config.Num == kv.curConfig.Num+1 && kv.readyForNewSync(){
		kv.lastConfig = kv.curConfig
		kv.curConfig = config
		kv.updateShardsState()
	}
}

// ------------ 执行达成raft共识的日志
func (kv* ShardKV)engineStart(){
	for !kv.killed(){
		msg:= <- kv.applyCh
		if msg.CommandValid{
			kv.mu.Lock()
			var res OpReply
			if _, ok := msg.Command.(Op); ok {
				// Put/Append/Get
				res = kv.handleOps(msg.Command.(Op))
			} else if _, ok := msg.Command.(InstallShardArgs); ok {
				// InstallShard
				res = kv.handleInstallShard(msg.Command.(InstallShardArgs))
			} else if _, ok := msg.Command.(DeleteShardArgs); ok {
				// DeleteShard
				res = kv.handleDeleteShard(msg.Command.(DeleteShardArgs))
			} else if _, ok := msg.Command.(shardctrler.Config); ok {
				// Config handle
				res = OpReply{
					Err: OK,
				}
				kv.handleConfig(msg.Command.(shardctrler.Config))
			} else {
				kv.mu.Unlock()
				continue
			}
			ch ,ok := kv.waitChannels[int64(msg.CommandIndex)]
			if ok{
				ch <- res
				
			}
			kv.snapshotIndex = msg.CommandIndex
			kv.mu.Unlock()
		}else if msg.SnapshotValid{
			kv.mu.Lock()
			kv.readSnapShot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

// ------------ 是否准备好更新配置了
func (kv *ShardKV) readyForNewSync() bool {
	for _,v := range kv.shardStates {
		if v != Serving{
			return false
		}
	}
	return true
}

// 
func (kv *ShardKV) syncConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() || !kv.readyForNewSync() {
			kv.mu.Unlock()
			time.Sleep(UpdateConfigInterval)
			continue
		}
		configNum := kv.curConfig.Num
		kv.mu.Unlock()
		config := kv.mck.Query(configNum + 1)
		if config.Num > configNum {
			kv.rf.Start(config)
		}
		time.Sleep(UpdateConfigInterval)
	}
}

func (kv *ShardKV) needReconfigure() bool {
	for _, v := range kv.shardStates {
		if v == Offering {
			return true
		}
	}
	return false
}

/*
type InstallShardArgs struct{
	Num int 			// 配置号（Config.Num），表示这是第几个配置
	Dest []string 		// 目标group的服务器列表
	Src []string 		// 源group的服务器列表
	Shards []int 		// 要迁移的Shards列表
	ClientSequences map[int64]int64 	// 客户端去重表
	Data map[string]string 			// 分片内的所有k/v数据
}
*/

func (kv *ShardKV) prepareInstallShardArgs() []InstallShardArgs {
	misplacedShards := make(map[int][]int)
	for shard, gid := range kv.curConfig.Shards {
		lastGid := kv.lastConfig.Shards[shard]
		// 该分片上个config在本group，但是在当前config中，不属于本group，因此需要发送给其他group
		if lastGid == kv.gid && gid != kv.gid {
			misplacedShards[gid] = append(misplacedShards[gid], shard)
		}
	}
	args := make([]InstallShardArgs, 0)
	for gid, shards := range misplacedShards {
		shardMap := make(map[int]bool)
		for _, shard := range shards {
			shardMap[shard] = true
		}
		arg := InstallShardArgs {
			Num: kv.curConfig.Num,
			Dest: kv.curConfig.Groups[gid],
			Src: kv.lastConfig.Groups[kv.gid],
			Shards: shards,
			Data: make(map[string]string),
			ClientSequences: int64TableDeepCopy(kv.clientSequences),
		}
		for k, v := range kv.kv {
			if shardMap[key2shard(k)] {
				arg.Data[k] = v
			}
		}
		args = append(args, arg)
	}
	return args
}

func (kv *ShardKV) sendShards() {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() || !kv.needReconfigure() {
			kv.mu.Unlock()
			time.Sleep(InstallShardsInterval)
			continue
		}
		argsArray := kv.prepareInstallShardArgs()
		kv.mu.Unlock()
		for _, args := range argsArray {
			kv.sendInstallShard(args)
		}
		time.Sleep(InstallShardsInterval)
	}
}

//---------------------------------------------------- go routine部分 ----------------------------------------------------

// 检查空任期
func (kv *ShardKV) checkEmptyTerm() {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() {
			kv.mu.Unlock()
			time.Sleep(EmptyTermCheckInterval)
			continue
		}
		if !kv.rf.CheckEmptyTermLog() {
			kv.mu.Unlock()
			kv.rf.Start(EmptyOp{})
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(EmptyTermCheckInterval)
	}
}

// 检查是否需要快照
func (kv *ShardKV) snapshotThread() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			if kv.snapshotIndex == 0 {
				kv.mu.Unlock()
				time.Sleep(SnapshotInterval)
				continue
			}
			snapshot := kv.getSnapShot()
			index := kv.snapshotIndex
			kv.mu.Unlock()
			kv.rf.Snapshot(index, snapshot)
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(SnapshotInterval)
	}
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// 注册结构体类型到 labgob 序列化库，以便 Raft 日志和快照能够正确地序列化（编码）和反序列化（解码）这些类型的数据。
	labgob.Register(Op{})
	labgob.Register(InstallShardArgs{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(EmptyOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// 为当前 ShardKV 服务器创建一个与 shardctrler（分片控制器）通信的客户端 Clerk，并赋值给 kv.mck。
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)	

	kv.kv = make(map[string]string)
	kv.clientSequences = make(map[int64]int64)
	kv.waitChannels = make(map[int64]chan OpReply)
	kv.persister = persister

	kv.lastConfig = shardctrler.Config{}
	kv.curConfig = shardctrler.Config{}
	kv.shardStates = make(map[int]int8)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStates[i] = Serving
	}
	

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.snapshotIndex = 0
	kv.readSnapShot(persister.ReadSnapshot())

	go kv.engineStart()
	go kv.syncConfig()
	go kv.sendShards()
	go kv.checkEmptyTerm()
	go kv.snapshotThread()

	return kv
}
