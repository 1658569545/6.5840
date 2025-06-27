package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	//"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	"unsafe"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 操作类型
	CmdType int8
	ClientId int64
	SeqNum int64
	Key   string
	Value string
}

// 命令的执行结果
type OpReply struct{
	CmdType int8
	ClientId int64
	SeqNum int64
	Err string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// kv数据库
	kvDB map[string]string
	// 客户端map，客户端id和客户端的最后请求，来判断是否重复
	clientLastSeq map[int64]int64
	// 客户端的等待通道，获取执行结果
	waitChannels map[int64]chan OpReply

	persister *raft.Persister
	currentBytes int
}

// 从管道读取客户端命令的执行结果
func (kv *KVServer) waitCmd(cmd Op) OpReply{
	kv.mu.Lock()
	// 创建一个通道
	ch := make(chan OpReply, 1)
	// 存入map中
	kv.waitChannels[cmd.ClientId] = ch
	kv.mu.Unlock()
	// 等待结果
	select {
	case res:= <- ch:
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SeqNum != cmd.SeqNum {
			res.Err = ErrCmd
		} 
		kv.mu.Lock()
		delete (kv.waitChannels,cmd.ClientId)
		kv.mu.Unlock()
		return res
	case <- time.After(100*time.Millisecond):
		kv.mu.Lock()
		delete (kv.waitChannels,cmd.ClientId)
		kv.mu.Unlock()
		res:=OpReply{
			Err: ErrTimeout,
		}
		return res
	}
}

// 处理客户端的Get请求
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// 判断是否重复
	if args.SeqNum <= kv.clientLastSeq[args.ClientId]{
		reply.Value=kv.kvDB[args.Key]
		reply.Err=OK
		kv.mu.Unlock()
		return 
	}
	kv.mu.Unlock()
	cmd := Op{
		CmdType:GetCmd,
		ClientId:args.ClientId,
		SeqNum:args.SeqNum,
		Key:args.Key,
	}
	// 写入日志
	_,_,isLeader := kv.rf.Start(cmd)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return 
	}
	// 获取结果
	res := kv.waitCmd(cmd)
	reply.Err, reply.Value = res.Err, res.Value

}

// 处理客户端的Put和Appendeq请求
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if args.SeqNum <= kv.clientLastSeq[args.ClientId]{
		reply.Err=OK
		kv.mu.Unlock()
		return 
	}
	kv.mu.Unlock()
	cmd := Op{
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
		Key:args.Key,
		Value:args.Value,
	}
	if args.Op=="Put"{
		cmd.CmdType=PutCmd
	}else if args.Op == "Append"{
		cmd.CmdType=AppendCmd
	}else{
		return 
	}
	// 写入Raft日志
	_,_,isLeader := kv.rf.Start(cmd)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return 
	}
	// 获取结果
	res := kv.waitCmd(cmd)
	reply.Err = res.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isRepeated(clientId, sequenceNum int64) bool {
	seq, ok := kv.clientLastSeq[clientId]
	if ok && seq >= sequenceNum {
		return true
	}
	return false
}

func (kv *KVServer) getSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientLastSeq)
	e.Encode(kv.kvDB)
	snapshot := w.Bytes()
	return snapshot
}

// should be in lock
func (kv *KVServer) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	labgob.NewDecoder(r)
	
}

// ========================== go routines ============================
// 后台运行的一个协程，用来具体执行Put、Appende、Get等操作
func (kv *KVServer) enginerStart(){
	for !kv.killed(){
		// 接收来自applyCh中的消息
		msg:= <-kv.applyCh
		if msg.CommandValid{
			op:=msg.Command.(Op)
			clientId := op.ClientId
			kv.mu.Lock()
			kv.currentBytes += int(unsafe.Sizeof(op)) + len(op.Key) + len(op.Value)
			// 重复操作，直接跳过就行了，重复操作已经在Get和PutAppend中处理过了
			if op.SeqNum < kv.clientLastSeq[clientId]{
				kv.mu.Unlock()
				continue
			}
			if op.CmdType == PutCmd || op.CmdType == AppendCmd {
				if !kv.isRepeated(clientId, op.SeqNum) {
					if op.CmdType == PutCmd {
						kv.kvDB[op.Key] = op.Value
					} else if op.CmdType == AppendCmd {
						kv.kvDB[op.Key] += op.Value
					}
				}
			}
			if op.SeqNum > kv.clientLastSeq[clientId] {
				kv.clientLastSeq[clientId] = op.SeqNum
			}
			//kv.clientLastSeq[clientId] = op.SeqNum
			// 如果有管道就代表那边已经在等待结果了
			if _,ok := kv.waitChannels[clientId];ok{
				res:=OpReply{
					CmdType:op.CmdType,
					Err:OK,
					ClientId:op.ClientId,
					SeqNum:op.SeqNum,
					Value:kv.kvDB[op.Key],
				}
				kv.waitChannels[clientId]<-res
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.currentBytes > kv.maxraftstate {
				snapshot := kv.getSnapShot()
				kv.currentBytes = 0
				kv.mu.Unlock()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			} else {
				kv.mu.Unlock()
			}
		}else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapShot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}


// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.kvDB =make(map[string]string)
	kv.clientLastSeq = make(map[int64]int64)
	kv.waitChannels =  make(map[int64]chan OpReply)

	kv.persister = persister
	kv.currentBytes = 0
	kv.readSnapShot(kv.persister.ReadSnapshot())

	go kv.enginerStart()
	
	return kv
}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	_,isleader := kv.rf.GetState()
	reply.IsLeader = isleader
}
