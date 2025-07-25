package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId int 
	ClientId int64
	Key string
	Value string
	// 操作类型
	OpType string
	// raft传过来的日志索引，一个日志索引对应一个通道
	Index int 
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB map[string]string
	// 通知通道
	notifyChan map[int]chan Op
	// 记录每个客户端的最大已经处理的请求序列号，防止重复执行
	seqMap	map[int64]int

	persister *raft.Persister
	currentBytes int
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

	// Your definitions here.
	kv.kvDB =make(map[string]string)
	kv.notifyChan = make(map[int]chan Op) 
	kv.seqMap	= make(map[int64]int)

	kv.persister = persister
	kv.ReadSnapShot(kv.persister.ReadSnapshot())

	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}

	go kv.applyMsgLoop()
	return kv
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return 
	}
	_,isLeader := kv.rf.GetState()
	if !isLeader{
		reply.Err = ErrWrongLeader
		return 
	}
	// 将操作封装成Op然后写入raft日志
	op := Op{
		OpType: "Get",
		Key:args.Key,
		SeqId : args.SeqId,
		ClientId : args.ClientId,
	}
	lastIndex, _, _ := kv.rf.Start(op)
	
	// 获取通道
	ch := kv.getWaitCh(lastIndex)

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChan, op.Index)
		kv.mu.Unlock()
	}()

	// 超时
	timer := time.NewTicker(100*time.Millisecond)
	defer timer.Stop()

	select{
	case res:= <- ch:
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId !=res.ClientId || op.SeqId != res.SeqId{
			reply.Err = ErrWrongLeader
		}else{
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvDB[args.Key]
			kv.mu.Unlock()
			return 
		}
	case <- timer.C:
		reply.Err = ErrWrongLeader
	}
	
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return 
	}
	_,isLeader := kv.rf.GetState()
	if !isLeader{
		reply.Err = ErrWrongLeader
		return 
	}
	// 将操作封装成Op然后写入raft日志
	op := Op{
		OpType: args.Op,
		Key:args.Key,
		SeqId : args.SeqId,
		ClientId : args.ClientId,
		Value: args.Value,
	}
	lastIndex, _, _ := kv.rf.Start(op)
	
	// 获取通道
	ch := kv.getWaitCh(lastIndex)

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChan, op.Index)
		kv.mu.Unlock()
	}()

	// 超时
	timer := time.NewTicker(100*time.Millisecond)
	defer timer.Stop()

	select{
	case res:= <- ch:
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId !=res.ClientId || op.SeqId != res.SeqId{
			reply.Err = ErrWrongLeader
		}else{
			reply.Err = OK
			return 
		}
	case <- timer.C:
		reply.Err = ErrWrongLeader
	}
}

// 获取index日志对应的通道
func (kv *KVServer) getWaitCh (index int) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch,exist := kv.notifyChan[index]
	if !exist{
		kv.notifyChan[index] = make(chan Op , 1)
		ch = kv.notifyChan[index]
	}
	return ch
}

// 判断是否重复
func (kv *KVServer) isRepeat(clientId int64,seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId,exist := kv.seqMap[clientId]
	if !exist{
		return false
	}
	return seqId<=lastSeqId
}

// 后台协程，读取ApplyCh中的数据
func (kv* KVServer) applyMsgLoop() {
	for {
		if kv.killed(){
			return 
		}
		select{
		case msg:= <-kv.applyCh:
			if msg.CommandValid{
				op:=msg.Command.(Op)
				index := msg.CommandIndex
				
				if !kv.isRepeat(op.ClientId, op.SeqId){
					kv.mu.Lock()
					switch op.OpType{
					case "Append":
						kv.kvDB[op.Key]+=op.Value
					case "Put":
						kv.kvDB[op.Key]=op.Value
					}
					kv.seqMap[op.ClientId]=op.SeqId
					kv.mu.Unlock()
				}

				// 需要进行快照压缩
				// RaftStateSize获取当前Raft日志中已经使用了多少内存，大了的话就直接压缩
				if kv.maxraftstate!=-1 && kv.persister.RaftStateSize() > kv.maxraftstate{
					// 将快照中kv数据库中的数据进行持久化
					snapshot := kv.PersistSnapShot()
					// 将快照的最后Index和快照数据传给Raft日志
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				// 将结果压到通道里面
				kv.getWaitCh(index) <- op
			}else if msg.SnapshotValid{
				// 如果是Raft那边对于这个数据是直接从快照里面读取的话，就可以直接读快照
				kv.mu.Lock()
				// 读取快照的数据
				kv.ReadSnapShot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// unreliable net, restarts, partitions, snapshots, random keys, many clients

// 将快照中的数据持久化
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

// 读取快照中的数据
func (kv *KVServer) ReadSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvDB map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvDB) == nil && d.Decode(&seqMap) == nil {
		kv.kvDB = kvDB
		kv.seqMap = seqMap
	}
}
