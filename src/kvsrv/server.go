package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	store map[string]string
	clientLastSeq map[int64]int64
	// 存储回复
	clientLastReply map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value=kv.store[args.Key]
}

// put是直接返回空字符串
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientID := args.CID
	seq,ok := kv.clientLastSeq[clientID]
	if ok && args.SeqNum <= seq{
		reply.Value = kv.clientLastReply[clientID]
		return 
	}

	reply.Value = ""
	kv.store[args.Key]=args.Value

	kv.clientLastSeq[clientID]=args.SeqNum
	kv.clientLastReply[clientID]=reply.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientID:=args.CID
	seq,ok:=kv.clientLastSeq[clientID]
	if ok && args.SeqNum<=seq{
		reply.Value=kv.clientLastReply[clientID]
		return 
	}
	reply.Value=kv.store[args.Key]
	kv.store[args.Key]+=args.Value
	kv.clientLastSeq[clientID]=args.SeqNum
	kv.clientLastReply[clientID]=reply.Value

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.clientLastSeq = make(map[int64]int64)
	kv.clientLastReply = make(map[int64]string)

	return kv
}
