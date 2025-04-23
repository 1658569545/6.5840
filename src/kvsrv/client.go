package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int64
	seq int64
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.seq=0
	ck.clientID=nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// 由于get是幂等的，因此无须标记序列号
	args := GetArgs{
		Key:key,
	}
	for{
		reply := GetReply{}
		ok:=ck.server.Call("KVServer.Get",&args,&reply)
		if ok{
			return reply.Value
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:	key,
		Value:	value,
		CID:	ck.clientID,
		SeqNum:	ck.seq,
	}
	ck.mu.Unlock()
	// 消耗一个序列号
	ck.seq++;
	for{
		reply:=PutAppendReply{}
		ok := ck.server.Call("KVServer."+op,&args,&reply)
		if ok{
			return reply.Value
		}
	}
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
