package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// 确定哪个服务器是leader
	leaderId int
	// 防止重复
	seqId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = int(nrand())%len(servers)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	args:=GetArgs{
		Key:key,
		ClientId:ck.clientId,
		SeqId:ck.seqId,
	}
	lastLeaderId := ck.leaderId
	// 发送RPC
	for {
		reply := GetReply{}
		ok := ck.servers[lastLeaderId].Call("KVServer.Get", &args, &reply)
		if ok{
			if reply.Err ==ErrNoKey{
				ck.leaderId = lastLeaderId
				return ""
			}else if reply.Err == OK{
				ck.leaderId = lastLeaderId
				return reply.Value
			}else if reply.Err == ErrWrongLeader{
				// 换个服务器继续连接
				lastLeaderId =(lastLeaderId+1)%len(ck.servers)
				continue
			}
		}
		// 可能这个leader节点会崩
		lastLeaderId =(lastLeaderId+1)%len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	args:=PutAppendArgs{
		Key:key,
		Value:value,
		Op:op,
		ClientId:ck.clientId,
		SeqId:ck.seqId,	
	}
	lastLeaderId := ck.leaderId
	// 发送RPC
	for {
		reply := PutAppendReply{}
		ok := ck.servers[lastLeaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok{
			if reply.Err == OK{
				ck.leaderId = lastLeaderId
				return 
			}else if reply.Err == ErrWrongLeader{
				// 换个服务器继续连接
				lastLeaderId =(lastLeaderId+1)%len(ck.servers)
				continue
			}
		}
		// 可能这个leader节点会崩
		lastLeaderId =(lastLeaderId+1)%len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
