package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int64
	// 客户端ID+客户请求序列号
	clientId int64
	seqNum int64
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
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqNum = 1
	return ck
}

// ========= 找到一个可以用的leaderId =========
func (ck *Clerk) GetState() int {
	for {
		args := IsLeaderArgs{}
		reply := IsLeaderReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.IsLeader", &args, &reply)
		if ok && reply.IsLeader{
			return int(ck.leaderId)
		}else{
			// 判断下一个
			time.Sleep(10*time.Millisecond )
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		}
	}
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
	args:=GetArgs{
		Key:key, 
		ClientId:ck.clientId, 
		SeqNum:ck.seqNum,
	}
	reply:=GetReply{}
	// 找到一个可以用的leaderId
	ck.GetState()
	ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	for !ok || reply.Err!= OK{
		// 这个不行，继续下一个
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	}
	if ok{
		// 成功了
		ck.seqNum++
	}
	return reply.Value
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
	args:=PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		SeqNum: ck.seqNum,
	}
	reply:=PutAppendReply{}
	ck.GetState()
	ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	for !ok || reply.Err!= OK{
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	}
	if ok{
		ck.seqNum++
	}
}


func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
