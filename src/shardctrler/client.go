package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId int64
	seqId   int

	mu sync.Mutex
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seqId++
	args := &QueryArgs{
		ClientId: ck.clientId,
		SeqId: ck.seqId,
		Num: num,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seqId++	
	args := &JoinArgs{
		Servers: servers,
		ClientId: ck.clientId,
		SeqId: ck.seqId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seqId++
	args := &LeaveArgs{
		GIDs: gids,
		ClientId: ck.clientId,
		SeqId: ck.seqId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seqId++
	args := &MoveArgs{
		Shard: shard,
		GID: gid,
		ClientId: ck.clientId,
		SeqId: ck.seqId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
