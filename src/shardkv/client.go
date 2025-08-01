package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// 将数据映射到分片中
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	
	// 记录各个group的leaderId
	leaderIds map[int]int64
	
	clientId int64
	sequenceNum int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderIds = make(map[int]int64)
	ck.clientId = nrand()
	ck.sequenceNum = 1
	return ck
}

// 对某个分片进行get
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		SequenceNum : ck.sequenceNum,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _,ok := ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			for {
				var reply GetReply
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.sequenceNum++
					return reply.Value
				} else if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrShardNotReady  || reply.Err == ErrTimeout) {
					ck.config = ck.sm.Query(-1)
					break
				} else {
					newLeaderId = (newLeaderId + 1) % int64(len(servers))
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// 重新获取配置
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _,ok := ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			for {
				var reply PutAppendReply
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.sequenceNum++
					return
				} else if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrShardNotReady  || reply.Err == ErrTimeout) {
					ck.config = ck.sm.Query(-1)
					break
				} else {
					newLeaderId = (newLeaderId + 1) % int64(len(servers))
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// 重新获取配置
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
