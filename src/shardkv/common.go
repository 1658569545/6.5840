package shardkv
import (
	"time"

)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//


const (
	EngineStepInterval = 100 * time.Millisecond
	ClientRetryInterval = 100 * time.Millisecond
	UpdateConfigInterval = 100 * time.Millisecond
	InstallShardsInterval = 300 * time.Millisecond
	EmptyTermCheckInterval = 300 * time.Millisecond
	SnapshotInterval = 300 * time.Millisecond
)


const (
	OK             		= "OK"
	ErrNoKey       		= "ErrNoKey"
	ErrWrongGroup  		= "ErrWrongGroup"
	ErrWrongLeader 		= "ErrWrongLeader"
	ErrTimeout     		= "ErrTimeout"
	ErrShardNotReady 	= "ErrShardNotReady"
	ErrCmd				= "ErrCmd"
)

type Err string

type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64 
	SequenceNum int64
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	ClientId int64 
	SequenceNum int64 
}


type GetReply struct {
	Err   string
	Value string
}


type InstallShardArgs struct{
	Num int 			// 配置号（Config.Num），表示这是第几个配置
	Dest []string 		// 目标group的服务器列表
	Src []string 		// 源group的服务器列表
	Shards []int 		// 要迁移的Shards列表
	ClientSequences map[int64]int64 	// 客户端去重表
	Data map[string]string 			// 分片内的所有k/v数据
}

type InstallShardReply struct {
	Err string
}

type DeleteShardArgs struct {
	Num int 
	Dest []string
	Src []string
	Shards []int  	// 要删除的分片编号列表
	Keys []string   // 要删除的 key 列表（可选，通常是分片内所有 key）
}

type DeleteShardReply struct {
	Err string
}

const (
	Serving = 1		// 本组（gid）当前拥有该分片，且可以正常对外提供服务。
	Pulling = 2		// 本组（gid）在新配置下将要拥有该分片，但还没有拿到数据，正在“拉取”分片数据
	Offering = 3	// 本组（gid）在新配置下不再拥有该分片，但还没有把数据交给新 owner，正在“提供”分片数据。
)
type ShardNodeState int8

const (
	GetCmd = 1
	PutCmd = 2
	AppendCmd = 3
	InstallShardCmd = 4
	DeleteShardCmd = 5
	SyncConfigCmd = 6	// 同步配置操作
)
type CmdType int8

type EmptyOp struct {
}

func int64TableDeepCopy(src map[int64]int64) map[int64]int64 {
	dst := make(map[int64]int64)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func stringTableDeepCopy(src map[string]string) map[string]string {
	dst := make(map[string]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
