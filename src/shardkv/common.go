package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            		Err = "OK"
	ErrNoKey       	  		= "ErrNoKey"
	ErrWrongGroup  	  		= "ErrWrongGroup"
	ErrWrongLeader 	  		= "ErrWrongLeader"
	ShardNotArrived         = "ShardNotArrived"
	ConfigNotArrived        = "ConfigNotArrived"
	ErrInconsistentData     = "ErrInconsistentData"
	ErrOverTime             = "ErrOverTime"
)

const (
	PutType 	Operation = "Put"
	AppendType 	    	  = "Append"
	GetType 	  	  	  = "Get"
	UpConfigType 		  = "UpConfig"
	AddShardType 		  = "AddShard"
	RemoveShardType 	  = "RemoveShard"
)

type Operation string

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	SeqId   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	SeqId   int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArg struct {
	LastAppliedRequestId map[int64]int // 接收方用来更新其客户端请求序号映射
	ShardId              int
	Shard                Shard // Shard to be sent
	ClientId             int64
	RequestId            int64// 请求序号（通常是配置版本号）
}

type AddShardReply struct {
	Err Err
}
