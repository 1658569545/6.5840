package kvraft

type Err string
type OpType string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrKilled      Err = "ErrKilled"
	ErrDuplicate   Err = "ErrDuplicate"
	ErrTimeout     Err = "ErrTimeout"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	
	Op	string
	ClientId int64
	SeqId	int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId	int
}

type GetReply struct {
	Err   Err
	Value string
}
