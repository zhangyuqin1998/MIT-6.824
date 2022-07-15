package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	ClientId	int64
	Op    		string // "Put" or "Append"
	Seq			int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err 	 Err
	ServerId int
}

type GetArgs struct {
	Key 		string
	ClientId	int64
	Seq			int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   	 Err
	Value 	 string
	ServerId int
}
