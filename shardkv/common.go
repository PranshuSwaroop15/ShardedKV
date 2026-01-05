package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running OmniPaxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// const NShards = 10

// const delay = 250 * time.Millisecond

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNorReady"
	ErrNotLeader   = "ErrNotLeader"
	ErrTryAgain    = "ErrTryAgain"
	ErrTimeout     = "ErrTimeout"
	WrongLeader    = "Wrong leader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.z
	// You'll have to add definitions here.
	ClientID  int64
	RequestId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

// type DeleteArgs struct {
// 	Num     int
// 	ShardId int
// }

// type DeleteReply struct {
// 	Err Err
// }

// type ReconfigArgs struct {
// 	Config shardctrler.Config

// 	Store [NShards]map[string]string
// 	Ack   map[int64]int64
// }

// type TransferArgs struct {
// 	Num      int
// 	ShardIds []int
// }

// type TransferReply struct {
// 	Store [NShards]map[string]string

// 	Prev map[int64]int64

// 	Err Err
// }
