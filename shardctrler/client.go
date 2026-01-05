// package shardctrler

// //
// // Shardctrler clerk.
// //

// import (
// 	"crypto/rand"
// 	"cs651/labrpc"
// 	"math/big"
// 	"sync"
// 	"time"
// )

// type Clerk struct {
// 	servers []*labrpc.ClientEnd
// 	// Your data here.
// 	clientId  int64
// 	requestId int64
// 	mu        sync.Mutex
// }

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

// func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
// 	ck := new(Clerk)
// 	ck.servers = servers
// 	// Your code here.
// 	ck.clientId = nrand()
// 	ck.requestId = 0
// 	return ck
// }

// func (ck *Clerk) Query(num int) Config {
// 	args := &QueryArgs{}
// 	// Your code here.
// 	args.Num = num
// 	args.ClientId = ck.clientId
// 	ck.mu.Lock()
// 	args.RequestId = ck.requestId
// 	ck.requestId++
// 	ck.mu.Unlock()
// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply QueryReply
// 			ok := srv.Call("ShardCtrler.Query", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				return reply.Config
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// func (ck *Clerk) Join(servers map[int][]string) {
// 	args := &JoinArgs{}
// 	// Your code here.
// 	args.Servers = servers
// 	args.ClientId = ck.clientId
// 	ck.mu.Lock()
// 	args.RequestId = ck.requestId
// 	ck.requestId++
// 	ck.mu.Unlock()

// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply JoinReply
// 			ok := srv.Call("ShardCtrler.Join", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				return
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// func (ck *Clerk) Leave(gids []int) {
// 	args := &LeaveArgs{}
// 	// Your code here.
// 	args.GIDs = gids
// 	args.ClientId = ck.clientId
// 	ck.mu.Lock()
// 	args.RequestId = ck.requestId
// 	ck.requestId++
// 	ck.mu.Unlock()

// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply LeaveReply
// 			ok := srv.Call("ShardCtrler.Leave", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				return
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// func (ck *Clerk) Move(shard int, gid int) {
// 	args := &MoveArgs{}
// 	// Your code here.
// 	args.Shard = shard
// 	args.GID = gid
// 	args.ClientId = ck.clientId
// 	ck.mu.Lock()
// 	args.RequestId = ck.requestId
// 	ck.requestId++
// 	ck.mu.Unlock()

// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply MoveReply
// 			ok := srv.Call("ShardCtrler.Move", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				return
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"cs651/labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu       sync.Mutex
	leaderId int
	ckId     int64 // clerk unique id
	seqId    int   // re
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
	ck.ckId = nrand()
	ck.seqId = 0

	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	ck.seqId += 1
	args.Num = num
	args.ClientId = ck.ckId
	args.SequenceID = ck.seqId
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
	args := &JoinArgs{}
	// Your code here.
	ck.seqId += 1
	args.ClientId = ck.ckId
	args.SequenceID = ck.seqId
	args.Servers = servers

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
	args := &LeaveArgs{}
	// Your code here.
	ck.seqId += 1
	args.ClientId = ck.ckId
	args.SequenceID = ck.seqId
	args.GIDs = gids
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
	args := &MoveArgs{}
	// Your code here.
	ck.seqId += 1
	args.ClientId = ck.ckId
	args.SequenceID = ck.seqId
	args.Shard = shard
	args.GID = gid

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
