package shardkv

import (
	"bytes"
	"cs651/a3b-sharding/shardctrler"
	"cs651/labgob"
	"cs651/labrpc"
	"fmt"
	"plugin"
	"runtime"
	"sync"
	"time"

	omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
)

type ShardKV struct {
	mu            sync.Mutex
	me            int
	op            omnipaxoslib.IOmnipaxos
	applyCh       chan omnipaxoslib.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxpaxosstate int

	data     [shardctrler.NShards]map[string]string
	ack      map[int64]int64
	resultCh map[int]chan Result
	config   shardctrler.Config
	mck      *shardctrler.Clerk
}

type Op struct {
	Command   string
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
	Config    shardctrler.Config
	Data      [shardctrler.NShards]map[string]string
	Ack       map[int64]int64
	ConfigNum int
	ShardId   int
}

type Result struct {
	Command     string
	OK          bool
	WrongLeader bool
	Err         Err
	ClientId    int64
	RequestId   int64
	Value       string
	ConfigNum   int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	entry := kv.createGetEntry(args)
	result := kv.executeEntry(entry)
	kv.fillGetReply(reply, result)
}

func (kv *ShardKV) createGetEntry(args *GetArgs) Op {
	return Op{
		Command:   "get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
	}
}

func (kv *ShardKV) fillGetReply(reply *GetReply, result Result) {
	reply.WrongLeader = !result.OK
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := kv.createPutAppendEntry(args)
	result := kv.executeEntry(entry)
	kv.fillPutAppendReply(reply, result)
}

func (kv *ShardKV) createPutAppendEntry(args *PutAppendArgs) Op {
	return Op{
		Command:   args.Op,
		ClientId:  args.ClientID,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}
}

func (kv *ShardKV) fillPutAppendReply(reply *PutAppendReply, result Result) {
	reply.WrongLeader = !result.OK
	reply.Err = result.Err
}

func (kv *ShardKV) executeEntry(entry Op) Result {
	index, _, isLeader := kv.op.Proposal(entry)
	if !isLeader {
		return Result{OK: false}
	}

	return kv.waitForResult(index, entry)
}

func (kv *ShardKV) waitForResult(index int, entry Op) Result {
	kv.mu.Lock()
	ch := kv.getResultChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		if kv.isMatchingResult(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(240 * time.Millisecond):
		return Result{OK: false}
	}
}

func (kv *ShardKV) getResultChannel(index int) chan Result {
	if _, ok := kv.resultCh[index]; !ok {
		kv.resultCh[index] = make(chan Result, 1)
	}
	return kv.resultCh[index]
}

func (kv *ShardKV) isMatchingResult(entry Op, result Result) bool {
	if entry.Command == "reconfigure" {
		return entry.Config.Num == result.ConfigNum
	}
	if entry.Command == "delete" {
		return entry.ConfigNum == result.ConfigNum
	}
	return entry.ClientId == result.ClientId && entry.RequestId == result.RequestId
}

func (kv *ShardKV) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId

	switch op.Command {
	case "put":
		kv.applyPut(op, &result)
	case "append":
		kv.applyAppend(op, &result)
	case "get":
		kv.applyGet(op, &result)
	case "reconfigure":
		kv.applyReconfigure(op, &result)
	case "delete":
		kv.applyCleanup(op, &result)
	}
	return result
}

func (kv *ShardKV) applyPut(op Op, result *Result) {
	shard := key2shard(op.Key)
	if kv.data[shard] == nil {
		kv.data[shard] = make(map[string]string)
	}

	if !kv.isValidKey(op.Key) {
		result.Err = ErrWrongGroup
		return
	}

	if !kv.isDuplicated(op) {
		kv.data[shard][op.Key] = op.Value
		kv.ack[op.ClientId] = op.RequestId
	}

	result.Err = OK
}

func (kv *ShardKV) applyAppend(op Op, result *Result) {
	shard := key2shard(op.Key)
	if kv.data[shard] == nil {
		kv.data[shard] = make(map[string]string)
	}

	if !kv.isValidKey(op.Key) {
		result.Err = ErrWrongGroup
		return
	}

	if !kv.isDuplicated(op) {
		kv.data[shard][op.Key] += op.Value
		kv.ack[op.ClientId] = op.RequestId
	}

	result.Err = OK
}

func (kv *ShardKV) applyGet(op Op, result *Result) {
	if !kv.isValidKey(op.Key) {
		result.Err = ErrWrongGroup
		return
	}

	if !kv.isDuplicated(op) {
		kv.ack[op.ClientId] = op.RequestId
	}

	shard := key2shard(op.Key)
	if value, exists := kv.data[shard][op.Key]; exists {
		result.Value = value
		result.Err = OK
	} else {
		result.Err = ErrNoKey
	}
}

func (kv *ShardKV) applyReconfigure(op Op, result *Result) {
	result.ConfigNum = op.Config.Num

	if op.Config.Num == kv.config.Num+1 {
		for shardId, shardData := range op.Data {
			if kv.data[shardId] == nil {
				kv.data[shardId] = make(map[string]string)
			}
			for key, value := range shardData {
				kv.data[shardId][key] = value
			}
		}

		for clientId, requestId := range op.Ack {
			if _, exists := kv.ack[clientId]; !exists || kv.ack[clientId] < requestId {
				kv.ack[clientId] = requestId
			}
		}

		kv.config = op.Config
	}

	result.Err = OK
}

func (kv *ShardKV) applyCleanup(op Op, result *Result) {
	if op.ConfigNum <= kv.config.Num {
		if kv.gid != kv.config.Shards[op.ShardId] {
			kv.data[op.ShardId] = make(map[string]string)
		}
	}
}

func (kv *ShardKV) isDuplicated(op Op) bool {
	lastRequestId, found := kv.ack[op.ClientId]
	return found && lastRequestId >= op.RequestId
}

func (kv *ShardKV) isValidKey(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) Kill() {
	kv.op.Kill()
}

func (kv *ShardKV) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			result := kv.applyOp(op)
			if ch, ok := kv.resultCh[msg.CommandIndex]; ok {
				select {
				case <-ch:
				default:
				}
			} else {
				kv.resultCh[msg.CommandIndex] = make(chan Result, 1)
			}
			kv.resultCh[msg.CommandIndex] <- result

			if kv.maxpaxosstate != -1 && kv.op.(omnipaxoslib.Persistable).OmnipaxosStateSize() > kv.maxpaxosstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.ack)
				e.Encode(kv.config)
				kv.op.(omnipaxoslib.Persistable).SaveStateAndSnapshot(w.Bytes(), nil)
			}
		}
		kv.mu.Unlock()
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable, maxOmniPaxosstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxpaxosstate = maxOmniPaxosstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan omnipaxoslib.ApplyMsg)
	kv.data = [shardctrler.NShards]map[string]string{}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}

	kv.ack = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Result)
	kv.mck = shardctrler.MakeClerk(ctrlers)

	p, err := plugin.Open(fmt.Sprintf("../omnipaxosmain-%s-%s.so", runtime.GOOS, runtime.GOARCH))
	if err != nil {
		panic(err)
	}
	xrf, err := p.Lookup("MakeOmnipaxos")
	if err != nil {
		panic(err)
	}

	mkrf := xrf.(func([]omnipaxoslib.Callable, int, omnipaxoslib.Persistable, chan omnipaxoslib.ApplyMsg) omnipaxoslib.IOmnipaxos)

	callables := make([]omnipaxoslib.Callable, len(servers))
	for i, s := range servers {
		callables[i] = s
	}

	kv.op = mkrf(callables, me, persister, kv.applyCh)

	go kv.Run()
	go kv.Reconfigure()

	return kv
}

func (kv *ShardKV) Reconfigure() {
	for {
		if _, isLeader := kv.op.GetState(); isLeader {
			latestConfig := kv.mck.Query(-1)
			for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
				nextConfig := kv.mck.Query(i)
				entry, ok := kv.getReconfigureEntry(nextConfig)
				if !ok {
					break
				}
				result := kv.executeEntry(entry)
				if !result.OK {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// func (kv *ShardKV) getReconfigureEntry(nextConfig shardctrler.Config) (Op, bool) {
// 	entry := Op{}
// 	entry.Command = "reconfigure"
// 	entry.Config = nextConfig
// 	for i := 0; i < shardctrler.NShards; i++ {
// 		entry.Data[i] = make(map[string]string)
// 	}
// 	entry.Ack = make(map[int64]int64)
// 	ok := true

// 	var ackMu sync.Mutex
// 	var wg sync.WaitGroup

// 	transferShards := kv.getShardsToTransfer(nextConfig)
// 	for gid, shardIds := range transferShards {
// 		wg.Add(1)

// 		go func(gid int, args TransferShardArgs, reply TransferShardReply) {
// 			defer wg.Done()

// 			if kv.sendTransferShard(gid, &args, &reply) {
// 				ackMu.Lock()
// 				for _, shardId := range args.ShardIds {
// 					shardData := reply.Data[shardId]
// 					for k, v := range shardData {
// 						entry.Data[shardId][k] = v
// 					}
// 				}
// 				for clientId := range reply.Ack {
// 					if _, ok := entry.Ack[clientId]; !ok || entry.Ack[clientId] < reply.Ack[clientId] {
// 						entry.Ack[clientId] = reply.Ack[clientId]
// 					}
// 				}
// 				ackMu.Unlock()
// 			} else {
// 				ok = false
// 			}
// 		}(gid, TransferShardArgs{Num: nextConfig.Num, ShardIds: shardIds}, TransferShardReply{})
// 	}
// 	wg.Wait()
// 	return entry, ok
// }

// func (kv *ShardKV) getShardsToTransfer(nextConfig shardctrler.Config) map[int][]int {
// 	transferShards := make(map[int][]int)
// 	for i := 0; i < shardctrler.NShards; i++ {
// 		if kv.config.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
// 			gid := kv.config.Shards[i]
// 			if gid != 0 {
// 				if _, ok := transferShards[gid]; !ok {
// 					transferShards[gid] = make([]int, 0)
// 				}
// 				transferShards[gid] = append(transferShards[gid], i)
// 			}
// 		}
// 	}
// 	return transferShards
// }

// type TransferShardArgs struct {
// 	Num      int
// 	ShardIds []int
// }

// type TransferShardReply struct {
// 	Err  Err
// 	Data [shardctrler.NShards]map[string]string
// 	Ack  map[int64]int64
// }

// func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	if kv.config.Num < args.Num {
// 		reply.Err = ErrWrongLeader
// 		return
// 	}

// 	for i := 0; i < shardctrler.NShards; i++ {
// 		reply.Data[i] = make(map[string]string)
// 	}
// 	for _, shardId := range args.ShardIds {
// 		for k, v := range kv.data[shardId] {
// 			reply.Data[shardId][k] = v
// 		}
// 	}
// 	reply.Ack = make(map[int64]int64)
// 	for clientId, requestId := range kv.ack {
// 		reply.Ack[clientId] = requestId
// 	}
// 	reply.Err = OK
// }

// func (kv *ShardKV) sendTransferShard(gid int, args *TransferShardArgs, reply *TransferShardReply) bool {
// 	for _, server := range kv.config.Groups[gid] {
// 		srv := kv.make_end(server)
// 		ok := srv.Call("ShardKV.TransferShard", args, reply)
// 		if ok {
// 			if reply.Err == OK {
// 				return true
// 			}
// 			if reply.Err == ErrWrongLeader {
// 				return false
// 			}
// 		}
// 	}
// 	return true
// }

func (kv *ShardKV) getReconfigureEntry(nextConfig shardctrler.Config) (Op, bool) {
	entry := Op{}
	entry.Command = "reconfigure"
	entry.Config = nextConfig
	for i := 0; i < shardctrler.NShards; i++ {
		entry.Data[i] = make(map[string]string)
	}
	entry.Ack = make(map[int64]int64)
	ok := true

	var ackMu sync.Mutex
	var wg sync.WaitGroup

	transferShards := kv.getShardsToTransfer(nextConfig)
	for gid, shardIds := range transferShards {
		wg.Add(1)

		go func(gid int, args TransferShardArgs, reply TransferShardReply) {
			defer wg.Done()

			if kv.sendTransferShard(gid, &args, &reply) {
				ackMu.Lock()
				for _, shardId := range args.ShardIds {
					shardData := reply.Data[shardId]
					for k, v := range shardData {
						entry.Data[shardId][k] = v
					}
				}
				for clientId := range reply.Ack {
					if _, ok := entry.Ack[clientId]; !ok || entry.Ack[clientId] < reply.Ack[clientId] {
						entry.Ack[clientId] = reply.Ack[clientId]
					}
				}
				ackMu.Unlock()
			} else {
				ok = false
			}
		}(gid, TransferShardArgs{Num: nextConfig.Num, ShardIds: shardIds}, TransferShardReply{})
	}
	wg.Wait()
	return entry, ok
}

func (kv *ShardKV) getShardsToTransfer(nextConfig shardctrler.Config) map[int][]int {
	transferShards := make(map[int][]int)
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.config.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := transferShards[gid]; !ok {
					transferShards[gid] = make([]int, 0)
				}
				transferShards[gid] = append(transferShards[gid], i)
			}
		}
	}
	return transferShards
}

type TransferShardArgs struct {
	Num      int
	ShardIds []int
}

type TransferShardReply struct {
	Err  Err
	Data [shardctrler.NShards]map[string]string
	Ack  map[int64]int64
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.Num {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i < shardctrler.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	for _, shardId := range args.ShardIds {
		for k, v := range kv.data[shardId] {
			reply.Data[shardId][k] = v
		}
	}
	reply.Ack = make(map[int64]int64)
	for clientId, requestId := range kv.ack {
		reply.Ack[clientId] = requestId
	}
	reply.Err = OK
}

func (kv *ShardKV) sendTransferShard(gid int, args *TransferShardArgs, reply *TransferShardReply) bool {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.TransferShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrWrongLeader {
				return false
			}
		}
	}
	return true
}

type DeleteShardArgs struct {
	Num     int
	ShardId int
}

type DeleteShardReply struct {
	WrongLeader bool
	Err         Err
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.op.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	if args.Num > kv.config.Num {
		reply.WrongLeader = false
		reply.Err = ErrWrongLeader
		return
	}

	entry := Op{}
	entry.Command = "delete"
	entry.ConfigNum = args.Num
	entry.ShardId = args.ShardId
	kv.executeEntry(entry)

	reply.WrongLeader = false
	reply.Err = OK
}

func (kv *ShardKV) sendDeleteShard(gid int, lastConfig *shardctrler.Config, args *DeleteShardArgs, reply *DeleteShardReply) bool {
	for _, server := range lastConfig.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.DeleteShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrWrongLeader {
				return false
			}
		}
	}
	return true
}
