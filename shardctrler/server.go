// // Working version of ShardCtrler.
package shardctrler

import (
	"cs651/labgob"
	"cs651/labrpc"
	"fmt"
	"plugin"
	"runtime"
	"sort"
	"sync"
	"time"

	omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	op      omnipaxoslib.IOmnipaxos
	applyCh chan omnipaxoslib.ApplyMsg

	// Your data here.
	applyCommands map[int64]chan interface{}
	duplicate     map[int64]int // Client -> SequenceID
	lastQuery     map[int64]int
	configs       []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type       string
	Args       interface{}
	Client     int64
	Leader     int
	SequenceID int
}

func (sc *ShardCtrler) handleRequest(opType string, args interface{}, clientId int64, seqId int) (bool, Config) {
	// Check for duplicate request
	sc.mu.Lock()
	if seq, exists := sc.duplicate[clientId]; exists && seq >= seqId {
		if opType == "Query" {
			config := sc.configs[sc.lastQuery[clientId]]
			sc.mu.Unlock()
			return true, config
		}
		sc.mu.Unlock()
		return true, Config{}
	}
	sc.mu.Unlock()

	// Create and submit operation
	op := Op{
		Type:       opType,
		Args:       args,
		Client:     clientId,
		Leader:     sc.me,
		SequenceID: seqId,
	}

	_, _, isLeader := sc.op.Proposal(op)
	if !isLeader {
		return false, Config{}
	}

	// Wait for response
	rpcChan := make(chan interface{}, 1)
	sc.mu.Lock()
	sc.applyCommands[clientId] = rpcChan
	sc.mu.Unlock()

	select {
	case val := <-rpcChan:
		return true, val.(Config)
	case <-time.After(200 * time.Millisecond):
		return false, Config{}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	success, _ := sc.handleRequest("Join", *args, args.ClientId, args.SequenceID)
	reply.WrongLeader = !success
	if !success {
		reply.Err = "TimeoutErr"
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	success, _ := sc.handleRequest("Leave", *args, args.ClientId, args.SequenceID)
	reply.WrongLeader = !success
	if !success {
		reply.Err = "TimeoutErr"
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	success, _ := sc.handleRequest("Move", *args, args.ClientId, args.SequenceID)
	reply.WrongLeader = !success
	if !success {
		reply.Err = "TimeoutErr"
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	success, config := sc.handleRequest("Query", *args, args.ClientId, args.SequenceID)
	reply.WrongLeader = !success
	if success {
		reply.Config = config
	} else {
		reply.Err = "TimeoutErr"
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.op.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) OmniPaxos() omnipaxoslib.IOmnipaxos {
	return sc.op
}

// servers[] contains the ports of the set of
// servers that will cooperate via OmniPaxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan omnipaxoslib.ApplyMsg)
	// sc.op = omnipaxos.Make(servers, me, persister, sc.applyCh)

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

	sc.op = mkrf(callables, me, persister, sc.applyCh)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(MoveArgs{})

	sc.duplicate = make(map[int64]int)
	sc.applyCommands = make(map[int64]chan interface{})
	sc.lastQuery = make(map[int64]int)

	go func() {
		for {
			cmd := <-sc.applyCh
			if op, ok := cmd.Command.(Op); ok {
				sc.mu.Lock()
				seq, exist := sc.duplicate[op.Client]
				if !exist || exist && seq < op.SequenceID {
					value := Config{}
					switch op.Type {
					case "Join":
						value = sc.handleJoin(op)
					case "Leave":
						value = sc.handleLeave(op)
					case "Move":
						value = sc.handleMove(op)
					case "Query":
						value = sc.handleQuery(op)
					}
					sc.duplicate[op.Client] = op.SequenceID
					if op.Leader == sc.me {
						select {
						case sc.applyCommands[op.Client] <- value:
							break
						case <-time.After(200 * time.Millisecond):
							// fmt.Printf("chan no response\n")
							break
						}
					}
				}
				sc.mu.Unlock()
			} else {
				fmt.Println("Type assertion failed")
			}
		}

	}()
	return sc
}

func (sc *ShardCtrler) handleJoin(op Op) Config {
	args := op.Args.(JoinArgs)
	config := Config{}
	sc.initNewConfig(&config)

	newGroupNum := NShards / (len(args.Servers) + len(sc.configs[len(sc.configs)-1].Groups))
	if newGroupNum < 1 {
		newGroupNum = 1
	}

	var dets []int
	for dst := range args.Servers {
		dets = append(dets, dst)
	}
	sort.Ints(dets)

	if len(sc.configs[len(sc.configs)-1].Groups) == 0 {
		shard := 0
		for _, dst := range dets {
			config.Groups[dst] = args.Servers[dst]
			for j := 0; j < newGroupNum; j++ {
				config.Shards[shard] = dst
				shard += 1
			}
			if NShards < shard+newGroupNum {
				for shard < NShards {
					config.Shards[shard] = dst
					shard += 1
				}
			}
		}
	} else {
		groupMap := GroupGidMap(&sc.configs[len(sc.configs)-1])
		for _, dst := range dets {
			config.Groups[dst] = args.Servers[dst]
			for j := 0; j < newGroupNum; j++ {
				src := MaxShardGroup(groupMap)
				config.Shards[groupMap[src][0]] = dst
				groupMap[src] = groupMap[src][1:]
			}
		}
	}

	sc.configs = append(sc.configs, config)
	return config
}

func (sc *ShardCtrler) handleLeave(op Op) Config {
	args := op.Args.(LeaveArgs)
	config := Config{}
	sc.initNewConfig(&config)

	groupMap := GroupGidMap(&sc.configs[len(sc.configs)-1])
	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		shards := groupMap[gid]
		delete(groupMap, gid)
		for _, shard := range shards {
			dst := MinShardGroup(groupMap)
			config.Shards[shard] = dst
			groupMap[dst] = append(groupMap[dst], shard)
		}
	}

	sc.configs = append(sc.configs, config)
	return config
}

func (sc *ShardCtrler) handleMove(op Op) Config {
	args := op.Args.(MoveArgs)
	config := Config{}
	sc.initNewConfig(&config)
	config.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, config)
	return config
}

func (sc *ShardCtrler) handleQuery(op Op) Config {
	args := op.Args.(QueryArgs)
	if args.Num == -1 || args.Num > len(sc.configs)-1 {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[args.Num]
	}
}

func (sc *ShardCtrler) initNewConfig(config *Config) {
	config.Num = sc.configs[len(sc.configs)-1].Num + 1
	config.Shards = sc.configs[len(sc.configs)-1].Shards
	config.Groups = make(map[int][]string)

	for groupId, servers := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[groupId] = servers
	}
}

func GroupGidMap(config *Config) map[int][]int {
	/* Get Map groupId -> pos in Shards */
	groupMap := make(map[int][]int)
	for groupId := range config.Groups {
		groupMap[groupId] = make([]int, 0)
	}
	for index, groupId := range config.Shards {
		groupMap[groupId] = append(groupMap[groupId], index)
	}
	return groupMap
}

func MaxShardGroup(groupMap map[int][]int) int {
	max := -1
	maxId := 0
	for groupId, shards := range groupMap {
		if len(shards) > max || (len(shards) == max && groupId > maxId) { //
			maxId = groupId
			max = len(shards)
		}
	}
	return maxId
}

func MinShardGroup(groupMap map[int][]int) int {
	min := NShards + 1
	minId := 0
	for groupId, shards := range groupMap {
		if len(shards) < min || (len(shards) == min && groupId > minId) { //
			minId = groupId
			min = len(shards)
		}
	}
	return minId
}
