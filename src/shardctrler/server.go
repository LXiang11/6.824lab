package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead int32

	waitreply map[OpIdentifier]chan Config
	//raft已执行的最大index
	maxexcuteindex int

	persister *raft.Persister

	//每个命令唯一标识
	ClerklastSeqID map[int64]int64
}

type Op struct {
	// Your data here.
	Optype  int
	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int
	ClerkID int64
	SeqlID  int64
}

type OpIdentifier struct {
	index int
	term  int
}

const Timeout = 1 * time.Second

func max(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{0, args.Servers, nil, 0, 0, 0, args.ClerkID, args.SeqlID}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	response := make(chan Config)
	sc.waitreply[OpIdentifier{index, term}] = response
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.waitreply, OpIdentifier{index, term})
		sc.mu.Unlock()
		close(response)
	}()

	select {
	case <-response:
		reply.Err = OK
		return
	case <-time.After(time.Duration(Timeout)):
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{1, nil, args.GIDs, 0, 0, 0, args.ClerkID, args.SeqlID}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	response := make(chan Config)
	sc.waitreply[OpIdentifier{index, term}] = response
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.waitreply, OpIdentifier{index, term})
		sc.mu.Unlock()
		close(response)
	}()

	select {
	case <-response:
		reply.Err = OK
		return
	case <-time.After(time.Duration(Timeout)):
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{2, nil, nil, args.Shard, args.GID, 0, args.ClerkID, args.SeqlID}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	response := make(chan Config)
	sc.waitreply[OpIdentifier{index, term}] = response
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.waitreply, OpIdentifier{index, term})
		sc.mu.Unlock()
		close(response)
	}()

	select {
	case <-response:
		reply.Err = OK
		return
	case <-time.After(time.Duration(Timeout)):
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{3, nil, nil, 0, 0, args.Num, args.ClerkID, args.SeqlID}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	response := make(chan Config)
	sc.waitreply[OpIdentifier{index, term}] = response
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.waitreply, OpIdentifier{index, term})
		sc.mu.Unlock()
		close(response)
	}()

	select {
	case cfg := <-response:
		reply.Err = OK
		reply.Config = cfg
		return
	case <-time.After(time.Duration(Timeout)):
		reply.WrongLeader = true
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) getshard_c(Shards [10]int) map[int]int {
	//gid -> count(shards)
	shard_c := make(map[int]int)
	for _, gid := range Shards {
		shard_c[gid]++
	}
	return shard_c
}

func (sc *ShardCtrler) gettarget_c(group_c int, list [][2]int) map[int]int {
	//每个副本组的目标分片数
	target_c := make(map[int]int)
	for i := 0; i < group_c; i++ {
		target_c[list[i][0]] = NShards / group_c
		if i >= group_c-NShards%group_c {
			target_c[list[i][0]]++
		}
	}
	return target_c
}

func (sc *ShardCtrler) applyCommandOnServer(op Op) Config {
	lastSeqID, ok := sc.ClerklastSeqID[op.ClerkID]
	if ok && lastSeqID >= op.SeqlID && op.Optype < 3 {
		return Config{}
	}
	sc.ClerklastSeqID[op.ClerkID] = max(lastSeqID, op.SeqlID)
	lastconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{lastconfig.Num + 1, lastconfig.Shards, make(map[int][]string, 0)}
	if op.Optype == 0 {
		shard_c := sc.getshard_c(lastconfig.Shards)
		shard_c_list := make([][2]int, 0)
		//添加已有的副本组
		for gid, servers := range lastconfig.Groups {
			//排除无效组
			if gid == 0 {
				continue
			}
			c := shard_c[gid]
			shard_c_list = append(shard_c_list, [2]int{gid, c})
			newconfig.Groups[gid] = servers
		}
		//添加新的副本组
		for gid, servers := range op.Servers {
			newconfig.Groups[gid] = servers
			shard_c_list = append(shard_c_list, [2]int{gid, 0})
		}
		//根据分片数排序
		sort.Slice(shard_c_list, func(i, j int) bool {
			return shard_c_list[i][1] < shard_c_list[j][1] ||
				(shard_c_list[i][1] == shard_c_list[j][1] && shard_c_list[i][0] < shard_c_list[j][0])
		})
		//副本组数量
		group_c := len(shard_c_list)
		//每个副本组的目标分片数
		target_c := sc.gettarget_c(group_c, shard_c_list)
		for shard_c_list_i, shard_i := 0, 0; shard_i < NShards; shard_i++ {
			//当前分片所属的副本组
			curgid := newconfig.Shards[shard_i]
			//如果当前分片所属的副本组分片数大于它的目标分片数或者该分片属于无效组，则再分配
			if shard_c[curgid] > target_c[curgid] || curgid == 0 {
				//如果当前副本组已分配满，则指向下一个
				for shard_c_list[shard_c_list_i][1] >= target_c[shard_c_list[shard_c_list_i][0]] {
					shard_c_list_i++
				}
				shard_c_list[shard_c_list_i][1]++
				shard_c[curgid]--
				newconfig.Shards[shard_i] = shard_c_list[shard_c_list_i][0]
			}
		}
	} else if op.Optype == 1 {
		shard_c := sc.getshard_c(lastconfig.Shards)
		remain_list := make([][2]int, 0)
		delete_dict := make(map[int]bool)
		for _, gid := range op.GIDs {
			delete_dict[gid] = true
		}
		//添加已有的副本组
		for gid, servers := range lastconfig.Groups {
			_, ok := delete_dict[gid]
			if ok {
				continue
			}
			c := shard_c[gid]
			remain_list = append(remain_list, [2]int{gid, c})
			newconfig.Groups[gid] = servers
		}
		//根据分片数排序
		sort.Slice(remain_list, func(i, j int) bool {
			return remain_list[i][1] < remain_list[j][1] ||
				(remain_list[i][1] == remain_list[j][1] && remain_list[i][0] < remain_list[j][0])
		})
		//副本组数量
		group_c := len(remain_list)
		//每个副本组的目标分片数
		target_c := sc.gettarget_c(group_c, remain_list)
		for remain_list_i, shard_i := 0, 0; shard_i < NShards; shard_i++ {
			curgid := newconfig.Shards[shard_i]
			_, ok := delete_dict[curgid]
			if ok {
				//如果不存在副本组，则全部放入无效组
				if group_c > 0 {
					for remain_list[remain_list_i][1] >= target_c[remain_list[remain_list_i][0]] {
						remain_list_i++
					}
					remain_list[remain_list_i][1]++
					newconfig.Shards[shard_i] = remain_list[remain_list_i][0]
				} else {
					newconfig.Shards[shard_i] = 0
				}
			}
		}
	} else if op.Optype == 2 {
		for gid, servers := range lastconfig.Groups {
			newconfig.Groups[gid] = servers
		}
		newconfig.Shards[op.Shard] = op.GID
	} else if op.Optype == 3 {
		if op.Num == -1 || op.Num >= len(sc.configs) {
			return lastconfig
		} else {
			return sc.configs[op.Num]
		}
	}
	sc.configs = append(sc.configs, newconfig)
	// func(config Config) {
	// 	fmt.Println("config", config.Num)
	// 	for gid, _ := range config.Groups {
	// 		fmt.Println("gid", gid)
	// 	}
	// 	for i, gid := range config.Shards {
	// 		fmt.Println("shard", i, "to", gid)
	// 	}
	// }(newconfig)
	return Config{}
}

func (sc *ShardCtrler) applyCommand() {
	for sc.killed() == false {
		for m := range sc.applyCh {
			if m.CommandValid {
				//类型断言
				op := m.Command.(Op)
				// DPrintf("1 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
				// 	sc.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
				sc.mu.Lock()
				if sc.maxexcuteindex >= m.CommandIndex {
					sc.mu.Unlock()
					continue
				}
				response := sc.applyCommandOnServer(op)
				currentterm, isleader := sc.rf.GetState()
				waitchannel, waitok := sc.waitreply[OpIdentifier{m.CommandIndex, currentterm}]
				sc.maxexcuteindex = int(max(int64(m.CommandIndex), int64(sc.maxexcuteindex)))
				// fmt.Println("me", sc.me, "raftstatesize: ", sc.persister.RaftStateSize())
				// if sc.maxraftstate > -1 && int(float64(sc.maxraftstate)*0.8) < sc.persister.RaftStateSize() {
				// 	sc.Snapshot()
				// }
				if !waitok || !isleader {
					sc.mu.Unlock()
					continue
				}
				waitchannel <- response

				sc.mu.Unlock()
				// DPrintf("11 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
				// 	sc.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
			} else if m.SnapshotValid {
				// sc.mu.Lock()
				// if sc.maxexcuteindex >= m.SnapshotIndex {
				// 	sc.mu.Unlock()
				// 	continue
				// }
				// //更新状态
				// sc.ReadSnapshot(m.Snapshot)
				// sc.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.persister = persister
	sc.waitreply = make(map[OpIdentifier]chan Config)
	sc.ClerklastSeqID = make(map[int64]int64)
	go sc.applyCommand()

	return sc
}
