package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const Timeout = 1 * time.Second

func max(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

type KV struct {
	Key   string
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype OpType
	Key    string
	Value  string
	//用于分片传输
	Term         int
	ToGid        int
	Shard        int
	TheShard2KVs []KV
	ToState      ShardState
	LastCfg      shardctrler.Config
	CSIDs        []int64
	ClerkID      int64
	SeqlID       int64
}

type OpIdentifier struct {
	index int
	term  int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32 // set by Kill()

	// store map[string]string

	//每种操作设置一个管道，等待raftapply了这个操作后再执行
	waitreply map[OpIdentifier]chan string

	//raft已执行的最大index
	maxexcuteindex int

	persister *raft.Persister

	mck *shardctrler.Clerk

	//分片下的key
	shard2key [shardctrler.NShards]map[string]string

	//0:不属于我  1:可服务  2:待push  3:可push 4:待获取
	shardmanager [shardctrler.NShards]ShardState

	//shard属于哪个gid，用于push
	shard2gid [shardctrler.NShards]int

	//key对应分片
	// key2shard map[string]int

	//gid->si
	leadercache map[int]int

	lastcfg shardctrler.Config

	//每个命令唯一标识
	ClerklastSeqID map[int64]int64

	//作为客户端向其他服务器推送消息需要“精确一次”
	ClerkID int64
	SeqID   int64
}

func (kv *ShardKV) Snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.shard2key)
	e.Encode(kv.ClerklastSeqID)
	e.Encode(kv.maxexcuteindex)
	e.Encode(kv.shardmanager)
	e.Encode(kv.shard2gid)
	e.Encode(kv.lastcfg)

	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.maxexcuteindex, snapshot)
}

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var store [shardctrler.NShards]map[string]string
	var ClerklastSeqID map[int64]int64
	var maxexcuteindex int
	var shardmanager [shardctrler.NShards]ShardState
	var shard2gid [shardctrler.NShards]int
	var lastcfg shardctrler.Config
	if d.Decode(&store) != nil ||
		d.Decode(&ClerklastSeqID) != nil ||
		d.Decode(&maxexcuteindex) != nil ||
		d.Decode(&shardmanager) != nil ||
		d.Decode(&shard2gid) != nil ||
		d.Decode(&lastcfg) != nil {
		//   error...
	} else {
		kv.shard2key = store
		kv.ClerklastSeqID = ClerklastSeqID
		kv.maxexcuteindex = maxexcuteindex
		kv.shardmanager = shardmanager
		kv.shard2gid = shard2gid
		kv.lastcfg = lastcfg
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// op := Op{0, args.Key, "", 0, 0, args.Shard, nil, 0, shardctrler.Config{}, nil, args.ClerkID, args.SeqID}
	op := Op{
		Optype:  Get,
		Key:     args.Key,
		Shard:   args.Shard,
		ClerkID: args.ClerkID,
		SeqlID:  args.SeqID,
	}
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.lastcfg.Num != args.Num || kv.shardmanager[args.Shard] != Hold {
		reply.Err = ErrWrongGroup
		DPrintf("gid %v me %d get shard %v wrong state %v", kv.gid, kv.me, args.Shard, kv.shardmanager[args.Shard])
		kv.mu.Unlock()
		return
	}
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("gid %v me %v get key %v shard %d", kv.gid, kv.me, args.Key, args.Shard)
	// kv.mu.Lock()
	// if kv.lastcfg.Num != args.Num || kv.shardmanager[args.Shard] != 1 {
	// 	reply.Err = ErrWrongGroup
	// 	kv.mu.Unlock()
	// 	return
	// }
	response := make(chan string)
	kv.waitreply[OpIdentifier{index, term}] = response
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitreply, OpIdentifier{index, term})
		kv.mu.Unlock()
		close(response)
	}()

	select {
	case value := <-response:
		reply.Err = OK
		reply.Value = value
		DPrintf("gid %d me %d get key %v shard %d success", kv.gid, kv.me, args.Key, args.Shard)
		return
	case <-time.After(time.Duration(Timeout)):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	if args.Op == "Put" {
		// op = Op{1, args.Key, args.Value, 0, 0, args.Shard, nil, 0, shardctrler.Config{}, nil, args.ClerkID, args.SeqID}
		op = Op{
			Optype:  Put,
			Key:     args.Key,
			Value:   args.Value,
			Shard:   args.Shard,
			ClerkID: args.ClerkID,
			SeqlID:  args.SeqID,
		}
	} else if args.Op == "Append" {
		// op = Op{2, args.Key, args.Value, 0, 0, args.Shard, nil, 0, shardctrler.Config{}, nil, args.ClerkID, args.SeqID}
		op = Op{
			Optype:  Append,
			Key:     args.Key,
			Value:   args.Value,
			Shard:   args.Shard,
			ClerkID: args.ClerkID,
			SeqlID:  args.SeqID,
		}
	}
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.lastcfg.Num != args.Num || kv.shardmanager[args.Shard] != Hold {
		reply.Err = ErrWrongGroup
		DPrintf("gid %v me %d putappend shard %d wrong state %v cfgnum %v", kv.gid, kv.me, args.Shard, kv.shardmanager[args.Shard], kv.lastcfg.Num)
		kv.mu.Unlock()
		return
	}
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("gid %v me %v putappend key %v shard %d", kv.gid, kv.me, args.Key, args.Shard)
	// kv.mu.Lock()
	// if kv.lastcfg.Num != args.Num || kv.shardmanager[args.Shard] != 1 {
	// 	reply.Err = ErrWrongGroup
	// 	kv.mu.Unlock()
	// 	return
	// }
	response := make(chan string)
	kv.waitreply[OpIdentifier{index, term}] = response
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitreply, OpIdentifier{index, term})
		kv.mu.Unlock()
		close(response)
	}()

	select {
	case <-response:
		reply.Err = OK
		DPrintf("gid %v me %d putappend key %v shard %d success", kv.gid, kv.me, args.Key, args.Shard)
		return
	case <-time.After(time.Duration(Timeout)):
		reply.Err = ErrWrongLeader
		return
	}
}

type PushKeyArgs struct {
	Num   int
	Shard int
	KVs   []KV
	CSIDs []int64
}

type PushKeyReply struct {
	HavePush bool
	Isleader bool
	NumLess  bool
}

func KVMap2Slice(mp map[string]string) []KV {
	ans := make([]KV, 0)
	for k, v := range mp {
		ans = append(ans, KV{k, v})
	}
	return ans
}

func KVSlice2Map(kvs []KV) map[string]string {
	ans := make(map[string]string)
	for _, kv := range kvs {
		ans[kv.Key] = kv.Value
	}
	return ans
}

func CSIDMap2Slice(mp map[int64]int64) []int64 {
	ans := make([]int64, 0)
	for ClerkID, SeqID := range mp {
		ans = append(ans, ClerkID, SeqID)
	}
	return ans
}

func (kv *ShardKV) PushShard() {
	for ; kv.killed() == false; time.Sleep(100 * time.Millisecond) {
		kv.mu.Lock()
		if _, isleader := kv.rf.GetState(); !isleader {
			kv.mu.Unlock()
			continue
		}
		for i, state := range kv.shardmanager {
			if kv.killed() == true {
				kv.mu.Unlock()
				return
			}
			if _, isleader := kv.rf.GetState(); !isleader {
				break
			}
			DPrintf("gid %d me %d try push shard %d state %d cfgnum %d", kv.gid, kv.me, i, state, kv.lastcfg.Num)
			if state == Push {
				KVs := KVMap2Slice(kv.shard2key[i])
				CSIDs := CSIDMap2Slice(kv.ClerklastSeqID)
				args := PushKeyArgs{kv.lastcfg.Num, i, KVs, CSIDs}
				curgid := kv.shard2gid[i]
				servers := kv.lastcfg.Groups[curgid]
				// 对集群每个服务器询问，找到leader，push分片内容
				for si := kv.leadercache[curgid]; si < len(servers) && kv.killed() == false; si = (si + 1) % len(servers) {
					if _, isleader := kv.rf.GetState(); !isleader {
						break
					}
					srv := kv.make_end(servers[si])
					var reply PushKeyReply
					kv.mu.Unlock()
					GetShardTimeoutch := make(chan bool)
					go func(srv *labrpc.ClientEnd, GetShardTimeoutch chan bool, args *PushKeyArgs, reply *PushKeyReply) {
						if srv.Call("ShardKV.GetShard", args, reply) {
							GetShardTimeoutch <- true
						}
						close(GetShardTimeoutch)
					}(srv, GetShardTimeoutch, &args, &reply)
					select {
					case <-GetShardTimeoutch:
						DPrintf("gid %d me %d send push shard %d to gid %d server %d reply %v", kv.gid, kv.me, i, curgid, si, reply)
					case <-time.After(time.Duration(100) * time.Millisecond):
						kv.mu.Lock()
						continue
					}
					kv.mu.Lock()
					//如果还处于恢复状态，能不能删除呢
					//应该不能，假设对于一个shard在两个push出去的配置之间插入了删除，就会出错
					// DPrintf("gid %d me %d push shard reply %v", kv.gid, kv.me, reply)
					//当配置小于对方时证明分片已经发过去了，需要删除（此时对方已经无法回复确认）
					if reply.HavePush || reply.NumLess {
						//删除分片
						// kv.rf.Start(Op{3, "", "", 0, 0, i, nil, 0, shardctrler.Config{kv.lastcfg.Num, kv.lastcfg.Shards, nil}, nil, 0, 0})
						index, term, isleader := kv.rf.Start(Op{
							Optype:  ShardStateChange,
							Shard:   i,
							ToState: NotHold,
							LastCfg: kv.lastcfg,
						})
						if isleader {
							DPrintf("gid %d me %d start delete shard %d index %d term %d rf commmit %d cfgnum %d", kv.gid, kv.me, i, index, term, kv.rf.GetCommitIndex(), kv.lastcfg.Num)
						}
						break
					} else if reply.Isleader {
						// DPrintf("gid %d me %d push shard leader %d", kv.gid, kv.me, si)
						kv.leadercache[curgid] = si
						break
					}
				}
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) GetShard(args *PushKeyArgs, reply *PushKeyReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer DPrintf("gid %d me %d getshard reply %v", kv.gid, kv.me, reply)
	_, reply.Isleader = kv.rf.GetState()
	if !reply.Isleader {
		return
	}
	if args.Num < kv.lastcfg.Num {
		reply.NumLess = true
		return
	} else if args.Num > kv.lastcfg.Num {
		return
	} else if kv.shardmanager[args.Shard] == Hold {
		reply.HavePush = true
		return
	} else if kv.shardmanager[args.Shard] != WaitGet {
		return
	}
	DPrintf("gid %d me %d get shard %d", kv.gid, kv.me, args.Shard)
	// _, _, isleader := kv.rf.Start(Op{4, "", "", 0, 0, args.Shard, args.KVs, 1, shardctrler.Config{kv.lastcfg.Num, kv.lastcfg.Shards, nil}, args.CSIDs, 0, 0})
	index, term, isleader := kv.rf.Start(Op{
		Optype:       AccpetShard,
		Shard:        args.Shard,
		TheShard2KVs: args.KVs,
		ToState:      Hold,
		LastCfg:      kv.lastcfg,
		CSIDs:        args.CSIDs,
	})
	if isleader {
		DPrintf("gid %d me %d start AccpetShard index %d term %d rf commit %d cfgnum %d", kv.gid, kv.me, index, term, kv.rf.GetCommitIndex(), kv.lastcfg.Num)
	}
	// reply.Isleader = isleader
}

func (kv *ShardKV) cfgisok() bool {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.lastcfg.Shards[i] == kv.gid {
			if kv.shardmanager[i] != Hold {
				return false
			}
		} else {
			if kv.shardmanager[i] != NotHold {
				return false
			}
		}
	}
	return true
}

func (kv *ShardKV) ConfigChangeDetect() {
	for ; kv.killed() == false; time.Sleep(100 * time.Millisecond) {
		if _, isleader := kv.rf.GetState(); !isleader {
			continue
		}
		kv.mu.Lock()
		curconfig := kv.lastcfg
		//判断当前cfg是否已处理完
		if kv.cfgisok() {
			DPrintf("gid %d me %d cfg %v isok", kv.gid, kv.me, kv.lastcfg.Num)
			curnum := kv.lastcfg.Num
			//需要释放锁，避免rpc超时导致其他服务停滞
			kv.mu.Unlock()
			curconfig = kv.mck.Query(curnum + 1)
			kv.mu.Lock()
		} else {
			DPrintf("gid %d me %d cfg %v isnotok", kv.gid, kv.me, kv.lastcfg)
		}

		//分片包含哪些key由客户端指定，服务端需要缓存key属于哪个分片，以及当前所维护的分片有哪些，
		//当检测到与最新配置不一致，则将不属于自己的发送出去，自己不再对此分片进行操作，但仍需保留此分片数据因为如果网络失败，则需要重新push，
		//等待其他副本组发送属于自己的分片
		//检测到配置更新，先写入raft，等到该日志被提交再执行
		//方案一：对方接收到push的key时，写入raft，push调用成功，我方就可以删除数据，因为该数据已到达对方，即使等到写入时配置再次更新，也只是再次
		//方案二：push依然写入raft，但是不必再返回成功，每个server为分片设置计时器，超时自动删除（不好，如果push一直不成功，等到删除就丢失key了）
		//每个服务器都要查看配置，及时更改自己对分片的控制，避免此时leader下线，新任leader仍对某个不属于它的分片提供服务（因为push命令还没到达）

		//配置更改也需要用raft同步
		if kv.lastcfg.Num < curconfig.Num {
			// for i, gid := range kv.lastcfg.Shards {
			//检测到新配置的那一刻就要对不属于自己的分片停止服务
			// if (kv.shardmanager[i] == 1 || kv.shardmanager[i] == 2) && gid != kv.gid {
			// 	//标记为待push
			// 	if kv.shardmanager[i] == 1 {
			// 		kv.shardmanager[i] = 2
			// 	}
			// }
			// }
			// kv.rf.Start(Op{3, "", "", 0, 0, 0, nil, 5, curconfig, nil, 0, 0})
			index, term, isleader := kv.rf.Start(Op{
				Optype:  ConfigChange,
				LastCfg: curconfig,
			})
			if isleader {
				DPrintf("gid %d me %d start ConfigChange to %d index %d rf commit %d term %d", kv.gid, kv.me, curconfig.Num, index, kv.rf.GetCommitIndex(), term)
			}
			kv.mu.Unlock()
			continue
		}
		if kv.lastcfg.Num == curconfig.Num {
			for i, gid := range kv.lastcfg.Shards {
				//此分片处于可服务状态并且需要转移
				//有可能出现所有服务器都处于待push，但是接收不到最新term的可push命令
				if (kv.shardmanager[i] == Hold || kv.shardmanager[i] == WaitPush) && gid != kv.gid {
					//（同时通过raft同步所有机器将该分片变为待push状态，因为别的机器可能配置更新慢，导致先得到转为可push命令）
					//不需要这个也可以，因为只有可服务的分片才会变为待push，如果已经变为可push，就不会回到待push

					//需要立刻丢弃分片所有权，防止在此之后还接收客户端请求
					//有可能日志写入失败，最后丢失数据，需要持续到执行push才结束
					//添加分片管理，分片管理有三个状态，待push（查询到配置变更，所有服务器立刻改为待push），可push（leader轮询待push分片，
					//通过raft同步所有机器变为可push），可服务。需要leader轮询可push分片，直到push成功，通过raft同步删除。
					//这样保证所有机器都是按照待push->可push->删除顺序执行，有可能有重复删除，忽略即可。
					//对于所有的可push分片持续推送，直到接收到对方的push成功则用raft同步删除本地存储（push和删除都由leader执行）
					term, _ := kv.rf.GetState()
					// op := Op{3, "", "", term, gid, i, nil, 3, shardctrler.Config{kv.lastcfg.Num, kv.lastcfg.Shards, nil}, nil, 0, 0}
					op := Op{
						Optype:  ShardStateChange,
						Term:    term,
						ToGid:   gid,
						Shard:   i,
						ToState: Push,
						LastCfg: kv.lastcfg,
					}
					kv.rf.Start(op)
					//标记为待push
					if kv.shardmanager[i] == Hold {
						kv.shardmanager[i] = WaitPush
					}
				}
				//如果是配置1可以直接转为可服务
				if gid == kv.gid && kv.shardmanager[i] != Hold {
					if curconfig.Num == 1 {
						// kv.rf.Start(Op{3, "", "", 0, 0, i, nil, 1, shardctrler.Config{kv.lastcfg.Num, kv.lastcfg.Shards, nil}, nil, 0, 0})
						kv.rf.Start(Op{
							Optype:  ShardStateChange,
							Shard:   i,
							ToState: Hold,
							LastCfg: kv.lastcfg,
						})
						// kv.shardmanager[i] = 1
						// DPrintf("gid %v shard %v to state 1", kv.gid, i)
					} else if kv.shardmanager[i] != WaitGet {
						// kv.rf.Start(Op{3, "", "", 0, 0, i, nil, 4, shardctrler.Config{kv.lastcfg.Num, kv.lastcfg.Shards, nil}, nil, 0, 0})
						kv.rf.Start(Op{
							Optype:  ShardStateChange,
							Shard:   i,
							ToState: WaitGet,
							LastCfg: kv.lastcfg,
						})
						// kv.shardmanager[i] = 4
					}
				}
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyCommandOnServer(op Op) string {
	if op.Optype == ShardStateChange {
		DPrintf("gid %v me %v shardstatechange shard %d to %d cfgnum %d", kv.gid, kv.me, op.Shard, op.ToState, op.LastCfg.Num)
	}
	if op.Optype == AccpetShard {
		DPrintf("gid %v me %v AccpetShard %d cfgnum %d", kv.gid, kv.me, op.Shard, op.LastCfg.Num)
	}
	if op.Optype == ConfigChange {
		//shard state change
		//都需要做幂等处理
		//更换配置
		if kv.lastcfg.Num+1 == op.LastCfg.Num {
			DPrintf("gid %v me %d change cfg to %v", kv.gid, kv.me, op.LastCfg.Num)
			kv.lastcfg = op.LastCfg
		}
		return ""
	} else if op.Optype == ShardStateChange {
		//如果有不是当前配置的指令，则忽略
		if op.LastCfg.Num != kv.lastcfg.Num {
			return ""
		}
		if op.ToState == NotHold {
			//标记为不属于我，删除对应分片
			if kv.shardmanager[op.Shard] == Push {
				// if op.Shard == 8 {
				// 	fmt.Println("gid", kv.gid, "server", kv.me, "Num", kv.lastcfg.Num, "delete shard", op.Shard, kv.shard2key[op.Shard])
				// }
				DPrintf("gid %d me %d delete shard %d", kv.gid, kv.me, op.Shard)
				kv.shardmanager[op.Shard] = NotHold
				kv.shard2key[op.Shard] = make(map[string]string)
				// kv.shard2gid[op.Shard] = 0
			}
		} else if op.ToState == Hold {
			//标记为可服务
			if (kv.shardmanager[op.Shard] == NotHold && kv.lastcfg.Num == 1) || kv.shardmanager[op.Shard] == WaitGet {
				DPrintf("gid %d me %d hold shard %d", kv.gid, kv.me, op.Shard)
				kv.shardmanager[op.Shard] = Hold
			}
		} else if op.ToState == Push {
			//标记为可push，但是不处于可服务以及待push的分片不能更改标记（有可能已经push完且更改为不属于状态），
			//如果不是此时term的命令也要拒绝，因为如果发生了leader切换，新leader还没转为待push，仍然在为上一个配置的分片服务，
			//那么此时转换为可push就会导致push的数据不完全，需要等待这一轮term的可push转换命令才能转为可push
			// term, _ := kv.rf.GetState()
			// && term == op.Term
			if kv.shardmanager[op.Shard] == Hold || kv.shardmanager[op.Shard] == WaitPush {
				DPrintf("gid %d me %d push shard %d", kv.gid, kv.me, op.Shard)
				kv.shardmanager[op.Shard] = Push
				//告知push对象
				kv.shard2gid[op.Shard] = op.ToGid
			}
		} else if op.ToState == WaitGet {
			//标记为待接收
			if kv.shardmanager[op.Shard] == NotHold {
				DPrintf("gid %d me %d waitget shard %d", kv.gid, kv.me, op.Shard)
				kv.shardmanager[op.Shard] = WaitGet
			}
		}
	} else if op.Optype == AccpetShard {
		if op.LastCfg.Num != kv.lastcfg.Num {
			return ""
		}
		//接收分片，如果分片不处于待接收则拒绝，转换为可服务
		if kv.shardmanager[op.Shard] == WaitGet {
			//更新Clerk的lastseqlId
			kv.shard2key[op.Shard] = KVSlice2Map(op.TheShard2KVs)
			for i := 0; i < len(op.CSIDs); i += 2 {
				lastSeqID := kv.ClerklastSeqID[op.CSIDs[i]]
				kv.ClerklastSeqID[op.CSIDs[i]] = max(lastSeqID, op.CSIDs[i+1])
			}
			DPrintf("gid %d me %d accpet shard %d", kv.gid, kv.me, op.Shard)
			kv.shardmanager[op.Shard] = Hold
			kv.shard2gid[op.Shard] = kv.gid
		}
		return ""
	}

	lastSeqID, ok := kv.ClerklastSeqID[op.ClerkID]
	if ok && lastSeqID >= op.SeqlID && (op.Optype == Put || op.Optype == Append) {
		return ""
	}
	kv.ClerklastSeqID[op.ClerkID] = max(lastSeqID, op.SeqlID)

	//拒绝不属于所在副本组的分片请求
	if op.Optype == Get {
		//"Get"
		value, ok := kv.shard2key[op.Shard][op.Key]
		if !ok {
			value = ""
		}
		return value
	} else if op.Optype == Put {
		//"Put"
		kv.shard2key[op.Shard][op.Key] = op.Value
		return ""
	} else if op.Optype == Append {
		//"Append"
		// if op.Key == "0" && kv.me == 0 {
		// 	fmt.Println("gid", kv.gid, "server", kv.me, "append value", op.Value)
		// }
		value, ok := kv.shard2key[op.Shard][op.Key]
		if ok {
			op.Value = value + op.Value
		}
		kv.shard2key[op.Shard][op.Key] = op.Value
		return ""
	}
	return ""
}

func (kv *ShardKV) applyCommand() {
	for kv.killed() == false {
		select {
		case m := <-kv.applyCh:
			if m.CommandValid {
				// DPrintf("applycmd: key: %v, value: %v, type: %v, me: %v\n",
				// 	m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, kv.me)
				//类型断言
				op := m.Command.(Op)
				// DPrintf("1 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
				// 	kv.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
				kv.mu.Lock()
				//不能过滤
				// if kv.maxexcuteindex >= m.CommandIndex {
				// 	kv.mu.Unlock()
				// 	continue
				// }
				//现在有个新问题，分片转移时，出现了重复执行
				//缓存溢出！
				response := kv.applyCommandOnServer(op)
				currentterm, isleader := kv.rf.GetState()
				waitchannel, waitok := kv.waitreply[OpIdentifier{m.CommandIndex, currentterm}]
				kv.maxexcuteindex = int(max(int64(m.CommandIndex), int64(kv.maxexcuteindex)))
				// fmt.Println("me", kv.me, "raftstatesize: ", kv.persister.RaftStateSize())
				if kv.maxraftstate > -1 && int(float64(kv.maxraftstate)*0.8) < kv.persister.RaftStateSize() {
					kv.Snapshot()
				}
				if !waitok || !isleader {
					kv.mu.Unlock()
					continue
				}
				waitchannel <- response

				kv.mu.Unlock()
				// DPrintf("11 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
				// 	kv.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
			} else if m.SnapshotValid {
				kv.mu.Lock()
				// if kv.maxexcuteindex >= m.SnapshotIndex {
				// 	kv.mu.Unlock()
				// 	continue
				// }
				DPrintf("gid %d me %d readsnapshot", kv.gid, kv.me)
				//更新状态
				kv.ReadSnapshot(m.Snapshot)
				kv.mu.Unlock()
			}
		}
		// for m := range kv.applyCh {
		// 	if m.CommandValid {
		// 		// DPrintf("applycmd: key: %v, value: %v, type: %v, me: %v\n",
		// 		// 	m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, kv.me)
		// 		//类型断言
		// 		op := m.Command.(Op)
		// 		// DPrintf("1 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
		// 		// 	kv.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
		// 		kv.mu.Lock()
		// 		if kv.maxexcuteindex >= m.CommandIndex {
		// 			kv.mu.Unlock()
		// 			continue
		// 		}
		// 		//现在有个新问题，分片转移时，出现了重复执行
		// 		//缓存溢出！
		// 		response := kv.applyCommandOnServer(op)
		// 		currentterm, isleader := kv.rf.GetState()
		// 		waitchannel, waitok := kv.waitreply[OpIdentifier{m.CommandIndex, currentterm}]
		// 		kv.maxexcuteindex = int(max(int64(m.CommandIndex), int64(kv.maxexcuteindex)))
		// 		// fmt.Println("me", kv.me, "raftstatesize: ", kv.persister.RaftStateSize())
		// 		if kv.maxraftstate > -1 && int(float64(kv.maxraftstate)*0.8) < kv.persister.RaftStateSize() {
		// 			kv.Snapshot()
		// 		}
		// 		if !waitok || !isleader {
		// 			kv.mu.Unlock()
		// 			continue
		// 		}
		// 		waitchannel <- response

		// 		kv.mu.Unlock()
		// 		// DPrintf("11 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
		// 		// 	kv.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
		// 	} else if m.SnapshotValid {
		// 		kv.mu.Lock()
		// 		if kv.maxexcuteindex >= m.SnapshotIndex {
		// 			kv.mu.Unlock()
		// 			continue
		// 		}
		// 		//更新状态
		// 		kv.ReadSnapshot(m.Snapshot)
		// 		kv.mu.Unlock()
		// 	}
		// }
	}
}

func (kv *ShardKV) NopWorker() {
	for ; kv.killed() == false; time.Sleep(time.Duration(100) * time.Millisecond) {
		kv.mu.Lock()
		term, isleader := kv.rf.GetState()
		snapshotlastindex, snapshotlastterm := kv.rf.GetSnapShotLast()
		lastindex, lastterm := kv.rf.GetLastLog()
		kv.mu.Unlock()
		if !isleader {
			continue
		}
		if term != lastterm {
			index, term, _ := kv.rf.Start(Op{
				Optype: Nop,
			})
			DPrintf("gid %d me %d start Nop index %d term %d", kv.gid, kv.me, index, term)
		}
		DPrintf("gid %d me %d commit %d snapshotlast index %d term %d lastlog index %d term %d", kv.gid, kv.me,
			kv.rf.GetCommitIndex(), snapshotlastindex, snapshotlastterm, lastindex, lastterm)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shard2key[i] = make(map[string]string)
	}
	kv.waitreply = make(map[OpIdentifier]chan string)
	kv.ClerklastSeqID = make(map[int64]int64)
	kv.persister = persister

	kv.leadercache = make(map[int]int)
	kv.lastcfg = shardctrler.Config{}
	kv.ReadSnapshot(persister.ReadSnapshot())
	DPrintf("gid %d me %d start", kv.gid, kv.me)

	// You may need initialization code here.
	go kv.applyCommand()
	go kv.ConfigChangeDetect()
	go kv.PushShard()
	go kv.NopWorker()

	return kv
}
