package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype  int
	Key     string
	Value   string
	ClerkID int64
	SeqlID  int64
}

type OpIdentifier struct {
	index int
	term  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string

	//每种操作设置一个管道，等待raftapply了这个操作后再执行
	waitreply map[OpIdentifier]chan string

	//raft已执行的最大index
	maxexcuteindex int

	//当前快照的最后一个日志，用于判断哪个快照更新
	// snapshotlastindex int
	// snapshotlastterm  int

	persister *raft.Persister

	//每个命令唯一标识
	ClerklastSeqID map[int64]int64
}

func (kv *KVServer) Snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.store)
	e.Encode(kv.ClerklastSeqID)
	e.Encode(kv.maxexcuteindex)

	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.maxexcuteindex, snapshot)
}

func (kv *KVServer) ReadSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var store map[string]string
	var ClerklastSeqID map[int64]int64
	var maxexcuteindex int
	if d.Decode(&store) != nil ||
		d.Decode(&ClerklastSeqID) != nil ||
		d.Decode(&maxexcuteindex) != nil {
		//   error...
	} else {
		kv.store = store
		kv.ClerklastSeqID = ClerklastSeqID
		kv.maxexcuteindex = maxexcuteindex
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("1 server%v get %v", kv.me, args.Key)
	op := Op{0, args.Key, "", args.ClerkID, args.SeqID}
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
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
		return
	case <-time.After(time.Duration(Timeout)):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("1 server%v putappend %v, value %v", kv.me, args.Key, args.Value)
	var op Op
	if args.Op == "Put" {
		op = Op{1, args.Key, args.Value, args.ClerkID, args.SeqID}
	} else if args.Op == "Append" {
		op = Op{2, args.Key, args.Value, args.ClerkID, args.SeqID}
	}
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
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
		return
	case <-time.After(time.Duration(Timeout)):
		reply.Err = ErrWrongLeader
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyCommandOnServer(op Op) string {
	lastSeqID, ok := kv.ClerklastSeqID[op.ClerkID]
	if ok && lastSeqID >= op.SeqlID && (op.Optype == 1 || op.Optype == 2) {
		return ""
	}
	kv.ClerklastSeqID[op.ClerkID] = max(lastSeqID, op.SeqlID)
	if op.Optype == 0 {
		//"Get"
		value, ok := kv.store[op.Key]
		if !ok {
			value = ""
		}
		return value
	} else if op.Optype == 1 {
		//"Put"
		kv.store[op.Key] = op.Value
		return ""
	} else if op.Optype == 2 {
		//"Append"
		value, ok := kv.store[op.Key]
		if ok {
			op.Value = value + op.Value
		}
		kv.store[op.Key] = op.Value
		return ""
	}
	return ""
}

func (kv *KVServer) applyCommand() {
	for kv.killed() == false {
		for m := range kv.applyCh {
			if m.CommandValid {
				DPrintf("applycmd: key: %v, value: %v, type: %v, me: %v\n",
					m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, kv.me)
				//类型断言
				op := m.Command.(Op)
				// DPrintf("1 server%v applycmd key: %v, value: %v, type: %v, opindex: %v\n",
				// 	kv.me, m.Command.(Op).Key, m.Command.(Op).Value, m.Command.(Op).Optype, m.Command.(Op).Opindex)
				kv.mu.Lock()
				if kv.maxexcuteindex >= m.CommandIndex {
					kv.mu.Unlock()
					continue
				}
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
				// if kv.snapshotlastterm > m.SnapshotTerm {
				// 	kv.mu.Unlock()
				// 	continue
				// } else if kv.snapshotlastterm == m.SnapshotTerm {
				// 	if kv.snapshotlastindex >= m.SnapshotIndex {
				// 		kv.mu.Unlock()
				// 		continue
				// 	}
				// } else
				if kv.maxexcuteindex >= m.SnapshotIndex {
					kv.mu.Unlock()
					continue
				}
				//更新状态
				kv.ReadSnapshot(m.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.waitreply = make(map[OpIdentifier]chan string)
	kv.ClerklastSeqID = make(map[int64]int64)
	kv.persister = persister
	kv.ReadSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applyCommand()

	DPrintf("server %v get start!\n", me)

	return kv
}
