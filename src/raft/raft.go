package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	// "fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

const RPCtimeout = 100

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	timer int32

	heartbeatch        chan int
	electiontimeout    int32
	isheartbeattimeout []bool

	//当前快照最后一个条目的index和term
	snapshotlastindex int
	snapshotlastterm  int

	snapshotdata []byte

	//是否是第一次启动
	isnotfirst bool

	//当前leader是否还存活
	leaderislive bool
	//当前的leader
	currentleader int

	//记录最新的term，初始为0
	currentTerm int
	//candidateid
	votedFor int
	//记录log entry
	log []LogEntry

	//已经被提交的最新log entry index，初始为0
	commitIndex int
	//已经应用到状态机的最新log entry index，初始为0
	lastApplied int

	//对于每一个服务器，下一个要发送给它的log entry index，
	//初始为leader last log entry index+1
	nextIndex []int
	//对于每一个服务器，已知已经被复制到对方服务器上的最新log entry index，
	//初始为0
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.currentleader == rf.me

	return term, isleader
}

func (rf *Raft) GetLastLog() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) > 0 {
		return rf.snapshotlastindex + len(rf.log) + 1, rf.log[len(rf.log)-1].Term
	} else {
		return rf.snapshotlastindex + 1, rf.snapshotlastterm
	}
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex + 1
}

func (rf *Raft) GetSnapShotLast() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.snapshotlastindex + 1, rf.snapshotlastterm
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.snapshotlastindex)
	e.Encode(rf.snapshotlastterm)
	e.Encode(rf.isnotfirst)
	e.Encode(rf.currentTerm)
	e.Encode(rf.currentleader)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshotdata)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshotdata []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	if snapshotdata != nil && len(snapshotdata) >= 1 {
		rf.snapshotdata = snapshotdata
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotlastindex int
	var snapshotlastterm int
	var isnotfirst bool
	var currentTerm int
	var currentleader int
	var votedFor int
	var log []LogEntry
	var commitIndex int
	// var lastApplied int
	var nextIndex []int
	var matchIndex []int
	if d.Decode(&snapshotlastindex) != nil ||
		d.Decode(&snapshotlastterm) != nil ||
		d.Decode(&isnotfirst) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&currentleader) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&commitIndex) != nil ||
		// d.Decode(&lastApplied) != nil ||
		d.Decode(&nextIndex) != nil ||
		d.Decode(&matchIndex) != nil {
		DPrintf("decode error %d %d", rf.me, rf.currentTerm)
		//   error...
	} else {
		rf.snapshotlastindex = snapshotlastindex
		rf.snapshotlastterm = snapshotlastterm
		rf.isnotfirst = isnotfirst
		rf.currentTerm = currentTerm
		rf.currentleader = currentleader
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = commitIndex
		// rf.lastApplied = lastApplied
		rf.nextIndex = nextIndex
		rf.matchIndex = matchIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index--
	if index <= rf.snapshotlastindex {
		return
	}
	DPrintf("%d snapshot leader %d index %d snapshotlastindex %d term %d", rf.me, rf.currentleader, index, rf.snapshotlastindex, rf.currentTerm)
	rf.snapshotdata = snapshot
	rf.snapshotlastterm = rf.log[index-rf.snapshotlastindex-1].Term
	rf.log = rf.log[index-rf.snapshotlastindex:]
	rf.snapshotlastindex = index
	rf.persist()

}

type InstallSnapshotArgs struct {
	// Your data here (2D).
	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offest            int
	Data              []byte
	Done              bool
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	// Your data here (2D).
	Term int
}

type SnapshotFile struct {
	data              []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

func (rf *Raft) applySnapshot(sf SnapshotFile) {
	rf.applyCh <- ApplyMsg{false, nil, 0, true, sf.data, sf.lastIncludedTerm, sf.lastIncludedIndex + 1}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// DPrintf("%d installsnapshot lock", rf.me)
	if args.Term != rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	DPrintf("%d installsnapshot leader %d index %d leader term %d term %d", rf.me, args.Leader, args.LastIncludedIndex+1, args.Term, rf.currentTerm)
	if rf.lastApplied < args.LastIncludedIndex {
		rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
		rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
	}
	if rf.snapshotlastindex >= args.LastIncludedIndex {
		if rf.snapshotlastindex == args.LastIncludedIndex {
			if rf.snapshotlastterm == args.LastIncludedTerm {
				// fmt.Println()
			} else {
				rf.log = rf.log[:0]
			}
		} else {
			rf.log = rf.log[:0]
		}
	} else if rf.snapshotlastindex+len(rf.log) >= args.LastIncludedIndex {
		if rf.log[args.LastIncludedIndex-rf.snapshotlastindex-1].Term == args.LastIncludedTerm {
			rf.log = rf.log[args.LastIncludedIndex-rf.snapshotlastindex:]
		} else {
			rf.log = rf.log[:0]
		}
	} else {
		rf.log = rf.log[:0]
	}

	rf.snapshotdata = args.Data
	rf.snapshotlastindex = args.LastIncludedIndex
	rf.snapshotlastterm = args.LastIncludedTerm
	rf.persist()
	rf.mu.Unlock()
	// DPrintf("%d installsnapshot unlock", rf.me)
	//这里不能在持有锁的状态下写入管道，会与外部应用产生死锁
	rf.applySnapshot(SnapshotFile{args.Data, args.LastIncludedIndex, args.LastIncludedTerm})
	// DPrintf("%d applySnapshot ok", rf.me)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term      int
	Candidate int
	LastIndex int
	LastTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	IsVoted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d recive vote request %d other term %d my term %d", rf.me, args.Candidate, args.Term, rf.currentTerm)
	mylogindex := 0
	otherlogindex := args.LastIndex
	mylogterm := 0
	otherlogterm := args.LastTerm
	otherterm := args.Term
	candidate := args.Candidate
	if len(rf.log) > 0 {
		mylogindex = rf.snapshotlastindex + len(rf.log)
		mylogterm = rf.log[mylogindex-rf.snapshotlastindex-1].Term
	} else {
		mylogindex = rf.snapshotlastindex
		mylogterm = rf.snapshotlastterm
	}
	if mylogterm < otherlogterm {
		reply.IsVoted = true
	} else if mylogterm > otherlogterm {
		reply.IsVoted = false
	} else {
		if mylogindex <= otherlogindex {
			reply.IsVoted = true
		} else {
			reply.IsVoted = false
		}
	}
	//已经投过票或者已过时
	//这个term应该如何更改？
	if rf.currentTerm > otherterm {
		reply.IsVoted = false
		reply.Term = rf.currentTerm
	} else if rf.currentTerm < otherterm {
		rf.currentTerm = otherterm
		rf.setToCandidate()
		rf.votedFor = -1
	}
	//这个判断是有意义的，当收到更新的term请求时，满足投票条件就要投票。如果此term内已经投过票则不投。
	if rf.votedFor != -1 && rf.votedFor != candidate {
		reply.IsVoted = false
	}
	//同一term内只投一票
	if reply.IsVoted {
		DPrintf("%d vote %d mylogterm %d otherlogterm %d term %d", rf.me, candidate, mylogterm, otherlogterm, rf.currentTerm)
		rf.currentTerm = otherterm
		// rf.currentleader = candidate
		//这里应不应该设置为不存活呢
		//不应该，会出错，为什么？
		// rf.leaderislive = false
		rf.currentleader = -1
		rf.votedFor = candidate
		atomic.StoreInt32(&rf.timer, 0)
		rf.heartbeatch <- candidate
	}
	// rf.mu.Unlock()
	// rf.mu.Lock()
	rf.persist()
}

type AppendEntryArgs struct {
	LogEntryValid bool
	Term          int
	Leader        int
	PrevLogIndex  int
	PrevLogTerm   int
	//要被复制的entry
	StoredEntry []LogEntry
	//leadar's commitIndex
	LeaderCommit int
}

type AppendEntryReply struct {
	Term int
	//失败的话，指向follower第一个term为PrevLogTerm的entry
	FirstLogIndex int
	//如果follower包含的entry与prevlogindex和prevlogterm匹配则为true
	Success bool
	//成功的话，返回最后匹配的index
	CompleteIndex int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		if rf.currentTerm != args.Term || rf.currentleader != args.Leader {
			orterm := rf.currentTerm
			rf.currentTerm = args.Term
			rf.currentleader = args.Leader
			rf.leaderislive = true
			atomic.StoreInt32(&rf.timer, 0)
			DPrintf("%d to leader %d from term %d to term %d", rf.me, rf.currentleader, orterm, rf.currentTerm)
		}
	} else {
		reply.Term = rf.currentTerm
		return
	}

	if args.Leader == rf.currentleader {
		rf.leaderislive = true
		rf.heartbeatch <- args.Leader

		if args.LogEntryValid {
			//一切和rf.log相关的代码都要加上偏移
			if args.PrevLogIndex < rf.snapshotlastindex {
				// fmt.Println("PrevLogIndex <= snapshotlastindex", "me", rf.me, "leader", args.Leader)
				reply.FirstLogIndex = rf.snapshotlastindex + 2
				return
			} else if args.PrevLogIndex <= rf.snapshotlastindex+len(rf.log) {
				if args.PrevLogIndex == rf.snapshotlastindex {
					if rf.snapshotlastterm == args.PrevLogTerm {
						// fmt.Println("term same", "me", rf.me, "leader", args.Leader, "term", rf.currentTerm)
						// fmt.Println("aindex", args.PrevLogIndex, "meindex", rf.snapshotlastindex)
						reply.Success = true
						//不能直接append，如果有多个符合条件的请求乱序到达，最新的请求可能会被覆盖，导致丢失log
						curindex := args.PrevLogIndex - rf.snapshotlastindex
						for i := 0; i < len(args.StoredEntry); i++ {
							if curindex+i >= len(rf.log) || rf.log[curindex+i].Term != args.StoredEntry[i].Term {
								rf.log = append(rf.log[:curindex+i], args.StoredEntry[i:]...)
								break
							}
						}
						// rf.log = append(rf.log[:args.PrevLogIndex-rf.snapshotlastindex], args.StoredEntry...)
						DPrintf("%d change log leader %d term %d start %d end %d",
							rf.me, rf.currentleader, rf.currentTerm, args.PrevLogIndex-rf.snapshotlastindex, rf.snapshotlastindex+len(rf.log))
						reply.CompleteIndex = rf.snapshotlastindex + len(rf.log)
					} else {
						// fmt.Println("term not same")
						reply.FirstLogIndex = -1
						return
					}
				} else if rf.log[args.PrevLogIndex-rf.snapshotlastindex-1].Term == args.PrevLogTerm {
					reply.Success = true
					curindex := args.PrevLogIndex - rf.snapshotlastindex
					for i := 0; i < len(args.StoredEntry); i++ {
						if curindex+i >= len(rf.log) || rf.log[curindex+i].Term != args.StoredEntry[i].Term {
							rf.log = append(rf.log[:curindex+i], args.StoredEntry[i:]...)
							break
						}
					}
					// rf.log = append(rf.log[:args.PrevLogIndex-rf.snapshotlastindex], args.StoredEntry...)
					DPrintf("%d change log leader %d term %d start %d end %d",
						rf.me, rf.currentleader, rf.currentTerm, args.PrevLogIndex-rf.snapshotlastindex, rf.snapshotlastindex+len(rf.log))
					reply.CompleteIndex = rf.snapshotlastindex + len(rf.log)
					// fmt.Println("append from", args.Leader, "last comand", rf.log[len(rf.log)-1].Command, "last term", rf.log[len(rf.log)-1].Term, "me", rf.me, "leader", rf.currentleader)
				} else {
					// fmt.Println("remove from", args.Leader, "command", rf.log[args.PrevLogIndex].Command, "term", rf.log[args.PrevLogIndex].Term, "me", rf.me, "leader", rf.currentleader)
					for i := args.PrevLogIndex - 1; i > rf.snapshotlastindex; i-- {
						if rf.log[i-rf.snapshotlastindex-1].Term != rf.log[i-rf.snapshotlastindex].Term {
							reply.FirstLogIndex = i + 1
							break
						}
					}
					// DPrintf("%d append fail reply index %d term %d", rf.me, reply.FirstLogIndex, rf.log[reply.FirstLogIndex].Term)
					//这里会导致丢失最后一个日志，不应该-1
					// rf.log = rf.log[:args.PrevLogIndex-rf.snapshotlastindex-1]
					return
				}
			} else {
				reply.FirstLogIndex = rf.snapshotlastindex + len(rf.log) + 1
				return
			}
		}
		//日志必须与leader一致时才提交
		// fmt.Println("me", rf.me, "comi", rf.commitIndex, "leader comi", args.LeaderCommit, "leader", args.Leader, len(rf.log))
		if rf.commitIndex < args.LeaderCommit && args.LeaderCommit < rf.snapshotlastindex+len(rf.log)+1 && rf.log[args.LeaderCommit-rf.snapshotlastindex-1].Term == args.Term {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)+rf.snapshotlastindex)
			DPrintf("%d commit index %d argcommit %d leader %d", rf.me, rf.commitIndex+1, args.LeaderCommit+1, rf.currentleader)
		}
	}
	// rf.mu.Unlock()
	// rf.mu.Lock()
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if server == rf.me {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	if server == rf.me {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	if !ok {
		rf.isheartbeattimeout[server] = true
		return ok
	}

	rf.isheartbeattimeout[server] = false

	if args.LogEntryValid {
		rf.mu.Lock()
		if reply.Success {
			if rf.nextIndex[server] < reply.CompleteIndex+1 {
				rf.nextIndex[server] = reply.CompleteIndex + 1
			}
			if rf.matchIndex[server] < reply.CompleteIndex {
				rf.matchIndex[server] = reply.CompleteIndex
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.setToCandidate()
			} else {
				if reply.FirstLogIndex > 0 {
					rf.nextIndex[server] = reply.FirstLogIndex - 1
				} else {
					rf.nextIndex[server] = 0
				}
			}
		}
		rf.persist()
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.setToCandidate()
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if server == rf.me {
		return false
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) setToCandidate() {
	rf.currentleader = -1
	rf.leaderislive = false
	// rf.mu.Unlock()
	// rf.mu.Lock()
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//等待新的选举触发
	// time.Sleep(time.Duration(300) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.currentleader == rf.me

	// Your code here (2B).

	if !isLeader {
		return index, term, isLeader
	}
	DPrintf("%d start command %v", rf.me, command)

	// fmt.Println("append entry", command, "term", rf.currentTerm, "me", rf.me)
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	index = rf.snapshotlastindex + len(rf.log) + 1
	term = rf.currentTerm
	// rf.mu.Unlock()
	// rf.mu.Lock()
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderislive = false
	rf.currentleader = -1
	//关闭会导致向已关闭管道写入恐慌
	// close(rf.heartbeatch)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initleader() {
	for i := range rf.peers {
		// if i == rf.me {continue}
		rf.nextIndex[i] = len(rf.log) + rf.snapshotlastindex + 1
		rf.matchIndex[i] = -1
	}

	//外层已锁
	// rf.mu.Lock()
	rf.currentleader = rf.me
	rf.leaderislive = true
	// rf.mu.Unlock()

	//发送心跳
	//leader期间周期发送
	for i := range rf.peers {
		if i == rf.me {
			// go func() {
			// 	for ; rf.currentleader == rf.me; time.Sleep(time.Duration(100) * time.Millisecond) {
			// 		rf.heartbeatch <- rf.me
			// 	}
			// }()
			continue
		}
		go func(server int) {
			for rf.currentleader == rf.me {
				rf.mu.Lock()
				if rf.currentleader != rf.me {
					rf.mu.Unlock()
					break
				}
				appendEntryArgs := AppendEntryArgs{false, rf.currentTerm, rf.me, 0, 0, nil, rf.commitIndex}
				appendEntryReply := AppendEntryReply{rf.currentTerm, 0, false, 0}
				rf.mu.Unlock()

				go func(appendEntryArgs AppendEntryArgs, appendEntryReply AppendEntryReply) {
					rf.sendAppendEntry(server, &appendEntryArgs, &appendEntryReply)
				}(appendEntryArgs, appendEntryReply)

				// fmt.Println(rf.me, "send heartbeat to", server, "term", rf.currentTerm)
				//由于RPC超时时间设置的太长，可能导致旧leader根本无法知道自己处于少数区，所以当发现过半都超时时，应该认为自己处于少数区，转为candidate
				// heartbeattimeoutch := make(chan bool)
				// go func() {
				// 	if rf.sendAppendEntry(server, &appendEntryArgs, &appendEntryReply) {
				// 		heartbeattimeoutch <- true
				// 	}
				// 	close(heartbeattimeoutch)
				// }()
				// select {
				// case <-heartbeattimeoutch:
				// 	rf.isheartbeattimeout[server] = false
				// case <-time.After(time.Duration(RPCtimeout) * time.Millisecond):
				// 	rf.isheartbeattimeout[server] = true
				// }
				// rf.mu.Lock()
				// if appendEntryReply.Term > rf.currentTerm {
				// 	rf.currentTerm = appendEntryReply.Term
				// 	rf.setToCandidate()
				// }
				// rf.mu.Unlock()
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
		}(i)
	}
	//检查是否过半心跳超时
	//不能检查，否则会导致leader无法自己维持自己的leader身份
	// go func() {
	// 	for ; rf.currentleader == rf.me; time.Sleep(time.Duration(100) * time.Millisecond) {
	// 		rf.mu.Lock()
	// 		if rf.currentleader != rf.me {
	// 			rf.mu.Unlock()
	// 			break
	// 		}
	// 		count := 0
	// 		for i, v := range rf.isheartbeattimeout {
	// 			if i == rf.me {
	// 				continue
	// 			}
	// 			if v {
	// 				count++
	// 			}
	// 		}
	// 		if count > len(rf.peers)/2 {
	// 			DPrintf("%d 过半心跳超时", rf.me)
	// 			rf.setToCandidate()
	// 		}
	// 		rf.mu.Unlock()
	// 	}
	// }()
	go func() {
		for rf.currentleader == rf.me {
			rf.mu.Lock()
			//检查提交
			if rf.currentleader != rf.me {
				rf.mu.Unlock()
				break
			}
			num := make([]int, len(rf.peers))
			for i := range rf.peers {
				if i == rf.me {
					num[i] = rf.snapshotlastindex + len(rf.log)
					continue
				}
				num[i] = rf.matchIndex[i]
			}
			sort.Ints(num)
			if cmt := num[len(rf.peers)/2]; cmt > rf.commitIndex && rf.log[cmt-rf.snapshotlastindex-1].Term == rf.currentTerm {
				rf.commitIndex = cmt
				rf.persist()
				DPrintf("%d commit index %d", rf.me, rf.commitIndex+1)
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}()
	//发送最新log entry
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//并行发送
		go func(server int) {
			for rf.currentleader == rf.me {
				rf.mu.Lock()
				if rf.currentleader != rf.me {
					rf.mu.Unlock()
					break
				}
				if rf.snapshotlastindex+len(rf.log) < rf.nextIndex[server] {
					rf.mu.Unlock()
					time.Sleep(time.Duration(10) * time.Millisecond)
					continue
				}
				//如果需要发送的日志已被快照，则先发送快照
				if rf.nextIndex[server] <= rf.snapshotlastindex {
					installSnapshotArgs := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapshotlastindex, rf.snapshotlastterm,
						0, rf.snapshotdata, true}
					installSnapshotReply := InstallSnapshotReply{rf.currentTerm}
					rf.mu.Unlock()
					InstallSnapshottimeoutch := make(chan bool)
					go func() {
						if rf.sendInstallSnapshot(server, &installSnapshotArgs, &installSnapshotReply) {
							InstallSnapshottimeoutch <- true
						}
						close(InstallSnapshottimeoutch)
					}()
					select {
					case <-InstallSnapshottimeoutch:

					case <-time.After(time.Duration(RPCtimeout) * time.Millisecond):
						continue
					}
					rf.mu.Lock()
					rf.nextIndex[server] = rf.snapshotlastindex + 1
				}
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				if rf.nextIndex[server] >= rf.snapshotlastindex+2 {
					prevLogTerm = rf.log[rf.nextIndex[server]-rf.snapshotlastindex-2].Term
				} else if rf.nextIndex[server] == rf.snapshotlastindex+1 {
					prevLogTerm = rf.snapshotlastterm
				}
				curindex := len(rf.log) + rf.snapshotlastindex
				// for i := rf.nextIndex[server] + 1; i < len(rf.log); i++ {
				// 	if rf.log[i].Term != rf.log[i-1].Term {
				// 		curindex = i - 1
				// 	}
				// }
				appendEntryArgs := AppendEntryArgs{true, rf.currentTerm,
					rf.me, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[server]-rf.snapshotlastindex-1 : curindex-rf.snapshotlastindex],
					rf.commitIndex}
				appendEntryReply := AppendEntryReply{rf.currentTerm, 0, false, 0}
				rf.mu.Unlock()

				go func(appendEntryArgs AppendEntryArgs, appendEntryReply AppendEntryReply) {
					rf.sendAppendEntry(server, &appendEntryArgs, &appendEntryReply)
				}(appendEntryArgs, appendEntryReply)

				// AppendEntrytimeoutch := make(chan bool)
				// go func() {
				// 	if rf.sendAppendEntry(server, &appendEntryArgs, &appendEntryReply) {
				// 		AppendEntrytimeoutch <- true
				// 	}
				// 	close(AppendEntrytimeoutch)
				// }()
				// select {
				// case <-AppendEntrytimeoutch:

				// case <-time.After(time.Duration(RPCtimeout) * time.Millisecond):
				// 	continue
				// }
				// rf.mu.Lock()
				// if appendEntryReply.Success {
				// 	if rf.nextIndex[server] < curindex+1 {
				// 		rf.nextIndex[server] = curindex + 1
				// 	}
				// 	if rf.matchIndex[server] < curindex {
				// 		rf.matchIndex[server] = curindex
				// 	}
				// } else {
				// 	if appendEntryReply.Term > rf.currentTerm {
				// 		rf.currentTerm = appendEntryReply.Term
				// 		rf.setToCandidate()
				// 	} else {
				// 		if appendEntryReply.FirstLogIndex <= rf.snapshotlastindex {
				// 			installSnapshotArgs := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapshotlastindex, rf.snapshotlastterm,
				// 				0, rf.snapshotdata, true}
				// 			installSnapshotReply := InstallSnapshotReply{rf.currentTerm}
				// 			rf.mu.Unlock()
				// 			InstallSnapshottimeoutch := make(chan bool)
				// 			go func() {
				// 				if rf.sendInstallSnapshot(server, &installSnapshotArgs, &installSnapshotReply) {
				// 					InstallSnapshottimeoutch <- true
				// 				}
				// 				close(InstallSnapshottimeoutch)
				// 			}()
				// 			select {
				// 			case <-InstallSnapshottimeoutch:

				// 			case <-time.After(time.Duration(RPCtimeout) * time.Millisecond):
				// 				continue
				// 			}
				// 			rf.mu.Lock()
				// 			rf.nextIndex[server] = rf.snapshotlastindex + 1
				// 		} else if appendEntryReply.FirstLogIndex > 0 {
				// 			rf.nextIndex[server] = appendEntryReply.FirstLogIndex - 1
				// 		} else {
				// 			rf.nextIndex[server] = 0
				// 		}
				//	}
				//如果需要发送快照，则等待快照发送完毕再继续执行
				// }
				// rf.persist()
				// rf.mu.Unlock()
				time.Sleep(time.Duration(10) * time.Millisecond)
			}
		}(i)
	}
	rf.persist()
}

type applylogentry struct {
	Entry LogEntry
	Index int
}

func (rf *Raft) applylog() {
	for rf.killed() == false {
		applylog_list := make([]applylogentry, 0)
		rf.mu.Lock()
		lastApplied, commitIndex := rf.lastApplied, rf.commitIndex
		for lastApplied < commitIndex {
			lastApplied++
			// DPrintf("%d append logentry to list index %d command %v", rf.me, lastApplied-rf.snapshotlastindex-1, rf.log[lastApplied-rf.snapshotlastindex-1])
			applylog_list = append(applylog_list, applylogentry{rf.log[lastApplied-rf.snapshotlastindex-1], lastApplied + 1})
		}
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.persist()
		rf.mu.Unlock()
		for _, curlog := range applylog_list {
			//如果在执行过程中间有一个snapshot被安装，则放弃本轮执行
			if curlog.Index <= rf.snapshotlastindex+1 {
				continue
			}
			DPrintf("%d apply index %d", rf.me, curlog.Index)
			// fmt.Println("have applied  rf.me:", rf.me, "lastapplied", lastApplied, "rf.lastapplied", rf.lastApplied, "rf.term", rf.currentTerm)
			rf.applyCh <- ApplyMsg{true, curlog.Entry.Command, curlog.Index, false, nil, 0, 0}
		}
		// rf.mu.Lock()
		// rf.lastApplied = max(rf.lastApplied, commitIndex)
		// rf.persist()
		// rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) election(curTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if curTerm != rf.currentTerm {
		// fmt.Println(rf.me, "no election term", rf.currentTerm)
		return
	}

	rf.currentleader = -1
	rf.votedFor = rf.me
	rf.currentTerm++
	DPrintf("%d start election term %d", rf.me, rf.currentTerm)

	go func(count int, curterm int) {
		var cmu sync.Mutex
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				if rf.currentleader != -1 || curterm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				myindex := rf.snapshotlastindex
				lastterm := rf.snapshotlastterm
				if len(rf.log) > 0 {
					myindex = len(rf.log) + rf.snapshotlastindex
					lastterm = rf.log[myindex-rf.snapshotlastindex-1].Term
				}
				requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, myindex, lastterm}
				requestVoteReply := RequestVoteReply{rf.currentTerm, false}
				rf.mu.Unlock()
				if !rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply) {
					return
				}
				if !requestVoteReply.IsVoted {
					rf.mu.Lock()
					if requestVoteReply.Term > rf.currentTerm {
						rf.currentTerm = requestVoteReply.Term
						rf.setToCandidate()
					}
					rf.mu.Unlock()
					return
				}
				cmu.Lock()
				count++
				cmu.Unlock()
			}(i)
		}
		for rf.currentleader == -1 && curterm == rf.currentTerm {
			rf.mu.Lock()
			//投票数过半且我还没成为leader则开始发送心跳
			if count > len(rf.peers)/2 && rf.currentleader == -1 && curterm == rf.currentTerm {
				DPrintf("%d start leader term %d", rf.me, rf.currentTerm)
				rf.initleader()
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(20) * time.Millisecond)
		}
	}(1, rf.currentTerm)
	rf.persist()
}

func (rf *Raft) ticker1() {
	// rf.lastsystime = time.Now()
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// if rf.currentleader == rf.me {
		// 	time.Sleep(time.Duration(100) * time.Millisecond)
		// 	continue
		// }

		//接收到心跳则重新开始计时
		select {
		case <-rf.heartbeatch:
			// now := time.Now()
			// fmt.Println(rf.me, "get heartbeat from", getfrom, "term", rf.currentTerm, "sub from last", now.UnixMilli()-rf.lastsystime.UnixMilli())
			// rf.lastsystime = now
		case <-time.After(time.Duration(rf.electiontimeout) * time.Millisecond):
			//自身为leader时不要发起选举
			if rf.currentleader != rf.me {
				DPrintf("%d timeout term %d leader %d", rf.me, rf.currentTerm, rf.currentleader)
				//如果触发超时，则先重置下一次超时的时间
				atomic.StoreInt32(&rf.electiontimeout, 400+(rand.Int31()%400))
				go rf.election(rf.currentTerm)
			}
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//如果当前leader还存活，则不进行新的投票
		if rf.leaderislive {
			if rf.currentleader != rf.me {
				// rf.mu.Lock()
				rf.leaderislive = false
				// rf.mu.Unlock()
			}
			// fmt.Println("leaderislive", "term", rf.currentTerm, "leader", rf.currentleader, "me", rf.me)
			// rf.mu.Unlock()

			// time.Sleep(300 * time.Millisecond)
			// rf.mu.Lock()
			// rf.mu.Unlock()
		} else {
			// rf.votecount = 1
			rf.votedFor = rf.me
			rf.currentleader = -1
			rf.currentTerm++
			// curterm := rf.currentTerm
			// count := 1
			// fmt.Println("send vote", "term", rf.currentTerm, "me", rf.me)
			// rf.mu.Unlock()
			// rf.persist()

			// fmt.Println("leaderalive", rf.leaderislive, "votedfor", rf.votedFor, "me", rf.me, "term", rf.currentTerm, "curleader", rf.currentleader)

			//重新设置选举超时

			go func(count int, curterm int) {
				var cmu sync.Mutex
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(server int) {
						rf.mu.Lock()
						if rf.currentleader != -1 || curterm != rf.currentTerm {
							rf.mu.Unlock()
							return
						}
						myindex := rf.snapshotlastindex
						lastterm := rf.snapshotlastterm
						if len(rf.log) > 0 {
							myindex = len(rf.log) + rf.snapshotlastindex
							lastterm = rf.log[myindex-rf.snapshotlastindex-1].Term
						}
						requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, myindex, lastterm}
						requestVoteReply := RequestVoteReply{rf.currentTerm, false}
						rf.mu.Unlock()
						if !rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply) {
							return
						}
						if !requestVoteReply.IsVoted {
							rf.mu.Lock()
							if requestVoteReply.Term > rf.currentTerm {
								rf.currentTerm = requestVoteReply.Term
								rf.setToCandidate()
							}
							rf.mu.Unlock()
							return
						}
						cmu.Lock()
						count++
						cmu.Unlock()
					}(i)
				}
				// ms := 50 + (rand.Int63() % 300)
				// for i := 0; i < int(ms) && rf.currentleader == -1; i++ {
				// 	rf.mu.Lock()
				// 	if count > len(rf.peers)/2 && rf.currentleader == -1 && curterm == rf.currentTerm {
				// 		// fmt.Println("start leader", rf.me)
				// 		rf.initleader()
				// 	}
				// 	rf.mu.Unlock()
				// 	time.Sleep(time.Duration(1) * time.Millisecond)
				// }
				for rf.currentleader == -1 && curterm == rf.currentTerm {
					rf.mu.Lock()
					//投票数过半且我还没成为leader则开始发送心跳
					if count > len(rf.peers)/2 && rf.currentleader == -1 && curterm == rf.currentTerm {
						// fmt.Println("start leader", rf.me)
						rf.initleader()
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
			}(1, rf.currentTerm)
		}
		rf.mu.Unlock()
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 500 + (rand.Int63() % 150)
		// ms := 50 + rand.Int63()%200
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		for atomic.StoreInt32(&rf.timer, 0); rf.timer < int32(ms); atomic.AddInt32(&rf.timer, 1) {
			time.Sleep(time.Millisecond)
		}
		//不应该置为-1，可能会出现该term投多次票的情况
		// rf.mu.Lock()
		// if rf.votedFor != -1 {
		// 	rf.votedFor = -1
		// }
		// rf.persist()
		// rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.heartbeatch = make(chan int, 100)
	rf.isheartbeattimeout = make([]bool, len(rf.peers))
	// rf.nextIndex = make([]int, len(rf.peers))
	// rf.matchIndex = make([]int, len(rf.peers))
	// rf.commitIndex = -1
	// rf.lastApplied = -1
	// rf.votedFor = -1
	// //防止0当选时，无法开始发送心跳
	// rf.currentleader = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.lastApplied = rf.snapshotlastindex
	// rf.currentleader = -1

	if !rf.isnotfirst {
		rf.isnotfirst = true
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.snapshotlastindex = -1
		// rf.snapshotlastterm = -1
		rf.commitIndex = -1
		rf.lastApplied = -1
		rf.votedFor = -1
		//防止0当选时，无法开始发送心跳
		rf.currentleader = -1
		rf.persist()
	}
	rf.mu.Unlock()
	// rf.mu.Lock()
	// rf.currentleader = -1
	// rf.mu.Unlock()

	// start ticker goroutine to start elections
	// go rf.ticker()
	DPrintf("%d restart snapshotlastindex %d loglen %d", rf.me, rf.snapshotlastindex+1, len(rf.log))
	go rf.ticker1()
	go rf.applylog()

	return rf
}
