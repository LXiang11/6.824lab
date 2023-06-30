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
	"fmt"
	"math"
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

	timer int

	//是否是第一次启动
	isnotfirst bool

	//投票计数
	votecount int

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

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.currentleader == rf.me

	return term, isleader
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

	e.Encode(rf.isnotfirst)
	e.Encode(rf.currentTerm)
	e.Encode(rf.currentleader)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var isnotfirst bool
	var currentTerm int
	var currentleader int
	var votedFor int
	var log []LogEntry
	var commitIndex int
	var lastApplied int
	var nextIndex []int
	var matchIndex []int
	if d.Decode(&isnotfirst) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&currentleader) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&nextIndex) != nil ||
		d.Decode(&matchIndex) != nil {
		fmt.Println("decode error", rf.me, rf.currentTerm)
		//   error...
	} else {
		rf.isnotfirst = isnotfirst
		rf.currentTerm = currentTerm
		rf.currentleader = currentleader
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
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
	// fmt.Println("recive", args.Candidate, "other term", args.Term, "my term", rf.currentTerm, "me", rf.me)
	mylogindex := 0
	otherlogindex := args.LastIndex
	mylogterm := 0
	otherlogterm := args.LastTerm
	otherterm := args.Term
	candidate := args.Candidate
	if len(rf.log) > 0 {
		mylogindex = len(rf.log) - 1
		mylogterm = rf.log[mylogindex].Term
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
	if rf.currentTerm >= otherterm {
		reply.IsVoted = false
		reply.Term = rf.currentTerm
	} else if rf.currentTerm < otherterm {
		rf.currentTerm = otherterm
		rf.votedFor = -1
	}
	if rf.votedFor != -1 {
		reply.IsVoted = false
	}
	//同一term内只投一票
	if reply.IsVoted {
		// fmt.Println("vote", candidate, "mylogterm", mylogterm, "otherlogterm", otherlogterm, "term", rf.currentTerm, "me", rf.me)
		// rf.mu.Lock()
		rf.currentTerm = otherterm
		// rf.currentleader = candidate
		// rf.leaderislive = true
		rf.currentleader = -1
		rf.votedFor = candidate
		rf.timer = 0
		// rf.mu.Unlock()
	}
	rf.mu.Unlock()
	rf.mu.Lock()
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
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// || len(rf.log) == 0 || (len(rf.log) > 0 && args.Term >= rf.log[len(rf.log)-1].Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		// rf.mu.Lock()
		if rf.currentTerm != args.Term || rf.currentleader != args.Leader {
			// orterm := rf.currentTerm
			rf.currentTerm = args.Term
			rf.currentleader = args.Leader
			rf.leaderislive = true
			// fmt.Println("to leader", rf.currentleader, "to term", rf.currentTerm, "yuan term", orterm, "me", rf.me)
			// rf.persist()
		}
		// rf.mu.Unlock()
	} else {
		reply.Term = rf.currentTerm
		return
	}

	if args.Leader == rf.currentleader {
		// rf.mu.Lock()
		rf.leaderislive = true
		// if rf.votedFor != -1 {
		// 	rf.votedFor = -1
		// 	// rf.persist()
		// }
		// rf.mu.Unlock()
		// fmt.Println("append", rf.me, args.LogEntryValid)

		//出现过一个问题，已经提交的命令，没有出现在follower的日志中，导致follower投票给了另一个日志不包含这个已提交命令的candidate
		if args.LogEntryValid {
			if args.PrevLogIndex < len(rf.log) {
				if args.PrevLogIndex == -1 || rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
					reply.Success = true
					// rf.mu.Lock()
					rf.log = append(rf.log[:args.PrevLogIndex+1], args.StoredEntry...)
					// fmt.Println("append from", args.Leader, "last comand", rf.log[len(rf.log)-1].Command, "last term", rf.log[len(rf.log)-1].Term, "me", rf.me, "leader", rf.currentleader)
					// rf.persist()
					// rf.mu.Unlock()
				} else {
					// rf.mu.Lock()
					// fmt.Println("remove from", args.Leader, "command", rf.log[args.PrevLogIndex].Command, "term", rf.log[args.PrevLogIndex].Term, "me", rf.me, "leader", rf.currentleader)
					for i := args.PrevLogIndex - 1; i >= 0; i-- {
						if rf.log[i].Term != rf.log[i+1].Term {
							reply.FirstLogIndex = i + 1
							break
						}
					}
					rf.log = rf.log[:args.PrevLogIndex]
					// rf.persist()
					// rf.mu.Unlock()
					// reply.Term = rf.currentTerm
					return
				}
			} else {
				reply.FirstLogIndex = len(rf.log)
				return
			}
		}
		//日志必须与leader一致时才提交
		//
		// fmt.Println("me", rf.me, "comi", rf.commitIndex, "leader comi", args.LeaderCommit, len(rf.log))
		// rf.mu.Lock()
		if rf.commitIndex < args.LeaderCommit && args.LeaderCommit < len(rf.log) && rf.log[args.LeaderCommit].Term == args.Term {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
			// for rf.commitIndex < int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1))) {
			// }
			// rf.persist()
		}
		// rf.mu.Unlock()
	}
	rf.mu.Unlock()
	rf.mu.Lock()
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
	return ok
}

func (rf *Raft) setToCandidate() {
	rf.currentleader = -1
	rf.leaderislive = false
	rf.mu.Unlock()
	rf.mu.Lock()
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
	// fmt.Println("start command", command, "me", rf.me)

	// fmt.Println("append entry", command, "term", rf.currentTerm, "me", rf.me)
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	index = len(rf.log)
	term = rf.currentTerm
	rf.mu.Unlock()
	rf.mu.Lock()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initleader() {
	for i := range rf.peers {
		// if i == rf.me {continue}
		rf.nextIndex[i] = len(rf.log)
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
				appendEntryReply := AppendEntryReply{rf.currentTerm, 0, false}
				rf.mu.Unlock()
				rf.sendAppendEntry(server, &appendEntryArgs, &appendEntryReply)
				rf.mu.Lock()
				if appendEntryReply.Term > rf.currentTerm {
					rf.currentTerm = appendEntryReply.Term
					rf.setToCandidate()
				}
				rf.mu.Unlock()
				time.Sleep(time.Duration(40) * time.Millisecond)
			}
		}(i)
	}
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
					num[i] = len(rf.log) - 1
					continue
				}
				num[i] = rf.matchIndex[i]
			}
			sort.Ints(num)
			if cmt := num[len(rf.peers)/2]; cmt > rf.commitIndex {
				rf.commitIndex = cmt
			}
			// for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
			// 	count := 1
			// 	for j := range rf.peers {
			// 		if j != rf.me && rf.matchIndex[j] >= i {
			// 			count++
			// 		}
			// 	}
			// 	//
			// 	if rf.log[i].Term != rf.currentTerm {
			// 		break
			// 	}
			// 	if count <= len(rf.peers)/2 {
			// 		continue
			// 	}
			// 	rf.commitIndex = i
			// 	rf.mu.Unlock()
			// 	rf.mu.Lock()
			// 	rf.persist()
			// 	break
			// }
			rf.persist()
			rf.mu.Unlock()
			time.Sleep(time.Duration(40) * time.Millisecond)
		}
	}()
	//发送最新log entry
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//并行发送
		go func(server int) {
			for ; rf.currentleader == rf.me; time.Sleep(time.Duration(10) * time.Millisecond) {
				rf.mu.Lock()
				if rf.currentleader != rf.me {
					rf.mu.Unlock()
					break
				}
				if len(rf.log) <= rf.nextIndex[server] {
					rf.mu.Unlock()
					continue
				}
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				if rf.nextIndex[server] > 0 {
					prevLogTerm = rf.log[rf.nextIndex[server]-1].Term
				}
				curindex := len(rf.log) - 1
				// for i := rf.nextIndex[server] + 1; i < len(rf.log); i++ {
				// 	if rf.log[i].Term != rf.log[i-1].Term {
				// 		curindex = i - 1
				// 	}
				// }
				appendEntryArgs := AppendEntryArgs{true, rf.currentTerm,
					rf.me, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[server] : curindex+1],
					rf.commitIndex}
				appendEntryReply := AppendEntryReply{rf.currentTerm, 0, false}
				rf.mu.Unlock()
				// if curindex >= rf.nextIndex[server] {
				if !rf.sendAppendEntry(server, &appendEntryArgs, &appendEntryReply) {
					continue
				}
				// } else {
				// 	continue
				// }
				rf.mu.Lock()
				if appendEntryReply.Success {
					if rf.nextIndex[server] < curindex+1 {
						rf.nextIndex[server] = curindex + 1
					}
					if rf.matchIndex[server] < curindex {
						rf.matchIndex[server] = curindex
					}
				} else {
					if appendEntryReply.Term > rf.currentTerm {
						rf.currentTerm = appendEntryReply.Term
						rf.setToCandidate()
					} else {
						if appendEntryReply.FirstLogIndex > 0 {
							rf.nextIndex[server] = appendEntryReply.FirstLogIndex - 1
						} else {
							rf.nextIndex[server] = 0
						}
						// rf.nextIndex[server]--
						//没删干净就append了
						// rf.matchIndex[server] = rf.nextIndex[server]
					}
				}
				rf.mu.Unlock()
				rf.mu.Lock()
				rf.persist()
				rf.mu.Unlock()
			}
		}(i)
	}
	rf.mu.Unlock()
	rf.mu.Lock()
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
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applylog_list = append(applylog_list, applylogentry{rf.log[rf.lastApplied], rf.lastApplied + 1})
		}
		rf.persist()
		rf.mu.Unlock()
		for _, curlog := range applylog_list {
			// fmt.Println("have commited  rf.me:", rf.me, "rf.comindex", rf.commitIndex, rf.log[rf.commitIndex].Command, "rf.term", rf.currentTerm)
			rf.applyCh <- ApplyMsg{true, curlog.Entry.Command, curlog.Index, false, nil, 0, 0}
		}
		time.Sleep(time.Duration(40) * time.Millisecond)
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
			rf.mu.Unlock()

			time.Sleep(300 * time.Millisecond)
			rf.mu.Lock()
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
						myindex := 0
						lastterm := 0
						if len(rf.log) > 0 {
							myindex = len(rf.log) - 1
							lastterm = rf.log[myindex].Term
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
					}
					rf.mu.Unlock()
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
			}(1, rf.currentTerm)
		}
		// rf.mu.Lock()rf.mu.Unlock()
		rf.mu.Unlock()
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 300 + (rand.Int63() % 150)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		for rf.timer = 0; rf.timer < int(ms); rf.timer++ {
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
	rf.readPersist(persister.ReadRaftState())
	// rf.currentleader = -1
	rf.mu.Unlock()

	if !rf.isnotfirst {
		rf.mu.Lock()
		rf.isnotfirst = true
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.commitIndex = -1
		rf.lastApplied = -1
		rf.votedFor = -1
		//防止0当选时，无法开始发送心跳
		rf.currentleader = -1
		rf.persist()
		rf.mu.Unlock()
	}
	// rf.mu.Lock()
	// rf.currentleader = -1
	// rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applylog()

	return rf
}
