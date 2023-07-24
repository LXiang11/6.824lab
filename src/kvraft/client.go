package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	currentleader int
	ClerkID       int64
	SeqID         int64
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
	ck.ClerkID = nrand()
	ck.SeqID = 0
	// You'll have to add code here.
	DPrintf("clerk get start!\n")
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	getArgs := GetArgs{key, ck.ClerkID, atomic.AddInt64(&ck.SeqID, 1)}
	getReply := GetReply{}

	for {
		// ck.currentleader = ck.GetLeader()
		DPrintf("get: key: %v, ClerkID: %v, SeqID: %v\n", key, getArgs.ClerkID, getArgs.SeqID)

		ck.servers[ck.currentleader].Call("KVServer.Get", &getArgs, &getReply)

		if getReply.Err == OK {
			return getReply.Value
		}

		ck.currentleader = (ck.currentleader + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putAppendArgs := PutAppendArgs{key, value, op, ck.ClerkID, atomic.AddInt64(&ck.SeqID, 1)}
	putAppendReply := PutAppendReply{}

	for {
		// ck.currentleader = ck.GetLeader()
		DPrintf("putappend: key: %v, value: %v, op: %v, ClerkID: %v, SeqID: %v\n", key, value, op, putAppendArgs.ClerkID, putAppendArgs.SeqID)

		ck.servers[ck.currentleader].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)

		if putAppendReply.Err == OK {
			return
		}

		ck.currentleader = (ck.currentleader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
