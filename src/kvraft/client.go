package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64
	requestId  int64
	lastLeader int
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.lastLeader = 0
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
	reqId := atomic.AddInt64(&ck.requestId, 1)

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	// Try the last known leader first
	for {
		reply := GetReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err == OK {
			return reply.Value
		}

		if ok && reply.Err == ErrNoKey {
			return ""
		}

		// Try next server
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond) // Slightly longer delay for better performance
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
	reqId := atomic.AddInt64(&ck.requestId, 1)

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	// Try the last known leader first
	for {
		reply := PutAppendReply{}
		var ok bool

		if op == "Put" {
			ok = ck.servers[ck.lastLeader].Call("KVServer.Put", &args, &reply)
		} else {
			ok = ck.servers[ck.lastLeader].Call("KVServer.Append", &args, &reply)
		}

		if ok && reply.Err == OK {
			return
		}

		// Try next server
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond) // Slightly longer delay for better performance
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
