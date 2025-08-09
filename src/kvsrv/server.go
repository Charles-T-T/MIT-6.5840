package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println("--------------------------------------")
		log.Printf(format, a...)
		log.Println("--------------------------------------")
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	db          map[string]string
	lastRequest map[int64]int64    // clerkID -> its last requestID
	lastReply   map[int64]string // clerkID -> its last reply value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.lastRequest[args.ClerkID] == args.RequestID {
		return
	}

	kv.db[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.lastRequest[args.ClerkID] == args.RequestID {
		reply.Value = kv.lastReply[args.ClerkID]
		return
	}

	oldValue := kv.db[args.Key]
	kv.db[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	kv.lastRequest[args.ClerkID] = args.RequestID
	kv.lastReply[args.ClerkID] = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.lastRequest = make(map[int64]int64)
	kv.lastReply = make(map[int64]string)

	return kv
}
