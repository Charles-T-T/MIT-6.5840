package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false
const Log2File = false

var logFile *os.File

func init() {
	if Debug && Log2File {
		var err error
		logFile, err = os.OpenFile("debug.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("failed to open log file: %v", err)
		}
		log.SetOutput(logFile)
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Println(strings.Repeat("-", 100))
		log.Printf(format, a...)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // "Get", "Put", "Append"
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type OpResult struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore        map[string]string        // key-value store
	lastApplied    map[int64]int64          // clientId -> last applied requestId
	waitingClients map[int]chan OpResult    // index -> channel for waiting clients
	lastAppliedIdx int                      // last applied log index
	duplicateTable map[int64]string         // clientId -> last result for Get operations
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// Check if this request has already been processed
	if lastReqId, exists := kv.lastApplied[args.ClientId]; exists && lastReqId >= args.RequestId {
		if cachedResult, ok := kv.duplicateTable[args.ClientId]; ok && args.RequestId == lastReqId {
			reply.Value = cachedResult
		} else {
			reply.Value = kv.kvStore[args.Key]
		}
		reply.Err = OK
		DPrintf("KV[%d] Get duplicate request C[%d] R[%d] key=%s value=%s", kv.me, args.ClientId, args.RequestId, args.Key, reply.Value)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("KV[%d] Get new request C[%d] R[%d] key=%s", kv.me, args.ClientId, args.RequestId, args.Key)

	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("KV[%d] Get not leader C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
		return
	}

	DPrintf("KV[%d] Get started C[%d] R[%d] index=%d term=%d", kv.me, args.ClientId, args.RequestId, index, term)

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitingClients[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value = result.Value
		reply.Err = result.Err
		DPrintf("KV[%d] Get completed C[%d] R[%d] value=%s", kv.me, args.ClientId, args.RequestId, reply.Value)
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("KV[%d] Get timeout C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
	}

	// Clean up
	kv.mu.Lock()
	delete(kv.waitingClients, index)
	// Check if we're still leader
	currentTerm, stillLeader := kv.rf.GetState()
	if !stillLeader || currentTerm != term {
		reply.Err = ErrWrongLeader
		DPrintf("KV[%d] Get leader changed C[%d] R[%d] term=%d->%d leader=%v", kv.me, args.ClientId, args.RequestId, term, currentTerm, stillLeader)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// Check if this request has already been processed
	if lastReqId, exists := kv.lastApplied[args.ClientId]; exists && lastReqId >= args.RequestId {
		reply.Err = OK
		DPrintf("KV[%d] Put duplicate request C[%d] R[%d] key=%s value=%s", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("KV[%d] Put new request C[%d] R[%d] key=%s value=%s", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)

	op := Op{
		Type:      "Put",
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("KV[%d] Put not leader C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
		return
	}

	DPrintf("KV[%d] Put started C[%d] R[%d] index=%d term=%d", kv.me, args.ClientId, args.RequestId, index, term)

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitingClients[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
		DPrintf("KV[%d] Put completed C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("KV[%d] Put timeout C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
	}

	// Clean up
	kv.mu.Lock()
	delete(kv.waitingClients, index)
	// Check if we're still leader
	currentTerm, stillLeader := kv.rf.GetState()
	if !stillLeader || currentTerm != term {
		reply.Err = ErrWrongLeader
		DPrintf("KV[%d] Put leader changed C[%d] R[%d] term=%d->%d leader=%v", kv.me, args.ClientId, args.RequestId, term, currentTerm, stillLeader)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// Check if this request has already been processed
	if lastReqId, exists := kv.lastApplied[args.ClientId]; exists && lastReqId >= args.RequestId {
		reply.Err = OK
		DPrintf("KV[%d] Append duplicate request C[%d] R[%d] key=%s value=%s", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("KV[%d] Append new request C[%d] R[%d] key=%s value=%s", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)

	op := Op{
		Type:      "Append",
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("KV[%d] Append not leader C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
		return
	}

	DPrintf("KV[%d] Append started C[%d] R[%d] index=%d term=%d", kv.me, args.ClientId, args.RequestId, index, term)

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitingClients[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
		DPrintf("KV[%d] Append completed C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("KV[%d] Append timeout C[%d] R[%d]", kv.me, args.ClientId, args.RequestId)
	}

	// Clean up
	kv.mu.Lock()
	delete(kv.waitingClients, index)
	// Check if we're still leader
	currentTerm, stillLeader := kv.rf.GetState()
	if !stillLeader || currentTerm != term {
		reply.Err = ErrWrongLeader
		DPrintf("KV[%d] Append leader changed C[%d] R[%d] term=%d->%d leader=%v", kv.me, args.ClientId, args.RequestId, term, currentTerm, stillLeader)
	}
	kv.mu.Unlock()
}

// Apply operations from Raft log
func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		
		if applyMsg.CommandValid {
			kv.mu.Lock()
			
			// Ensure we apply commands in order
			if applyMsg.CommandIndex <= kv.lastAppliedIdx {
				DPrintf("KV[%d] duplicate apply index=%d lastApplied=%d", kv.me, applyMsg.CommandIndex, kv.lastAppliedIdx)
				kv.mu.Unlock()
				continue
			}
			
			op := applyMsg.Command.(Op)
			result := OpResult{}
			
			DPrintf("KV[%d] applying op: %+v at index=%d", kv.me, op, applyMsg.CommandIndex)
			
			// Check if we've already applied this request
			if lastReqId, exists := kv.lastApplied[op.ClientId]; !exists || lastReqId < op.RequestId {
				// Apply the operation
				switch op.Type {
				case "Get":
					result.Value = kv.kvStore[op.Key]
					result.Err = OK
					kv.duplicateTable[op.ClientId] = result.Value
					DPrintf("KV[%d] applied Get C[%d] R[%d] key=%s value=%s", kv.me, op.ClientId, op.RequestId, op.Key, result.Value)
				case "Put":
					oldValue := kv.kvStore[op.Key]
					kv.kvStore[op.Key] = op.Value
					result.Err = OK
					DPrintf("KV[%d] applied Put C[%d] R[%d] key=%s oldValue=%s newValue=%s", kv.me, op.ClientId, op.RequestId, op.Key, oldValue, op.Value)
				case "Append":
					oldValue := kv.kvStore[op.Key]
					kv.kvStore[op.Key] += op.Value
					result.Err = OK
					DPrintf("KV[%d] applied Append C[%d] R[%d] key=%s oldValue=%s appendValue=%s newValue=%s", kv.me, op.ClientId, op.RequestId, op.Key, oldValue, op.Value, kv.kvStore[op.Key])
				}
				kv.lastApplied[op.ClientId] = op.RequestId
			} else {
				// Already applied - for Get operations, still return the cached value
				if op.Type == "Get" {
					if cached, ok := kv.duplicateTable[op.ClientId]; ok {
						result.Value = cached
					} else {
						result.Value = kv.kvStore[op.Key]
					}
				}
				result.Err = OK
				DPrintf("KV[%d] duplicate apply C[%d] R[%d] lastApplied=%d", kv.me, op.ClientId, op.RequestId, lastReqId)
			}
			
			kv.lastAppliedIdx = applyMsg.CommandIndex
			
			// Notify waiting client if any
			if ch, exists := kv.waitingClients[applyMsg.CommandIndex]; exists {
				select {
				case ch <- result:
				default:
				}
			}
			
			kv.mu.Unlock()
		}
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
	kv.kvStore = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.waitingClients = make(map[int]chan OpResult)
	kv.duplicateTable = make(map[int64]string)
	kv.lastAppliedIdx = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyLoop()

	return kv
}