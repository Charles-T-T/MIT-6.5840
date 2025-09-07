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
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

var heartbeatInterval int64 = 70 // ms

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	isLeader          bool
	state             StateType
	lastHeartbeatTime time.Time
	applyCh           chan ApplyMsg

	// Persistent state
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	CurSnapshot       []byte
	LastIncludedIndex int // last log's Index in the snapshot
	LastIncludedTerm  int // last log's Term in the snapshot

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.isLeader
}

// translate LogEntry's Index to its position in log[]
func (rf *Raft) index2Pos(index int) (bool, int) {
	if index <= rf.LastIncludedIndex {
		return false, -1 // compacted in snapshot
	}
	pos := index - (rf.LastIncludedIndex + 1)
	if pos < 0 || pos >= len(rf.Log) {
		return false, -1
	}
	return true, pos
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	l := len(rf.Log)
	if l > 0 {
		return rf.Log[l-1].Term, rf.Log[l-1].Index
	} else {
		return rf.LastIncludedTerm, rf.LastIncludedIndex
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.CurSnapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		// DPrintf("R[%d_%d] readPersist, no data to restore.\n", rf.me, rf.CurrentTerm)
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("R[%d] readPersist decode error", rf.me)
	}

	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Log = logEntries
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm

	// Read snapshot
	snapshot := rf.persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rf.CurSnapshot = snapshot
	}

	// Truncate the log before the snapshot
	newLog := make([]LogEntry, 0)
	for _, entry := range rf.Log {
		if entry.Index > rf.LastIncludedIndex {
			newLog = append(newLog, entry)
		}
	}
	rf.Log = newLog

	// Update CID and AID
	rf.commitIndex = max(rf.commitIndex, rf.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.LastIncludedIndex)

	DPrintf("R[%d] readPersist done: term=%d, votedFor=%d, lastIncluded=(Term:%d,Index:%d), log=%v\n",
		rf.me, rf.CurrentTerm, rf.VotedFor, rf.LastIncludedTerm, rf.LastIncludedIndex, rf.Log)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		_, lastLogIndex := rf.lastLogTermIndex()
		index := lastLogIndex + 1
		rf.Log = append(rf.Log, LogEntry{Term: rf.CurrentTerm, Index: index, Command: command})
		rf.persist()
		DPrintf("Leader R[%d_%d] get new log: %v, lastInclude=(Term:%d, Id:%d), now log[]: %+v\n",
			rf.me, rf.CurrentTerm, rf.Log[len(rf.Log)-1], rf.LastIncludedTerm, rf.LastIncludedIndex, rf.Log)

		for peer := range rf.peers {
			if peer != rf.me {
				go rf.raiseAppendEntries(peer)
			}
		}
		return index, rf.CurrentTerm, true
	} else {
		return -1, -1, false
	}
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

	// Your initialization code here (3A, 3B, 3C).
	rf.lastHeartbeatTime = time.Now()
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 0, 128)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCh = applyCh
	DPrintf("R[%d_%d] is now online.\n", rf.me, rf.CurrentTerm)

	// start ticker goroutine to start elections
	go rf.electTicker()

	// start ticker goroutine to start apply committed log
	go rf.applyTicker()

	return rf
}
