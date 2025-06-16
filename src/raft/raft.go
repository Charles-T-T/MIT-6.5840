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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

var heartbeatTimeout int64 = 85 // ms

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

	isLeader         bool
	receiveHeartbeat bool
	applyCh          chan ApplyMsg

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

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
	return rf.currentTerm, rf.isLeader
}

// Safe functions of Raft variables with lock
func (rf *Raft) GetHeartBeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.receiveHeartbeat
}

func (rf *Raft) SetHeartbeat(v bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.receiveHeartbeat = v
}

func (rf *Raft) SetIsLeader(v bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isLeader = v

	if v { // newly becomes the leader
		// Reinitialize nextIndex[] and matchIndex[]
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			if len(rf.log) == 0 {
				rf.nextIndex[i] = 1
			} else {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
			}
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) SetTerm(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = v
}

func (rf *Raft) SafeLogf(logFn func() string) {
	if Debug {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("%s", logFn())
	}
}

// translate LogEntry's Index to its position in log[]
func (rf *Raft) index2Pos(index int) (bool, int) {
	if index == 0 {
		return false, 0
	}

	ok := false
	pos := -1
	for i, log := range rf.log {
		if log.Index == index {
			pos = i
			ok = true
			break
		}
	}
	return ok, pos
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	time.Sleep(60 * time.Millisecond) // a time to receive possible heartbeat
	rf.mu.Lock()
	term, isLeader = rf.currentTerm, rf.isLeader
	if isLeader {
		if len(rf.log) == 0 {
			index = 1
		} else {
			index = rf.log[len(rf.log)-1].Index + 1
		}

		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Index: index, Command: command})
		DPrintf("Leader R[%d_%d] get new log: %v, now log[]: %+v\n", rf.me, term, rf.log[len(rf.log)-1], rf.log)

		for peer := range rf.peers {
			if peer != rf.me {
				go rf.raiseAppendEntries(peer)
			}
		}
	}
	rf.mu.Unlock()

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

// Leader sends heartbeat to other peers
func (rf *Raft) broadHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// go rf.raiseAppendEntries(i)
		go func(peer int) {
			rf.mu.Lock()
			if !rf.isLeader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := max(rf.nextIndex[peer]-1, 0)
			prevLogTerm := 0
			prevLogPos := 0
			if prevLogIndex != 0 {
				_, prevLogPos = rf.index2Pos(prevLogIndex)
				prevLogTerm = rf.log[prevLogPos].Term
			}
			
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			} // no Entries (empty) for heartbeat
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			if !rf.sendAppendEntries(peer, &args, &reply) {
				// network error
				return
			}

			if !reply.Success {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm { // not leader anymore
					rf.currentTerm = reply.Term
					rf.isLeader = false
					rf.votedFor = -1
					rf.mu.Unlock()
				} else { // log inconsistency
					rf.nextIndex[peer] = max(rf.nextIndex[peer]-1, 1)
					rf.mu.Unlock() // following raiseAppendEntries requires mu.lock()
					go rf.raiseAppendEntries(peer)
				}
			}

		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		if _, isLeader := rf.GetState(); isLeader {
			// periodically send heartbeat to other peers
			rf.broadHeartbeat()
			time.Sleep(time.Duration(heartbeatTimeout) * time.Millisecond)
			continue
		}

		// Wait for an election timeout, then check if an election should be raised.
		rf.SetHeartbeat(false)
		time.Sleep(time.Duration(randElectionTimeout()) * time.Millisecond)

		if !rf.GetHeartBeat() {
			result := rf.raiseElection(randElectionTimeout())
			rf.SetIsLeader(result)

			rf.mu.Lock()
			DPrintf("R[%d_%d] finish election, result: %v", rf.me, rf.currentTerm, rf.isLeader)
			rf.mu.Unlock()
		}
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

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCh = applyCh
	DPrintf("R[%d_%d] is now online.\n", rf.me, rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.ticker()

	// start ticker goroutine to start apply committed log
	go rf.applyTicker()

	return rf
}
