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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	// term = rf.persister.
	return term, isleader
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

func (rf *Raft) DLPrintf(format string, a ...interface{}) {
	if Debug {

		// copy args to avoid data race
		args := make([]interface{}, len(a))
		copy(args, a)
		rf.mu.Lock()
		log.Printf(format, args...)
		rf.mu.Unlock()
	}
}

func (rf *Raft) SafeLogf(logFn func() string) {
	if Debug {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("%s", logFn())
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf.SetHeartbeat(true)
	rf.receiveHeartbeat = true
	DPrintf("R[%d_%d] receive RV from R[%d_%d]", rf.me, rf.currentTerm, args.CandidateID, args.Term)

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
	} else if args.Term < rf.currentTerm {
		return
	}

	// candidateTerm == currentTerm
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	if args.LastLogTerm > lastLogTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			reply.VoteGranted = true
		}
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
				reply.VoteGranted = true
			}
		}
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// translate LogEntry's Index to its position in log[]
func (rf *Raft) indexToPos(index int) (bool, int) {
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

// AppendEntries RPC, also serves as heartbeat.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.receiveHeartbeat = true

	// Reply false if term < currentTerm
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.votedFor = -1
	}

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	prevLogExist := false
	if args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {
		prevLogExist = true
		rf.log = rf.log[:0]
	} else {
		if len(rf.log) != 0 && rf.log[len(rf.log)-1].Index >= args.PrevLogIndex {
			for pos := 0; pos < len(rf.log); pos++ {
				if rf.log[pos].Index == args.PrevLogIndex {
					if rf.log[pos].Term == args.PrevLogTerm {
						prevLogExist = true
						break
					} else {
						// If an existing entry conflicts with a new one
						// (same index but different terms), delete the
						// existing entry and all that follow it
						prevLogExist = false
						rf.log = rf.log[:pos]
						break
					}
				}
			}
		} else {
			prevLogExist = false
		}
	}
	if !prevLogExist {
		DPrintf("R[%d_%d] reject AE from R[%d_%d] because of log inconsistency, its log: %+v\n",
			rf.me, rf.currentTerm, args.LeaderID, args.Term, rf.log)
		reply.Success = false
		return
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := 0
		if len(rf.log) > 0 {
			lastIndex = rf.log[len(rf.log)-1].Index
		}
		// newCommitIndex := min(args.LeaderCommit, lastIndex)
		newCommitIndex := min(args.PrevLogIndex, lastIndex) // NOTE: 和原文不太一致的实现
		if newCommitIndex != rf.commitIndex {
			oldCID := rf.commitIndex
			rf.commitIndex = newCommitIndex
			DPrintf("R[%d_%d] update CID by AE: %+v,\n leaderCID: %d, oldCID: %d, nowCID: %d, log: %+v\n",
				rf.me, rf.currentTerm, args, args.LeaderCommit, oldCID, rf.commitIndex, rf.log)
		}
	}

	if len(args.Entries) == 0 { // indicate that it's just a heartbeat
		DPrintf("R[%d_%d] receive headtbeat from R[%d_%d]", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		reply.Success = true
		return
	}

	DPrintf("R[%d_%d] receive AE from R[%d_%d]: %+v\n",
		rf.me, rf.currentTerm, args.LeaderID, args.Term, args)

	// Append any new entries not already in the log
	if len(rf.log) != 0 {
		if args.PrevLogIndex == 0 {
			rf.log = rf.log[:0]
		} else {
			_, pos := rf.indexToPos(args.PrevLogIndex)
			rf.log = rf.log[:pos+1]
		}
	}
	DPrintf("R[%d_%d] is going to append entries, now log: %+v\n", rf.me, rf.currentTerm, rf.log)
	rf.log = append(rf.log, args.Entries...)
	DPrintf("R[%d_%d] append entries success, now log: %+v\n", rf.me, rf.currentTerm, rf.log)

	DPrintf("R[%d_%d] finish AE from R[%d_%d]\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.updateCommitID()
	return ok
}

// check if commitIndex can be updated, for leader only
func (rf *Raft) updateCommitID() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) == 0 || !rf.isLeader {
		return
	}

	for i := len(rf.log) - 1; i >= 0 && rf.log[i].Index > rf.commitIndex; i-- {
		matchCount := 1 // initial one for leader itself
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= rf.log[i].Index {
				matchCount++
			}
			// only commit log entry whose term is the leader's own term
			if matchCount > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
				rf.commitIndex = rf.log[i].Index
				DPrintf("Leader R[%d_%d]'s CID updates to: %d\n", rf.me, rf.currentTerm, rf.commitIndex)
				break
			}
		}
	}
}

// Send AppendEntries RPCs with nonempty entries to a peer.
// Return immediately if not leader anymore or killed.
func (rf *Raft) raiseAppendEntries(peer int) {
	success := false
	for {
		rf.mu.Lock()
		if !rf.isLeader || rf.killed() || success {
			rf.mu.Unlock()
			break
		}

		curTerm := rf.currentTerm
		// prevLogIndex := max(rf.nextIndex[peer]-1, 0) // TODO: 这里不用判断了，nextIndex最小为1
		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := 0
		if prevLogIndex != 0 {
			_, prevLogPos := rf.indexToPos(prevLogIndex)
			prevLogTerm = rf.log[prevLogPos].Term
		}

		indexExist, nextPos := rf.indexToPos(rf.nextIndex[peer])
		if !indexExist {
			nextPos = 0
		}
		newEntries := make([]LogEntry, len(rf.log[nextPos:]))
		copy(newEntries, rf.log[nextPos:])

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      newEntries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}

		DPrintf("R[%d_%d] send AE to R[%d]: %+v\n", rf.me, curTerm, peer, args)
		rf.mu.Unlock()

		if !rf.sendAppendEntries(peer, &args, &reply) {
			DPrintf("R[%d_%d]'s AE for R[%d_%d] fail because of network error.\n",
				rf.me, curTerm, peer, reply.Term)
			time.Sleep(time.Second * 1)
			continue
		}

		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm { // not leader anymore
				rf.currentTerm = reply.Term
				rf.isLeader = false
				rf.votedFor = -1
				DPrintf("R[%d_%d]'s AE for R[%d_%d] fail because of smaller term.\n",
					rf.me, curTerm, peer, reply.Term)
			} else { // log inconsistency
				DPrintf("R[%d_%d]'s AE for R[%d_%d] fail because of log inconsistency.\n",
					rf.me, curTerm, peer, reply.Term)
				rf.nextIndex[peer] = max(rf.nextIndex[peer]-1, 1)
			}
		} else { // append successfully
			DPrintf("R[%d_%d]'s AE for R[%d_%d] success.\n",
				rf.me, curTerm, peer, reply.Term)
			rf.matchIndex[peer] = newEntries[len(newEntries)-1].Index
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			success = true
		}
		rf.mu.Unlock()
	}
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

// A follower turns into candidate when election timeout,
// then raises an election.
// Return whether it wins the election or not.
func (rf *Raft) raiseElection(electionTimeout int64) bool {
	rf.mu.Lock()
	DPrintf("R[%d_%d] timeout, raising an election.\n", rf.me, rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = rf.me
	var votesCount int32 = 1      // all votes, including agree and disagree
	var grantVotesCount int32 = 1 // default to vote for itself immediately

	currentTerm := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()

	resultCh := make(chan bool)

	// Send RequestVote RPC to other peers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(id int) {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(id, &args, &reply) {
				rf.SafeLogf(func() string {
					return fmt.Sprintf("R[%d_%d] get vote from [%d_%d]: %v\n", rf.me, rf.currentTerm, id, reply.Term, reply.VoteGranted)
				})
			}

			atomic.AddInt32(&votesCount, 1)
			if reply.VoteGranted {
				atomic.AddInt32(&grantVotesCount, 1)
				if atomic.LoadInt32(&grantVotesCount) > int32(len(rf.peers)/2) {
					resultCh <- true
				} else if atomic.LoadInt32(&votesCount) == int32(len(rf.peers)) {
					resultCh <- false
				}
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		}(i)
	}

	// Check whether more than half of the votes have been collected.
	select {
	case result := <-resultCh:
		rf.SafeLogf(func() string {
			return fmt.Sprintf("R[%d_%d] get election result.", rf.me, rf.currentTerm)
		})
		return result
	case <-time.After(time.Duration(electionTimeout) * time.Microsecond):
		rf.SafeLogf(func() string {
			return fmt.Sprintf("R[%d_%d]'s election timeout after %v ms", rf.me, rf.currentTerm, electionTimeout)
		})
		return false
	}
}

// Leader sends heartbeat to other peers
func (rf *Raft) broadHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			if !rf.isLeader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := max(rf.nextIndex[peer]-1, 0)
			prevLogTerm := 0
			if prevLogIndex != 0 {
				_, prevLogPos := rf.indexToPos(prevLogIndex)
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

			rf.mu.Lock()
			if !reply.Success {
				if reply.Term > rf.currentTerm { // not leader anymore
					rf.currentTerm = reply.Term
					rf.isLeader = false
					rf.votedFor = -1
				} else { // log inconsistency
					rf.nextIndex[peer] = max(rf.nextIndex[peer]-1, 1)
					// rf.raiseAppendEntries(peer)
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		if _, isLeader := rf.GetState(); isLeader {
			// periodically send heartbeat to other peers
			rf.broadHeartbeat()
			time.Sleep(85 * time.Millisecond)
			continue
		}

		// Wait for an election timeout, then check if an election should be raised.
		rf.SetHeartbeat(false)

		electionTimeout := 200 + (rand.Int63() % 300)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		if !rf.GetHeartBeat() {
			result := rf.raiseElection(electionTimeout)
			rf.SetIsLeader(result)
			rf.SafeLogf(func() string {
				return fmt.Sprintf("R[%d_%d] finish election, result: %v", rf.me, rf.currentTerm, rf.isLeader)
			})
		}
	}
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			for _, log := range rf.log {
				if log.Index == rf.lastApplied+1 {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      log.Command,
						CommandIndex: log.Index,
					}
					rf.lastApplied++
					DPrintf("R[%d_%d] apply log: %+v\n", rf.me, rf.currentTerm, log)
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
