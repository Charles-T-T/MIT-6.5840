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

	// TODO: supplement this raft struct
	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	isLeader   bool
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	receiveHeartbeat bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// TODO: GetState() 考虑是否加锁？
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	// term = rf.persister.
	return term, isleader
}

func (rf *Raft) CheckHeartBeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.receiveHeartbeat
}

func (rf *Raft) SetHeartbeat(state bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.receiveHeartbeat = state
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
	// TODO: RequestVoteArgs
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// TODO: RequestVoteReply
	Term        int
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// TODO: RequestVote()
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.SetHeartbeat(true)
	DPrintf("R[%d_%d] receive RV from R[%d_%d]", rf.me, rf.currentTerm, args.CandidateID, args.Term)

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	} else if args.Term < rf.currentTerm {
		reply.VoteGranted = false
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
		} else {
			reply.VoteGranted = false
		}
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
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

// AppendEntries, also serve as heartbeat.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
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
	reply.Term = rf.currentTerm

	// TODO - this need to be more specific in later tasks.
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.isLeader = false
		reply.Success = true
	} else if rf.currentTerm > args.Term {
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// rf.mu.Lock()
	DPrintf("R[%d_%d] timeout, raising an election.\n", rf.me, rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = rf.me
	var votesCount int32 = 1 // all votes, including agree and disagree
	var grantVotesCount int32 = 1 // default to vote for itself immediately

	currentTerm := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	// rf.mu.Unlock()

	resultCh := make(chan bool)

	// Send RequestVote RPC to other peers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// TODO - 考虑每个goroutine加一个timer？
		go func(id int) {
			// rf.mu.Lock()
			// defer rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{
				Term:        -1,
				VoteGranted: false,
			}
			rf.sendRequestVote(id, &args, &reply)
			DPrintf("R[%d_%d] get vote from [%d]: %v\n", rf.me, rf.currentTerm ,id, reply.VoteGranted)

			atomic.AddInt32(&votesCount, 1)
			if reply.VoteGranted {
				atomic.AddInt32(&grantVotesCount, 1)
				if atomic.LoadInt32(&grantVotesCount) > int32(len(rf.peers)/2) {
					resultCh <- true
				} else if atomic.LoadInt32(&votesCount) == int32(len(rf.peers)) {
					resultCh <- false
				}
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
			}
		}(i)
	}

	// Check whether more than half of the votes have been collected.
	select {
	case result := <- resultCh:
		DPrintf("R[%d_%d] get election result.", rf.me, rf.currentTerm)
		return result
	case <- time.After(time.Duration(electionTimeout) * time.Microsecond):
		DPrintf("R[%d_%d]'s election timeout after %v ms", rf.me, rf.currentTerm, electionTimeout)
		return false
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		if rf.isLeader {
			// periodically send heartbeat to other peers
			for i := range rf.peers {
				go func(id int) {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					if !rf.sendAppendEntries(id, &args, &reply) {
						return
					}
					if !reply.Success && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.isLeader = false
					}
				}(i)
			}

			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if a leader election should be started.
		rf.SetHeartbeat(false)

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 200 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if !rf.CheckHeartBeat() {
			electionTimeout := 100 + (rand.Int63() % 300)
			rf.isLeader = rf.raiseElection(electionTimeout)
			DPrintf("R[%d_%d] finish election, result: %v", rf.me, rf.currentTerm, rf.isLeader)
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
	// TODO: initialization in Make()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// DPrintf()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
