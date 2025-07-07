// election.go
//
// Code about leader election
// e.g. RequestVote RPC

package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func randElectionTimeout() int64 {
	return 400 + (rand.Int63() % 300) // ms
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

	// rf.receiveHeartbeat = true
	DPrintf("R[%d_%d] receive RV from R[%d_%d]", rf.me, rf.CurrentTerm, args.CandidateID, args.Term)

	reply.Term = rf.CurrentTerm

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.isLeader = false
		rf.persist()
	} else if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	// candidateTerm == rf.currentTerm
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.Log) > 0 {
		lastLogIndex = rf.Log[len(rf.Log)-1].Index
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
	}

	if args.LastLogTerm > lastLogTerm {
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateID
			rf.persist()
		}
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
				reply.VoteGranted = true
				rf.VotedFor = args.CandidateID
				rf.persist()
			}
		}
	}

	// if !reply.VoteGranted {
	// 	DPrintf("R[%d_%d] reject RV from R[%d_%d], its votedFor: %d.\n",
	// 		rf.me, rf.CurrentTerm, args.CandidateID, args.Term, rf.VotedFor)
	// }
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

// A follower turns into candidate when election timeout,
// then raises an election.
// Return whether it wins the election or not.
func (rf *Raft) raiseElection(electionTimeout int64) bool {
	rf.mu.Lock()
	DPrintf("R[%d_%d] timeout, raising an election.\n", rf.me, rf.CurrentTerm)
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	var votesCount int32 = 1      // all votes count, including agree and disagree
	var grantVotesCount int32 = 1 // default to vote for itself immediately

	currentTerm := rf.CurrentTerm
	candidateID := rf.me
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.Log) > 0 {
		lastLogIndex = rf.Log[len(rf.Log)-1].Index
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
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
				rf.mu.Lock()
				DPrintf("R[%d_%d] get vote from [%d_%d]: %v\n", rf.me, rf.CurrentTerm, id, reply.Term, reply.VoteGranted)
				rf.mu.Unlock()
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
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
		}(i)
	}

	// Check whether more than half of the votes have been collected.
	select {
	case result := <-resultCh:
		rf.mu.Lock()
		DPrintf("R[%d_%d] get election result.", rf.me, rf.CurrentTerm)
		rf.mu.Unlock()
		return result

	case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
		rf.mu.Lock()
		DPrintf("R[%d_%d]'s election timeout after %v ms", rf.me, rf.CurrentTerm, electionTimeout)
		rf.mu.Unlock()
		return false
	}
}
