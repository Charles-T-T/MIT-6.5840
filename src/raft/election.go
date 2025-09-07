// election.go
//
// Code about leader election
// e.g. RequestVote RPC

package raft

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"
)

func randElectionTimeout() int64 {
	return 300 + (rand.Int63() % 300) // ms
}

type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

func (rf *Raft) beFollower() {
	rf.state = Follower
	rf.isLeader = false
	rf.VotedFor = -1
	DPrintf("R[%d] become follower.\n", rf.me)
}

func (rf *Raft) beCandidate() {
	rf.state = Candidate
	rf.isLeader = false
}

func (rf *Raft) beLeader() {
	rf.state = Leader
	rf.isLeader = true
	DPrintf("R[%d] become leader.\n", rf.me)

	// Reinitialize nextIndex[] and matchIndex[]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	// Broadcast heartbeat immediately
	curTerm := rf.CurrentTerm
	curCID := rf.commitIndex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			args := AppendEntriesArgs{
				Term:         curTerm,
				LeaderID:     rf.me,
				PrevLogIndex: lastLogIndex,
				PrevLogTerm:  lastLogTerm,
				Entries:      []LogEntry{},
				LeaderCommit: curCID,
			}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(peer, &args, &reply)
		}(i)
	}
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

	DPrintf("R[%d_%d] receive RV from R[%d_%d]", rf.me, rf.CurrentTerm, args.CandidateID, args.Term)

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.beFollower()
		rf.persist()
	}

	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID {
		return
	}

	// candidateTerm == rf.currentTerm
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	if args.LastLogTerm > lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		rf.persist()
		rf.lastHeartbeatTime = time.Now()
		return
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
	DPrintf("R[%d_%d] send RV to R[%d]\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// A follower turns into candidate when election timeout,
// then raises an election.
// Return whether it wins the election or not.
func (rf *Raft) raiseElection(electionTimeout int64) bool {
	rf.mu.Lock()

	raiseTerm := rf.CurrentTerm + 1
	DPrintf("R[%d_%d] timeout after %d ms, become candidate R[%d_%d] and raise an election.\n",
		rf.me, rf.CurrentTerm, electionTimeout, rf.me, raiseTerm)
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()

	var votesCount int32 = 1 // all votes count, including agree and disagree
	var agreeVotes int32 = 1 // default to vote for itself immediately
	var disagreeVotes int32 = 0

	currentTerm := rf.CurrentTerm
	candidateID := rf.me
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	rf.mu.Unlock()

	resultCh := make(chan bool, len(rf.peers))

	// Send RequestVote RPC to other peers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
				select {
				case <-ctx.Done():
					// DPrintf("R[%d_%d]'s election already finished.\n", rf.me, currentTerm)
					return
				default:
				}

				rf.mu.Lock()
				DPrintf("R[%d_%d] get vote from [%d_%d]: %v\n",
					rf.me, raiseTerm, id, reply.Term, reply.VoteGranted)
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.beFollower()
					rf.persist()
					resultCh <- false
				}
				rf.mu.Unlock()
			} else {
				return
			}

			atomic.AddInt32(&votesCount, 1)
			if reply.VoteGranted {
				atomic.AddInt32(&agreeVotes, 1)
				if atomic.LoadInt32(&agreeVotes) > int32(len(rf.peers)/2) {
					resultCh <- true
				}
			} else {
				atomic.AddInt32(&disagreeVotes, 1)
				if atomic.LoadInt32(&disagreeVotes) > int32(len(rf.peers)/2) {
					resultCh <- false
				}
			}
			if atomic.LoadInt32(&votesCount) == int32(len(rf.peers)) {
				resultCh <- false
			}
		}(i)
	}

	// Check whether more than half of the votes have been collected.
	timeout := randElectionTimeout()
	select {
	case result := <-resultCh:
		DPrintf("R[%d_%d] get election result: %v.", rf.me, raiseTerm, result)
		return result
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		DPrintf("R[%d_%d]'s election timeout after %v ms", rf.me, raiseTerm, timeout)
		return false
	}
}

// Leader sends heartbeat to other peers
func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.raiseAppendEntries(i)
	}
}

func (rf *Raft) electTicker() {
	for !rf.killed() {
		timeout := randElectionTimeout()

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == Leader {
			rf.broadcastHeartbeat()
			time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)
			continue
		}

		time.Sleep(time.Duration(timeout) * time.Millisecond)

		// check election timeout
		rf.mu.Lock()
		if time.Since(rf.lastHeartbeatTime) >= time.Duration(timeout)*time.Millisecond {
			rf.beCandidate()
			rf.mu.Unlock()

			win := rf.raiseElection(timeout)

			rf.mu.Lock()
			rf.lastHeartbeatTime = time.Now() // reset election timeout
			if win && rf.state == Candidate {
				rf.beLeader()
			}
		}
		rf.mu.Unlock()
	}
}
