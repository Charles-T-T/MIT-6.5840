// log.go
//
// Code about log replicating
// e.g. AppendEntries RPC

package raft

import (
	"log"
	"time"
)

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

	// for conflict logs
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) hasPrevLog(prevLogIndex int, prevLogTerm int) (bool, int, int) {
	// starting index, defult to "has"
	if prevLogIndex == 0 && prevLogTerm == 0 {
		return true, -1, -1
	}

	// in snapshot
	if prevLogIndex == rf.LastIncludedIndex && prevLogTerm == rf.LastIncludedTerm {
		return true, -1, -1
	}

	// follower's log too short
	if len(rf.Log) == 0 || rf.Log[len(rf.Log)-1].Index < prevLogIndex {
		return false, -1, -1
	}

	ok, pos := rf.index2Pos(prevLogIndex)
	if !ok {
		return false, -1, -1
	}

	XTerm := rf.Log[pos].Term
	XIndex := rf.Log[pos].Index

	if XTerm == prevLogTerm && XIndex == prevLogIndex {
		return true, -1, -1
	}

	// confilict
	for i := pos; i >= 0; i-- {
		if rf.Log[i].Term != XTerm {
			XIndex = rf.Log[i].Index + 1
			break
		}
	}

	return false, XTerm, XIndex
}

func (rf *Raft) appendNewEntries(entries []LogEntry) {
	DPrintf("R[%d_%d] is going to append entries, now log: %+v\n", rf.me, rf.CurrentTerm, rf.Log)
	i := 0
	for ; i < len(entries); i++ {
		found, pos := rf.index2Pos(entries[i].Index)
		if found {
			if rf.Log[pos].Term != entries[i].Term {
				rf.Log = rf.Log[:pos]
				break
			}
		} else {
			break
		}
	}
	rf.Log = append(rf.Log, entries[i:]...)
	rf.persist()
	DPrintf("R[%d_%d] append entries success, now log: %+v\n", rf.me, rf.CurrentTerm, rf.Log)
}

func (rf *Raft) followerUpdateCID(args AppendEntriesArgs) {
	if args.LeaderCommit <= rf.commitIndex {
		return
	}

	lastIndex := 0
	if len(rf.Log) > 0 {
		lastIndex = rf.Log[len(rf.Log)-1].Index
	}

	oldCID := rf.commitIndex
	newCID := min(args.LeaderCommit, lastIndex)
	if newCID > oldCID {
		_, pos := rf.index2Pos(newCID)
		if rf.Log[pos].Term == args.Term {
			rf.commitIndex = newCID
			DPrintf("R[%d_%d] update CID by: %v,\n leaderCID: %d, oldCID: %d, nowCID: %d, log: %+v\n",
				rf.me, rf.CurrentTerm, args, args.LeaderCommit, oldCID, rf.commitIndex, rf.Log)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.receiveHeartbeat = true

	// Reply false if term < currentTerm
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	} else if args.Term > rf.CurrentTerm {
		DPrintf("R[%d_%d] update term to %d by AE from R[%d_%d]\n",
			rf.me, rf.CurrentTerm, args.Term, args.LeaderID, args.Term)
		rf.CurrentTerm = args.Term
		rf.isLeader = false
		rf.VotedFor = -1
		rf.persist()
	}

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	reply.XLen = len(rf.Log)
	ok, XTerm, XIndex := rf.hasPrevLog(args.PrevLogIndex, args.PrevLogTerm)
	if !ok {
		DPrintf("R[%d_%d] reject AE from R[%d_%d] because of log inconsistency, its lastInclude=(Term:%d, Id:%d), log: %+v\n args: %+v\n",
			rf.me, rf.CurrentTerm, args.LeaderID, args.Term, rf.LastIncludedTerm, rf.LastIncludedIndex, rf.Log, args)
		reply.Success = false
		reply.XTerm = XTerm
		reply.XIndex = XIndex
		return
	}

	DPrintf("R[%d_%d] receive AE from R[%d_%d]: %+v\n", rf.me, rf.CurrentTerm, args.LeaderID, args.Term, args)
	if len(args.Entries) > 0 {
		rf.appendNewEntries(args.Entries)
	}

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	rf.followerUpdateCID(*args)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.leaderUpdateCID()
	return ok
}

// Organize args and then send the AppendEntries RPC to a peer.
// Return immediately if not leader anymore or killed.
func (rf *Raft) raiseAppendEntries(peer int) {
	try := 1
	for {
		rf.mu.Lock()
		DPrintf("R[%d_%d] raise AE for R[%d], try time: %d\n", rf.me, rf.CurrentTerm, peer, try)
		try++

		if !rf.isLeader || rf.killed() {
			rf.mu.Unlock()
			break
		}

		if rf.nextIndex[peer] <= rf.LastIncludedIndex {
			DPrintf("R[%d_%d] raise IS for R[%d] because nextIndex[%d](%d) <= LastIncludedIndex(%d)\n",
				rf.me, rf.CurrentTerm, peer, peer, rf.nextIndex[peer], rf.LastIncludedIndex)
			rf.mu.Unlock() // raiseInstallSnapshot(peer) need Lock
			rf.raiseInstallSnapshot(peer)
			continue // break? return?
		}

		curTerm := rf.CurrentTerm
		// FIXME - use 'max(rf.nextIndex[peer]-1, 0)' is not a perfect way
		// to handle nextIndex, logically;
		// sometimes the nextIndex[peer] is ZERO, leading to prevLogIndex < 0,
		// which is wrong, but I just can't find why this occurs :(
		prevLogIndex := max(rf.nextIndex[peer]-1, 0)
		prevLogTerm := 0
		prevLogPos := 0
		ok := false

		if prevLogIndex != 0 {
			ok, prevLogPos = rf.index2Pos(prevLogIndex)
			if ok {
				prevLogTerm = rf.Log[prevLogPos].Term
			} else if prevLogIndex == rf.LastIncludedIndex {
				// prevLog in snapshot
				prevLogTerm = rf.LastIncludedTerm
			} else {
				DPrintf("⚠️ R[%d_%d] raiseAppendEntries for R[%d] failed because prevLogIndex %d not found in log: %+v\n",
					rf.me, curTerm, peer, prevLogIndex, rf.Log)
				log.Fatalf("⚠️ R[%d_%d] nextIndex[%d]=%d, lastIncludedIndex=%d\n",
					rf.me, curTerm, peer, rf.nextIndex[peer], rf.LastIncludedIndex)
			}
		}

		nextPos := prevLogPos + 1
		if prevLogPos == 0 {
			nextPos = 0
		}

		entries := make([]LogEntry, len(rf.Log[nextPos:]))
		copy(entries, rf.Log[nextPos:])

		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}

		DPrintf("R[%d_%d] send AE to R[%d]: %+v\n", rf.me, curTerm, peer, args)
		rf.mu.Unlock()

		if !rf.sendAppendEntries(peer, &args, &reply) {
			DPrintf("R[%d_%d]'s AE for R[%d_%d] fail because of network error.\n",
				rf.me, curTerm, peer, reply.Term)
			time.Sleep(time.Millisecond * 50)
			continue
		}

		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.CurrentTerm {
				// not leader anymore
				rf.CurrentTerm = reply.Term
				rf.isLeader = false
				rf.VotedFor = -1
				rf.persist()
				DPrintf("R[%d_%d]'s AE for R[%d_%d] fail because of smaller term.\n",
					rf.me, curTerm, peer, reply.Term)
			} else {
				// log inconsistency
				DPrintf("R[%d_%d]'s AE for R[%d_%d] fail because of log inconsistency.\n",
					rf.me, curTerm, peer, reply.Term)
				if reply.XTerm == -1 {
					// follower's log is too short
					rf.nextIndex[peer] = max(reply.XLen, 1)
				} else {
					// check if the leader has XTerm
					lastIndex := -1
					for i := len(rf.Log) - 1; i >= 0; i-- {
						if rf.Log[i].Term == reply.XTerm {
							lastIndex = rf.Log[i].Index
							break
						}
					}
					if lastIndex != -1 {
						// has XTerm
						rf.nextIndex[peer] = lastIndex
					} else {
						// doesn't has XTerm
						rf.nextIndex[peer] = reply.XIndex
					}
				}
			}
		} else {
			// append successfully
			DPrintf("R[%d_%d]'s AE for R[%d_%d] success.\n", rf.me, curTerm, peer, reply.Term)
			if len(entries) > 0 {
				rf.matchIndex[peer] = entries[len(entries)-1].Index
			} else {
				rf.matchIndex[peer] = prevLogIndex // empty entries, but still pass prevLogIndex check.
			}
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}
}

// (leader) Check if commitIndex can be updated
func (rf *Raft) leaderUpdateCID() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.Log) == 0 || !rf.isLeader {
		return
	}

	for i := len(rf.Log) - 1; i >= 0 && rf.Log[i].Index > rf.commitIndex; i-- {
		matchCount := 1 // initial one for leader itself
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= rf.Log[i].Index {
				matchCount++
			}
		}
		// only commit log entry whose term is the leader's own term
		if matchCount > len(rf.peers)/2 && rf.Log[i].Term == rf.CurrentTerm {
			rf.commitIndex = rf.Log[i].Index
			DPrintf("Leader R[%d_%d]'s CID updates to: %d\n", rf.me, rf.CurrentTerm, rf.commitIndex)
			break
		}
	}
}

// (all node) Check if new logs can be applied
func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.lastApplied < rf.commitIndex {
			nextIndex := rf.lastApplied + 1

			ok, pos := rf.index2Pos(nextIndex)
			if !ok {
				// index not found, may be compacted in snapshot, just wait for update
				break
			}

			log := rf.Log[pos]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}

			rf.lastApplied++

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			DPrintf("R[%d_%d] apply log: %+v\n", rf.me, rf.CurrentTerm, log)
		}

		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}
