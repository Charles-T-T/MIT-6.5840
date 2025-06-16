// log.go
//
// Code about log replicating
// e.g. AppendEntries RPC

package raft

import "time"

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

func (rf *Raft) hasPrevLog(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex == 0 && prevLogTerm == 0 {
		// rf.log = rf.log[:0]
		return true
	} else {
		if len(rf.log) == 0 {
			return false
		}

		if rf.log[len(rf.log)-1].Index < prevLogIndex {
			return false
		}

		for pos := 0; pos < len(rf.log); pos++ {
			if rf.log[pos].Index == prevLogIndex {
				if rf.log[pos].Term == prevLogTerm {
					return true
				} else {
					// rf.log = rf.log[:pos]
					return false
				}
			}
		}

		return false
	}
}

func (rf *Raft) appendNewEntries(entries []LogEntry) {
	DPrintf("R[%d_%d] is going to append entries, now log: %+v\n", rf.me, rf.currentTerm, rf.log)
	i := 0
	for ; i < len(entries); i++ {
		found, pos := rf.index2Pos(entries[i].Index)
		if found {
			if rf.log[pos].Term != entries[i].Term {
				rf.log = rf.log[:pos]
				break
			}
		} else {
			break
		}
	}
	rf.log = append(rf.log, entries[i:]...)
	DPrintf("R[%d_%d] append entries success, now log: %+v\n", rf.me, rf.currentTerm, rf.log)
}

func (rf *Raft) followerUpdateCID(args AppendEntriesArgs) {
	if args.LeaderCommit <= rf.commitIndex {
		return
	}

	lastIndex := 0
	if len(rf.log) > 0 {
		lastIndex = rf.log[len(rf.log)-1].Index
	}

	oldCID := rf.commitIndex
	newCID := min(args.LeaderCommit, lastIndex)
	if newCID > oldCID {
		_, pos := rf.index2Pos(newCID)
		if rf.log[pos].Term == args.Term {
			rf.commitIndex = newCID
			DPrintf("R[%d_%d] update CID by: %v,\n leaderCID: %d, oldCID: %d, nowCID: %d, log: %+v\n",
				rf.me, rf.currentTerm, args, args.LeaderCommit, oldCID, rf.commitIndex, rf.log)
		}
	}
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
	if !rf.hasPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		DPrintf("R[%d_%d] reject AE from R[%d_%d] because of log inconsistency, its log: %+v\n",
			rf.me, rf.currentTerm, args.LeaderID, args.Term, rf.log)
		reply.Success = false
		return
	}

	// Check if it's just a heartbeat
	if len(args.Entries) == 0 {
		DPrintf("R[%d_%d] receive headtbeat from R[%d_%d]", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	} else {
		// Append any new entries not already in the log
		DPrintf("R[%d_%d] receive AE from R[%d_%d]: %+v\n", rf.me, rf.currentTerm, args.LeaderID, args.Term, args)
		rf.appendNewEntries(args.Entries)
	}

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	rf.followerUpdateCID(*args)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.leaderUpdateCommitID()
	return ok
}

// Organize args and then send the AppendEntries RPC to a peer.
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
		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := 0
		prevLogPos := 0
		if prevLogIndex != 0 {
			_, prevLogPos = rf.index2Pos(prevLogIndex)
			prevLogTerm = rf.log[prevLogPos].Term
		}

		// indexExist, nextPos := rf.index2Pos(rf.nextIndex[peer])
		// if !indexExist {
		// 	nextPos = 0
		// }

		nextPos := prevLogPos + 1
		if prevLogPos == 0 {
			nextPos = 0
		}

		entries := make([]LogEntry, len(rf.log[nextPos:]))
		copy(entries, rf.log[nextPos:])

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
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
			if len(entries) > 0 {
				DPrintf("R[%d_%d]'s AE for R[%d_%d] success.\n", rf.me, curTerm, peer, reply.Term)
				rf.matchIndex[peer] = entries[len(entries)-1].Index
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			}
			success = true
		}
		rf.mu.Unlock()
	}
}

// (leader) Check if commitIndex can be updated
func (rf *Raft) leaderUpdateCommitID() {
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

// (all node) Check if new logs can be applied
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
