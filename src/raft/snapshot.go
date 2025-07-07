// snapshot.go
//
// code about snapshot and log compaction
// e.g. InstallSnapshot RPC

package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("R[%d_%d] receive snapshot at index %d, current log length: %d\n",
		rf.me, rf.CurrentTerm, index, len(rf.Log))

	// outdated snapshot
	if index <= rf.LastIncludedIndex {
		return
	}

	ok, pos := rf.index2Pos(index)
	if !ok {
		// index not in current log, may have been compacted in CurSnapshot
		DPrintf("R[%d_%d] Snapshot index %d not found in log, current log length: %d\n",
			rf.me, rf.CurrentTerm, index, len(rf.Log))
		return
	}

	// Log compaction, keep entries after Log[pos]
	rf.LastIncludedIndex = rf.Log[pos].Index
	rf.LastIncludedTerm = rf.Log[pos].Term
	rf.Log = append([]LogEntry{}, rf.Log[pos+1:]...)

	// save snapshot and persist
	rf.CurSnapshot = snapshot
	rf.persist()

	DPrintf("R[%d_%d] Snapshot done: lastIncluded=(Term:%d,Id:%d), curLogLen=%d, snapshotLen=%d\n",
		rf.me, rf.CurrentTerm, rf.LastIncludedTerm, rf.LastIncludedIndex,  len(rf.Log), len(rf.CurSnapshot))
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // the snapshot itself
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.receiveHeartbeat = true

	DPrintf("R[%d_%d] receive IS from R[%d_%d], lastIncluded=(Term:%d, Id:%d)",
		rf.me, rf.CurrentTerm, args.LeaderId, args.Term, args.LastIncludedTerm, args.LastIncludedIndex)

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		DPrintf("R[%d_%d] reject IS from R[%d_%d]: term larger.",
			rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		return
	}

	// Update term if needed
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.isLeader = false
		rf.persist()
	}

	// Ignore snapshot if it's outdated
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		DPrintf("R[%d_%d] reject IS from R[%d_%d]: outdated snapshot. Its lastIncludedIndex: %d, log: %+v\n",
			rf.me, rf.CurrentTerm, args.LeaderId, args.Term, rf.LastIncludedIndex, rf.Log)
		return
	}

	// Discard entire log if it's completely covered by snapshot
	if len(rf.Log) > 0 {
		newLog := make([]LogEntry, 0)
		for _, entry := range rf.Log {
			if entry.Index > args.LastIncludedIndex {
				newLog = append(newLog, entry)
			}
		}
		rf.Log = newLog
	} else {
		rf.Log = []LogEntry{}
	}

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.CurSnapshot = args.Data

	// Update commitIndex and lastApplied
	rf.commitIndex = max(rf.commitIndex, rf.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.LastIncludedIndex)

	rf.persist()

	// Tell service layer to install snapshot
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		rf.applyCh <- msg
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) raiseInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.CurSnapshot,
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.isLeader = false
			rf.persist()
			return
		}

		// Update follower’s nextIndex and matchIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex

		DPrintf("R[%d_%d] IS to R[%d_%d] done, nextIndex[%d]=%d\n",
			rf.me, args.Term, server, reply.Term, server, rf.nextIndex[server])
	}
}
