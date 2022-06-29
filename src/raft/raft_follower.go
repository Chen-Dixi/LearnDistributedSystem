package raft

import (
	
)
// AppendEtries prevLogIndex, PrevLogTerm不匹配时，计算该返回给Leader的 Index
func (rf *Raft) getFirstIndexStoreTheSameTerm(logIndex int) (int, int) {
	if logIndex == 0 {
		return 0, 0
	}

	if !rf.haveIndex(logIndex) {
		// logIndex is our of range in this entries
		// the caller need to plus one
		return rf.getLastLogIndex() + 1, 0
	}

	// binary search
	left := rf.getFirstLogIndex()
	right := logIndex
	term := rf.getLogTerm(logIndex)

	for left < right {
		mid := left + (right - left) / 2

		if rf.getLogTerm(mid) == term {
			right = mid
		} else { // LogTerm(mid) < term
			left = mid + 1
		}
	}

	return left, term
}

// AppendEtries prevLogIndex, PrevLogTerm不匹配时，计算该返回给Leader的 Index
func (rf *Raft) getAppendEntiresRejectionIndexAndTerm(prevLogIndex int, prevLogTerm int) (int, int) {
	// prevLogIndex 肯定大于0，否则不会执行次函数
	// 返回的index 和 term 肯定是与leader不匹配的条目
	haveSnapshot, snapshotLastIncludedIndex, snapshotLastIncludedTerm := rf.haveSnapshot()
	
	if haveSnapshot {
		if len(rf.logs) == 0 {
			return snapshotLastIncludedIndex + 1, snapshotLastIncludedTerm
		}
	}

	if !rf.haveLogIndex(prevLogIndex) {
		// prevLogIndex 在rf.logs之外
		return rf.getFirstLogIndex() + 1, 0
	}

	left := rf.getFirstLogIndex()
	right := prevLogIndex
	term := rf.getLogTerm(prevLogIndex)

	for left < right {
		mid := left + (right - left) / 2

		if rf.getLogTerm(mid) == term {
			right = mid
		} else { // LogTerm(mid) < term
			left = mid + 1
		}
	}

	return left, term
}

// TBD consider snapshot
func (rf *Raft) Match(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex == 0 {
		return true
	}
	
	// if !rf.haveIndex(prevLogIndex) {
	// 	return false
	// }
	haveSnapshot, snapshotLastIncludedIndex, _ := rf.haveSnapshot()

	if haveSnapshot && prevLogIndex <= snapshotLastIncludedIndex{
		// 假设，只要prevLogIndex 在snapshot之前（包括snapshotLastIncludedIndex), 则一定匹配
		// 因为是同一个 Term 的 同一个Leader
		return true
	}

	// prevLogIndex 在snapshot之后
	if !rf.haveLogIndex(prevLogIndex) {
		return false
	}

	term := rf.getLogTerm(prevLogIndex)
	return term == prevLogTerm
}

func (rf *Raft) TakeNewEntries(prevLogIndex int, entries []Entry) {
	i := 0
	if prevLogIndex!=0 {
		i = rf.getIndexFromZero(prevLogIndex)+1
	}
	j := 0

	// Append any new entries not already in the log
	for ; i<len(rf.logs) && j < len(entries); {
		if rf.logs[i].Index != entries[j].Index || rf.logs[i].Term != entries[j].Term {
			break
		}

		i++
		j++
	}

	
	if (j == len(entries)) {
		// The if here is crucial. If the follower has all the entries the leader sent, 
		// the follower MUST NOT truncate its log. 
		// Any elements following the entries sent by the leader MUST be kept.
		// we could be receiving an **outdated AppendEntries RPC from the leader, 
		// and truncating the log would mean “taking back” entries that we may have 
		// already told the leader that we have in our log.
		return
	}
	
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	rf.logs = append(rf.logs[:i], entries[j:]...)
	DPrintf("[AppenEntry:TakeNewEntries][Follower %d] resulted logs:%v", rf.me, rf.logs)
	// save Raft's persistent state to stable storage
	rf.persist()
}

// Tmp
func (rf *Raft) TakeNewEntriesConsideringSnapshot(prevLogIndex int, entries []Entry) {
	haveSnapshot, snapshotLastIncludedIndex, _ := rf.haveSnapshot()
	
	i := 0
	j := 0
	if haveSnapshot && prevLogIndex <= snapshotLastIncludedIndex{
		if entries[len(entries)-1].Index <= prevLogIndex {
			// 整个收到的entries都在snaphost之内
			return
		}
		// 收到的entries有一部分在snaphot之外
		j = snapshotLastIncludedIndex-entries[0].Index + 1
	} else {
		// 因为通过了Match，此时logs里面一定有 prevLogIndex这一项
		if prevLogIndex != 0 {
			i = rf.getIndexFromZero(prevLogIndex)+1
		}
	}

	// Append any new entries not already in the log
	for ; i<len(rf.logs) && j < len(entries); {
		if rf.logs[i].Index != entries[j].Index || rf.logs[i].Term != entries[j].Term {
			break
		}

		i++
		j++
	}

	
	if (j == len(entries)) {
		// The if here is crucial. If the follower has all the entries the leader sent, 
		// the follower MUST NOT truncate its log. 
		// Any elements following the entries sent by the leader MUST be kept.
		// we could be receiving an **outdated AppendEntries RPC from the leader, 
		// and truncating the log would mean “taking back” entries that we may have 
		// already told the leader that we have in our log.
		return
	}
	
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	rf.logs = append(rf.logs[:i], entries[j:]...)
	DPrintf("[AppenEntry:TakeNewEntries][Follower %d] resulted logs:%v", rf.me, rf.logs)
	// save Raft's persistent state to stable storage
	rf.persist()
}

//
// Receive heartbeats and log entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Heartbeat if args.Entries is empty; AppendEntries otherwise
	if len(args.Entries) == 0 {
		// receive heart beats, update the time
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("[HeartbeatRPC][Follower %d] received heart beat from %d with term:[%d]", rf.me, args.LeaderId, args.Term)
		if args.Term < rf.currentTerm {
			// Reply false if term < currentTerm (§5.1)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}

		if args.Term > rf.currentTerm {
			rf.MeetHigherTerm(args.Term)
		} else {
			rf.MeetLeaderWinCurrentTerm(args.LeaderId, args.Term)
		}

		// args.Term & args.LeaderCommit
		if args.LeaderCommit > rf.commitIndex {
			oldCommitIndex := rf.commitIndex
			nextApplyIndex := Max(oldCommitIndex + 1, rf.getFirstLogIndex())
			// it needs to be computed with the index of the last new entry.
			// If leaderCommit is beyond the entries the leader sent you, 
			// you may apply incorrect entries
			
			rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, args.MatchIndex))
			
			DPrintf("[HeartbeatRPC][Follower %d] commitIndex update from %d to %d in term:[%d]", rf.me, oldCommitIndex, rf.commitIndex, rf.currentTerm)
			appliedCmd := make([]Entry, 0)
			for i := nextApplyIndex; i<=rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid : true,
					Command : rf.getLog(i).Command,
					CommandIndex : i,
				}
				appliedCmd = append(appliedCmd, rf.getLog(i))
				rf.applyCh <- msg
			}
			DPrintf("[HeartbeatRPC][Follower: %d] apply logs: [%v]", rf.me, appliedCmd)
			rf.lastApplied = rf.commitIndex
		}

		rf.ResetElectionTimeout()
		rf.ResetLastReceiveRpcTime()
		reply.Success = true
		reply.Term = rf.currentTerm
	} else {
		// Handle replicated log sent from leader
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("[AppendEntryRPC][Follower %d] received entries from [Leader %d] in term:[%d] with new entries: [%v]", rf.me, args.LeaderId, rf.currentTerm, args.Entries)
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		
		
		if args.Term > rf.currentTerm {
			rf.MeetHigherTerm(args.Term)
		} else {
			rf.MeetLeaderWinCurrentTerm(args.LeaderId, args.Term)
		}
		// Now we have correct Leader and Term
		// Reply false if don't have match log
		if !rf.Match(args.PrevLogIndex, args.PrevLogTerm) {
			DPrintf("[AppendEntryRPC:NotMatch][Follower %d] reject Leader %d, PrevLogIndex:[%d], PrevLogTerm:[%d]", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
			// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			reply.Success = false
			reply.Term = rf.currentTerm
			// include the term of the conflicting entry and the first index it stores for that term
			// reply.FirstIndexOfTerm, reply.ConflictTerm = rf.getFirstIndexStoreTheSameTerm(args.PrevLogIndex)
			reply.FirstIndexOfTerm, reply.ConflictTerm = rf.getAppendEntiresRejectionIndexAndTerm(args.PrevLogIndex, args.PrevLogTerm)
			return
		}

		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		// Append any new entries not already in the log
		// rf.TakeNewEntries(args.PrevLogIndex, args.Entries)
		rf.TakeNewEntriesConsideringSnapshot(args.PrevLogIndex, args.Entries)
		DPrintf("[AppendEntryRPC TakeNewEntriesConsideringSnapshot][Follower %d] Receive Entries from [Leader %d], now has %d entires: [%v]", rf.me, args.LeaderId, len(rf.logs), rf.logs)

		if args.LeaderCommit > rf.commitIndex {
			oldCommitIndex := rf.commitIndex
			// considering snapshot
			nextApplyIndex := Max(oldCommitIndex + 1, rf.getFirstLogIndex())
			rf.commitIndex = Min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
			
			DPrintf("[AppendEntryRPC][Follower %d] commitIndex update from %d to %d in term:[%d]", rf.me, oldCommitIndex, rf.commitIndex, rf.currentTerm)
			appliedCmd := make([]interface{}, 0)
			for i := nextApplyIndex; i<=rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid : true,
					Command : rf.getLog(i).Command,
					CommandIndex : i,
				}
				appliedCmd = append(appliedCmd, rf.getLog(i).Command)
				rf.applyCh <- msg
			}
			DPrintf("[AppendEntryRPC][Follower: %d] apply logs: [%v]", rf.me, appliedCmd)
			rf.lastApplied = rf.commitIndex
		}
		
		rf.ResetElectionTimeout()
		rf.ResetLastReceiveRpcTime()
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	
	if args.Term > rf.currentTerm {
		rf.MeetHigherTerm(args.Term)
	} else {
		rf.MeetLeaderWinCurrentTerm(args.LeaderId, args.Term)
	}

	haveSnapshot, snapshotLastIncludedIndex, _ := rf.haveSnapshot()
	
	if haveSnapshot && snapshotLastIncludedIndex >= args.LastIncludedIndex {
		// 现有的snapshot 比 发送过来的snapshot更新
		return
	}

	
	// snapshot 具有 本地snapshot没有的信息
	rf.snapshotInMem.Snapshot = args.Data
	rf.snapshotInMem.LastIncludedIndex = args.LastIncludedIndex
	rf.snapshotInMem.LastIncludedTerm = args.LastIncludedTerm

	if rf.haveLogIndex(args.LastIncludedIndex) && rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		i := rf.getIndexFromZero(args.LastIncludedIndex)
		// log entries covered by the snaphost are deleted
		rf.logs = rf.logs[i+1:]	
	} else {
		// snapshot containes new information, follower discards its entire log
		rf.logs = make([]Entry, 0)
	}
	
	if rf.commitIndex < args.LastIncludedIndex {
		// apply somthing
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm: args.LastIncludedTerm,
		}
		
		rf.applyCh <- msg
	}
	rf.commitIndex = Max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = rf.commitIndex
	rf.persist()
	DPrintf("[InstallSnapshot][Follower %d] receive snapshot from Leader %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	
	rf.ResetElectionTimeout()
	rf.ResetLastReceiveRpcTime()
}