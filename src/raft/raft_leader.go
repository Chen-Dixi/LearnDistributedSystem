package raft

import "time"

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Try start on:[%d] ", rf.me)
	rf.mu.Lock()
	index := rf.getLastLogIndex()+1
	if index == 1 {
		if haveSnapshot, snapshotLastIncludedIndex, _ := rf.haveSnapshot(); haveSnapshot {
			index = snapshotLastIncludedIndex + 1
		}
	}

	term := rf.currentTerm
	isLeader := rf.role == Leader
	if !isLeader {
		DPrintf("[%d] isn't the Leader, return false", rf.me)
		rf.mu.Unlock()
		return index, term, isLeader
	}

	entry := Entry{
		Term:    term,
		Command: command,
		Index:   index,
	}
	rf.addLog(entry)
	rf.persist()
	rf.mu.Unlock()

	// Your code here (2B).
	DPrintf("[Start][Leader %d] Start cmd [%v] with term:[%d] and index:[%d] ", rf.me, command, term, index)
	// Check the index of Logs in concurrent situation
	
	// go rf.replicateLog(term, index)
	for peer := range rf.peers {
		if peer != rf.me{
			go func(peer int){
				// rf.replicatorCond[peer].L.Lock()
				rf.replicatorCond[peer].Signal()
				// rf.replicatorCond[peer].L.Unlock()
			} (peer)
		}
	}
	return index, term, isLeader
}

func (rf *Raft) replicator_backgroundTask(server int) {
	
	rf.replicatorCond[server].L.Lock()
	for rf.killed() == false {
		
		// code here
		for !rf.needReplicating(server) {
			rf.replicatorCond[server].Wait()
		}
		// make use of condition
		rf.mu.RLock()
		leaderCommit := rf.commitIndex
		term := rf.currentTerm
		role := rf.role
		if role != Leader {
			rf.mu.RUnlock()
			continue
		}

		DPrintf("[ReplicateLog:BackgroundTask][Leader %d] to follower [%d] term:[%d] rf.Logs:%v", rf.me, server, term, rf.logs)
		prevLogIndex := rf.nextIndex[server] - 1
		var prevLogTerm int
		// check if leader has already discarded the next log entry that it needs to send to a follower
		haveSnapshot, snapshotLastIncludedIndex, snapshotLastIncludedTerm := rf.haveSnapshot()
		
		//
		if haveSnapshot && rf.nextIndex[server] <=  snapshotLastIncludedIndex {
			DPrintf("[CallInstallSnapshot Start][Leader %d] send snapshot to follower %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, server, snapshotLastIncludedIndex, snapshotLastIncludedTerm)
			// leader has already discarded the next log entry that it needs to send to a follower
			rf.mu.RUnlock()
			// send snapshots to followers that lag behind
			ok, replyTerm := rf.CallInstallSnapshot(server)
			rf.mu.Lock()
			if !ok {
				rf.mu.Unlock()
				continue
			}
			
			if replyTerm > term {
				// RPC response contains term T > currentTerm, convert to follower
				// if our term has passed, immediately step down, and not update nextIndex
				rf.MeetHigherTerm(replyTerm)
				rf.mu.Unlock()
				
				continue
			}

			// snapshot 发送完毕 更新 nextIndex
			rf.nextIndex[server] = snapshotLastIncludedIndex + 1
			rf.mu.Unlock()
			
			DPrintf("[CallInstallSnapshot Finish][Leader %d] send snapshot to follower %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, server, snapshotLastIncludedIndex, snapshotLastIncludedTerm)
			continue
		}

		if haveSnapshot && prevLogIndex == snapshotLastIncludedIndex {
			DPrintf("[prevLogTerm = snapshotLastIncludedTerm][Leader %d] send snapshot to follower %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, server, snapshotLastIncludedIndex, snapshotLastIncludedTerm)
			prevLogTerm = snapshotLastIncludedTerm
		} else {
			// 此时 prevLogIndex 必定在 rf.logs里面
			prevLogTerm = rf.getLogTerm(prevLogIndex)
		}

		entries := make([]Entry, 0)
		for i := rf.nextIndex[server]; i <= rf.getLastLogIndex(); i++ {
			entries = append(entries, rf.getLog(i))
		}
		
		rf.mu.RUnlock()
		if len(entries) == 0{
			// unnecessary AE
			DPrintf("[replicator_backgroundTask][Leader %d] emptry entries! term: %d follower %d, nextIndex %d", rf.me, term, server, rf.nextIndex[server])
		}

		DPrintf("[Leader %d] send AppendEntriesRPC to %d: prevLogIndex:[%d] prevLogTerm:[%d] entries_size:[%d]", rf.me, server, prevLogIndex, prevLogTerm, len(entries))
		ok, replicated, replyTerm, _, firstIndexOfTerm := rf.CallAppendEntries(server, term, leaderCommit, entries, prevLogIndex, prevLogTerm)
		rf.mu.Lock()

		if !ok {
			// sending rpc failed
			DPrintf("[CallAppendEntries:Not_Ok][Leader %d] to [Follower %d] in term: %d", rf.me, server, term)
			rf.mu.Unlock()
			continue
		}

		if replyTerm > term {
			// RPC response contains term T > currentTerm, convert to follower
			// if our term has passed, immediately step down, and not update nextIndex
			rf.MeetHigherTerm(replyTerm)
			rf.mu.Unlock()
			
			continue
		}
		
		// 现在还是leader的轮次，
		if !replicated {
			// TBD 🤔
			DPrintf("[ReplicateLog Not_Replicated][Leader %d][Term %d] send follower %d: prevLogIndex %d replyIndex %d", rf.me, term, server, prevLogIndex, firstIndexOfTerm)
			rf.nextIndex[server] = firstIndexOfTerm - 1
			if rf.nextIndex[server] <= 0 {
				rf.nextIndex[server] = 1
			}

			rf.mu.Unlock()
			
			// if firstIndexOfTerm == -1 {
			// 	// server may crash
			// 	time.Sleep(130 * time.Millisecond)
			// }
			continue
		}

		DPrintf("[Leader %d] got success AppendEntry reply from %d in term:[%d]", rf.me, server, term)
		if term != rf.currentTerm || rf.role != Leader {
			rf.mu.Unlock()
			
			continue
		}
		
		// TestConcurrentStarts2B shows us a concurrent situation
		// the reply may be outdated, so I use Max function
		rf.matchIndex[server] = Max(rf.matchIndex[server], prevLogIndex + len(entries))
		rf.nextIndex[server] = Max(rf.nextIndex[server], prevLogIndex + len(entries) + 1)
		
		rf.mu.Unlock()
	}
	rf.replicatorCond[server].L.Unlock()
}

func (rf *Raft) replicateLog(term int, index int) {
	// prevLogIndex, prevLogTerm 发给每个server的不一样
	// LeaderCommit
	rf.mu.Lock()
	role := rf.role
	leaderCommit := rf.commitIndex
	DPrintf("[ReplicateLog:checkLogs][Leader %d] term:[%d] rf.Logs:%v", rf.me, term, rf.logs)
	nextIndex := make([]int, len(rf.peers))
	for server := range rf.peers {
		nextIndex[server] = rf.nextIndex[server]
	}
	rf.mu.Unlock()

	if role != Leader {
		return
	}

	// replicate to all other servers
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			failrpc_cnt := 0
			for rf.killed() == false {
				rf.mu.Lock()
				// 这里的nextIndex 换成local的
				
				if !rf.isLeader() {
					rf.mu.Unlock()
					break
				}
				prevLogIndex := nextIndex[server] - 1
				
				var prevLogTerm int
				// check if leader has already discarded the next log entry that it needs to send to a follower
				haveSnapshot, snapshotLastIncludedIndex, snapshotLastIncludedTerm := rf.haveSnapshot()
				
				if haveSnapshot && nextIndex[server] <=  snapshotLastIncludedIndex {
					DPrintf("[CallInstallSnapshot Start][Leader %d] send snapshot to follower %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, server, snapshotLastIncludedIndex, snapshotLastIncludedTerm)
					// leader has already discarded the next log entry that it needs to send to a follower
					rf.mu.Unlock()
					// send snapshots to followers that lag behind
					ok, replyTerm := rf.CallInstallSnapshot(server)
					rf.mu.Lock()
					if !ok {
						failrpc_cnt = failrpc_cnt + 1
						if failrpc_cnt > 30 {
							rf.mu.Unlock()
							break
						}
						rf.mu.Unlock()
						time.Sleep(50 * time.Millisecond)
						continue
					}
					
					if replyTerm > term {
						// RPC response contains term T > currentTerm, convert to follower
						// if our term has passed, immediately step down, and not update nextIndex
						rf.MeetHigherTerm(replyTerm)
						rf.mu.Unlock()
						return
					}

					// snapshot 发送完毕 更新 nextIndex
					nextIndex[server] = snapshotLastIncludedIndex + 1
					rf.mu.Unlock()
					DPrintf("[CallInstallSnapshot Finish][Leader %d] send snapshot to follower %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, server, snapshotLastIncludedIndex, snapshotLastIncludedTerm)
					continue
				}
				
				if haveSnapshot && prevLogIndex == snapshotLastIncludedIndex {
					DPrintf("[prevLogTerm = snapshotLastIncludedTerm][Leader %d] send snapshot to follower %d, snapshotLastIncludedIndex %d, snapshotLastIncludedTerm %d", rf.me, server, snapshotLastIncludedIndex, snapshotLastIncludedTerm)
					prevLogTerm = snapshotLastIncludedTerm
				} else {
					// 此时 prevLogIndex 必定在 rf.logs里面
					prevLogTerm = rf.getLogTerm(prevLogIndex)
				}

				role := rf.role
				entries := make([]Entry, 0)
				
				if index != rf.getLastLogIndex() {
					DPrintf("[ReplicateLog Unnecessary work][Leader %d] send to follower %d: index %d, currentLastLogIndex %d, in term %d", rf.me, server, index, rf.getLastLogIndex(), term)
					rf.mu.Unlock()
					break
				}

				for i := nextIndex[server]; i <= rf.getLastLogIndex(); i++ {
					entries = append(entries, rf.getLog(i))
				}

				rf.mu.Unlock()

				if role != Leader {
					break
				}

				if len(entries) == 0{
					// unnecessary AE
					break
				}
				DPrintf("[Leader %d] send AppendEntriesRPC to %d: prevLogIndex:[%d] prevLogTerm:[%d] entries_size:[%d]", rf.me, server, prevLogIndex, prevLogTerm, len(entries))
				
				ok, replicated, replyTerm, _, firstIndexOfTerm := rf.CallAppendEntries(server, term, leaderCommit, entries, prevLogIndex, prevLogTerm)
				rf.mu.Lock()

				if !ok {
					// sending rpc failed
					failrpc_cnt = failrpc_cnt + 1
					if failrpc_cnt > 30 {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					time.Sleep(50 * time.Millisecond)
					continue
				}

				if replyTerm > term {
					// RPC response contains term T > currentTerm, convert to follower
					// if our term has passed, immediately step down, and not update nextIndex
					rf.MeetHigherTerm(replyTerm)
					rf.mu.Unlock()
					return
				}
				
				// 现在还是leader的轮次，
				if !replicated {
					// TBD 🤔
					DPrintf("[ReplicateLog Not_Replicated][Leader %d][Term %d] send follower %d: prevLogIndex %d replyIndex %d", rf.me, term, server, prevLogIndex, firstIndexOfTerm)
					nextIndex[server] = firstIndexOfTerm - 1
					if nextIndex[server] <= 0 {
						nextIndex[server] = 1
					}

					rf.mu.Unlock()

					// if firstIndexOfTerm == -1 {
					// 	// server may crash
					// 	time.Sleep(130 * time.Millisecond)
					// }
					
					continue
				}

				DPrintf("[Leader %d] got success AppendEntry reply from %d in term:[%d]", rf.me, server, term)
				if term != rf.currentTerm || rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				
				// TestConcurrentStarts2B shows us a concurrent situation
				// the reply may be outdated, so I use Max function
				rf.matchIndex[server] = Max(rf.matchIndex[server], prevLogIndex + len(entries))
				rf.nextIndex[server] = Max(rf.nextIndex[server], prevLogIndex + len(entries) + 1)
				
				rf.mu.Unlock()
				break
			}
		} (server)
	}
}

func (rf *Raft) CallAppendEntriesHeartBeat(server int, term int, leaderCommit int, matchIndex int) bool {
	DPrintf("[%d] sending heartbeat to %d", rf.me, server)
	// 调用 sendRequestVote
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
		LeaderCommit : leaderCommit,
		MatchIndex : matchIndex,
		PrevLogIndex: -1,
		PrevLogTerm: -1,
	}

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok == false {
		return false
	}

	return reply.Success
}

func (rf *Raft) CallAppendEntries(server int, term int, leaderCommit int, entries []Entry, prevLogIndex int, prevLogTerm int) (bool, bool, int, int, int) {
	DPrintf("[CallAppendEntries][Leader %d] sending append entries to %d in term:[%d]", rf.me, server, term)
	// 调用 sendRequestVote
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		LeaderCommit: leaderCommit,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok == false {
		return false, false, 0, 0, 0
	}

	// 
	return ok, reply.Success, reply.Term, reply.ConflictTerm, reply.FirstIndexOfTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) checkCommitTask(term int) {
	// A leader is not allowed to update commitIndex to somewhere in a previous term 
	count := 0
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		lastLogIndex := rf.getLastLogIndex()
		leaderCommitIndex := rf.commitIndex
		// considering snapshot
		nextApplyIndex := Max(leaderCommitIndex + 1, rf.getFirstLogIndex())
		// lastAppliedIndex := rf.lastApplied
		matchIndex := rf.matchIndex
		
		if role != Leader {
			rf.mu.Unlock()
			break
		}
		
		if lastLogIndex <= leaderCommitIndex { // already update commitIndex
			rf.mu.Unlock()
			time.Sleep(130 * time.Millisecond)
			continue
		}

		// Binary Search
		left := leaderCommitIndex + 1
		right := lastLogIndex
		count++
		DPrintf("[CheckCommitTask][Leader: %d] start the %dth Binary Search in term:[%d]", rf.me, count, term)
		for left <= right {
			mid := (left + right) / 2
			
			// If there exists an N such that N > commitIndex, 
			// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			if rf.getLogTerm(mid) == term {
				match := 0
				for server, _ := range rf.peers {
					if server == rf.me {
						match++
						continue
					}
					if matchIndex[server] >= mid {
						match++
					}
				}
				if match > len(rf.peers) / 2 {
					DPrintf("[CheckCommitTask][Leader: %d] commitIndex update from %d to %d in term:[%d]", rf.me, leaderCommitIndex, mid, term)
					rf.commitIndex = mid
					appliedCmd := make([]Entry, 0)
					for i := nextApplyIndex; i<=mid; i++ {
						msg := ApplyMsg{
							CommandValid : true,
							Command : rf.getLog(i).Command,
							CommandIndex : i,
						}
						appliedCmd = append(appliedCmd, rf.getLog(i))
						rf.applyCh <- msg
					}
					rf.lastApplied = mid
					DPrintf("[CheckCommitTask][Leader: %d] apply logs: [%v]", rf.me, appliedCmd)
					break
				} else {
					right--
				}
			} else if rf.getLogTerm(mid) > term {
				right = mid - 1
			} else {
				left = mid + 1
			}
		}
		rf.mu.Unlock()
		time.Sleep(130 * time.Millisecond)
	}
}

func (rf *Raft) checkCommitTask1(term int) {
	// A leader is not allowed to update commitIndex to somewhere in a previous term 
	count := 0
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		lastLogIndex := rf.getLastLogIndex()
		leaderCommitIndex := rf.commitIndex
		// considering snapshot
		// nextApplyIndex := Max(leaderCommitIndex + 1, rf.getFirstLogIndex())
		// lastAppliedIndex := rf.lastApplied
		matchIndex := rf.matchIndex
		
		if role != Leader {
			rf.mu.Unlock()
			break
		}
		
		if lastLogIndex <= leaderCommitIndex { // already update commitIndex
			rf.mu.Unlock()
			time.Sleep(130 * time.Millisecond)
			continue
		}

		// Binary Search
		left := leaderCommitIndex + 1
		right := lastLogIndex
		count++
		DPrintf("[CheckCommitTask1][Leader: %d] start the %dth Binary Search in term:[%d]", rf.me, count, term)
		for left <= right {
			mid := (left + right) / 2
			
			// If there exists an N such that N > commitIndex, 
			// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			if rf.getLogTerm(mid) == term {
				match := 0
				for server, _ := range rf.peers {
					if server == rf.me {
						match++
						continue
					}
					if matchIndex[server] >= mid {
						match++
					}
				}
				if match > len(rf.peers) / 2 {
					DPrintf("[CheckCommitTask1][Leader: %d] commitIndex update from %d to %d in term:[%d]", rf.me, leaderCommitIndex, mid, term)
					rf.commitIndex = mid
					break
				} else {
					right--
				}
			} else if rf.getLogTerm(mid) > term {
				right = mid - 1
			} else {
				left = mid + 1
			}
		}
		rf.mu.Unlock()
		time.Sleep(130 * time.Millisecond)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	lastLogIndex := rf.getLastLogIndex()
	if haveSnapshot, lastIncludedIndex, _ := rf.haveSnapshot(); lastLogIndex == 0 && haveSnapshot{
		lastLogIndex = lastIncludedIndex
	}
	
	return rf.isLeader() && rf.matchIndex[peer] < lastLogIndex
}

//
// The goroutine starts at beginning as ticker()
func (rf *Raft) heartbeat() {
	// send appendEntry RPC
	for rf.killed() == false {
		rf.mu.Lock()
		term := rf.currentTerm
		role := rf.role
		leaderCommit := rf.commitIndex
		matchIndex := make([]int, len(rf.peers))
		for server, _ := range rf.peers {
			matchIndex[server] = rf.matchIndex[server]
		}
		rf.mu.Unlock()

		if role != Leader { // 如果不是leader, 退出循环
			DPrintf("[%d] is no longer Leader and stop sending heartbeat with term:[%d]", rf.me, term)
			break
		}

		DPrintf("[%d] starting heartbeat at term %d", rf.me, term)

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.CallAppendEntriesHeartBeat(server, term, leaderCommit, matchIndex[server])
			} (server)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) heartbeat_timer() {
	for rf.killed() == false {
		<- rf.heartbeatTimer.C
		rf.mu.Lock()
		term := rf.currentTerm
		role := rf.role
		leaderCommit := rf.commitIndex
		matchIndex := make([]int, len(rf.matchIndex))
		
		for server, _ := range rf.matchIndex {
			matchIndex[server] = rf.matchIndex[server]
		}
		
		
		
		if role != Leader { // 如果不是leader, 退出循环
			DPrintf("[%d] is no longer Leader and stop sending heartbeat with term:[%d]", rf.me, term)
			// rf.heartbeatTimer.Reset(150 * time.Millisecond)
			rf.mu.Unlock()
			continue
		}

		DPrintf("[%d] starting heartbeat at term %d", rf.me, term)

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.CallAppendEntriesHeartBeat(server, term, leaderCommit, matchIndex[server])
			} (server)
		}
		rf.heartbeatTimer.Reset(150 * time.Millisecond)
		rf.mu.Unlock()
	}
}

// Leader send snapshots to followers that lag behind
func (rf *Raft) CallInstallSnapshot(server int) (bool, int) {
	args := InstallSnapshotArgs{
		Term:	rf.currentTerm,
		LeaderId:	rf.me,
		LastIncludedIndex:	rf.snapshotInMem.LastIncludedIndex,
		LastIncludedTerm:	rf.snapshotInMem.LastIncludedTerm,
		Data:				rf.snapshotInMem.Snapshot,
		Done:				true,
	}
	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshotRPC(server, &args, &reply)
	if !ok {
		return false, 0
	}
	return ok, reply.Term
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}