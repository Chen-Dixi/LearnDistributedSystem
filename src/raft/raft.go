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
	"time"
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// Log Entry
type Entry struct {
	Term    int
	Command interface{}
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
	logmu     sync.Mutex
	cond      *sync.Cond          //
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	role RaftServerRole
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent State
	currentTerm int
	votedFor    int
	// log []Entry
	logs []Entry

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Timeout clock
	lastRpcTime     time.Time
	electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// MMARK -- Section  Non-Synchronized methods

//
// Rules For Server
// votedFor = -1; role = Follower; currentTerm = args.term
//
func (rf *Raft) MeetHigherTerm(term int) {
	rf.currentTerm = term
	// reset votedFor
	rf.votedFor = -1
	rf.role = Follower
}

//
// rf.role = Follower
func (rf *Raft) MeetLeaderWinCurrentTerm(leaderId int, term int) {
	// TBD, not sure here
	if rf.votedFor == -1 {
		rf.votedFor = leaderId
	}
	rf.role = Follower
}

func (rf *Raft) VoteFor(term int, candidateId int) {
	rf.UpdateTermAndState(term, Follower)
	rf.votedFor = candidateId
}

func (rf *Raft) UpdateTermAndState(term int, role RaftServerRole) {
	rf.currentTerm = term
	rf.role = role
}

//
// become leader
func (rf *Raft) BecomeLeader() {
	// initialize necessary variable
	rf.role = Leader
	rf.logmu.Lock()
	lastLogIndexPlusOne := rf.getLastLogIndex() + 1
	rf.nextIndex = make([]int, len(rf.peers))
	for server, _ := range rf.peers {
		// Initialized to leader last log index + 1
		rf.nextIndex[server] = lastLogIndexPlusOne
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logmu.Unlock()
}

//
// rf.lastRpcTime = time.Now()
func (rf *Raft) ResetLastReceiveRpcTime() { // 重新计时
	rf.lastRpcTime = time.Now()
}

//
// 设置一个随机超时时间，
// 500~700 ms
func (rf *Raft) ResetElectionTimeout() { // 重新制定 timeout 时长
	rf.electionTimeout = time.Duration((rand.Int63n(200) + 500)) * time.Millisecond
}

//
// return time.Since(rf.lastRpcTime) > rf.electionTimeout
func (rf *Raft) IsElectionTimeout() bool {
	// 判断有没有 超过
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastRpcTime) > rf.electionTimeout
}

func (rf *Raft) addLog(entry Entry) {
	rf.logs = append(rf.logs, entry)
}

//
// As illustrated in Figure 2, the first index of logs is "1"
// 0 if logs is empty
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLogTerm(index int) int {
	//DPrintf("[getLogTerm] %v logs=%+v, index=%v", rf.me, rf.logs, index)
	if index <= 0 {
		return 0
	}
	offset := rf.logs[0].Index
	return rf.logs[index-offset].Term
}

func (rf *Raft) getLog(index int) Entry {
	offset := rf.logs[0].Index
	return rf.logs[index-offset]
}

//
//	offset := rf.logs[0].Index
//	return index-offset
func (rf *Raft) getIndexFromZero(index int) int {
	offset := rf.logs[0].Index
	return index-offset
}

//
// true if the server is more up-to-date than the parameters
func (rf *Raft) MoreUpToDate(lastLogIndex int, lastLogTerm int) bool {
	// Though, We have had the mu lock hear, we still need logmu lock
	// if this server have empty logs, he can vote for anyone
	if len(rf.logs) == 0 {
		return false 
	}

	term := rf.logs[len(rf.logs)-1].Term
	if term != lastLogTerm {
		return term > lastLogTerm
	}

	// if same term, the lastLogIndex need to >= len(rf.logs) - 1
	return len(rf.logs) > lastLogIndex
}

func (rf *Raft) haveIndex(logIndex int) bool {
	if logIndex <= 0 {
		return true
	}

	if (len(rf.logs)==0) {
		return false
	}

	return rf.logs[0].Index <= logIndex && logIndex <= rf.getLastLogIndex()
}


//
func (rf *Raft) Match(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex == 0 {
		return true
	}
	
	if !rf.haveIndex(prevLogIndex) {
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
		if rf.logs[i].Index != entries[j].Index || rf.logs[j].Term != rf.logs[j].Term {
			break
		}

		i++
		j++
	}
	rf.logs = append(rf.logs[:i], entries[j:]...)
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RaftServerRole string

const (
	Follower  RaftServerRole = "Follower"
	Candidate RaftServerRole = "Candidate"
	Leader    RaftServerRole = "Leader"
)

func (rf *Raft) AttemptElection() {
	// 创建 local variable 记录 自己的票数
	// 在这个函数里，通过发送rpc请求，确定自己是否能成为Learder
	// Update shared viriables
	rf.mu.Lock()
	rf.logmu.Lock()
	// increment currentTerm
	rf.currentTerm++
	// convert to candidate
	rf.role = Candidate
	// vote for self
	rf.votedFor = rf.me
	log.Printf("[%d] attempting an election at term %d", rf.me, rf.currentTerm)
	// reset election timer
	rf.ResetElectionTimeout()
	rf.ResetLastReceiveRpcTime()
	votes := 1
	done := false
	term := rf.currentTerm

	
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.getLogTerm(lastLogIndex)
	}
	rf.logmu.Unlock()
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.CallSendRequestVote(server, term, lastLogIndex, lastLogTerm)
			if !voteGranted {
				return
			}
			rf.mu.Lock()

			votes++
			log.Printf("[%d] got vote from %d", rf.me, server)
			if done || votes <= len(rf.peers)/2 {
				rf.mu.Unlock()
				return
			}
			done = true
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			log.Printf("[%d] we got enough votes, we are now the leader (currentTerm=%d, state=%v)!", rf.me, rf.currentTerm, rf.role)
			rf.BecomeLeader()
			rf.mu.Unlock()
			go rf.heartbeat()
		}(server)
	}
}

func (rf *Raft) CallSendRequestVote(server int, term int, lastLogIndex int, lastLogTerm int) bool {
	log.Printf("[%d] sending request vote to %d", rf.me, server)
	// 调用 sendRequestVote
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok == false {
		return false
	}

	return reply.VoteGranted
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// This is a rpc HANDLER!!!!!!
	// TBD not sure if locking here is decent
	// Implement Atomic
	log.Printf("[%d] received request vote from [%d](Term:%d)", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock() // 不可重入
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		// for candidate to update itself
		reply.Term = rf.currentTerm
		return
	}

	// when receiving any rpc request with term larger than current turn
	if args.Term > rf.currentTerm {
		// mutex是不可重入的锁
		// step down first
		rf.MeetHigherTerm(args.Term) // votedFor = -1; role = Follower; currentTerm = args.term
	}

	// currentTerm == args.term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// election restriction,
		if rf.MoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
		// grant vote
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.ResetElectionTimeout()
		rf.ResetLastReceiveRpcTime()
	} else {
		// has voted before，deny vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}



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
	log.Printf("Try start on:[%d] ", rf.me)
	rf.mu.Lock()
	// TBD
	index := len(rf.logs) + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if !isLeader {
		log.Printf("[%d] isn't the Leader, return false", rf.me)
		rf.mu.Unlock()
		return index, term, isLeader
	}

	entry := Entry{
		Term:    term,
		Command: command,
		Index:   index,
	}
	rf.mu.Unlock()

	// Your code here (2B).
	log.Printf("[%d] start aggreement with term:[%d] and index:[%d] ", rf.me, term, index)

	go rf.replicateLog(term, entry)
	go rf.checkCommitTask(term)
	return index, term, isLeader
}

func (rf *Raft) replicateLog(term int, entry Entry) {
	// prevLogIndex, prevLogTerm 发给每个server的不一样
	// LeaderCommit
	rf.mu.Lock()
	rf.addLog(entry)
	role := rf.role
	leaderCommit := rf.commitIndex
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
			for role == Leader {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.getLogTerm(prevLogIndex)
				entries := make([]Entry, 0)
				for i := rf.nextIndex[server]; i <= rf.getLastLogIndex(); i++ {
					entries = append(entries, rf.getLog(i))
				}
				log.Printf("RPC: prevLogIndex:[%d] prevLogTerm:[%d] entries_size:[%d]",prevLogIndex, prevLogTerm, len(entries))
				rf.mu.Unlock()
				replicated, replyTerm := rf.CallAppendEntries(server, term, leaderCommit, entries, prevLogIndex, prevLogTerm)
				rf.mu.Lock()
				if replyTerm > term { // RPC response contains term T > currentTerm, convert to follower
					// if our term has passed, immediately step down, and not update nextIndex
					rf.MeetHigherTerm(replyTerm)
					rf.mu.Unlock()
					return
				}

				if !replicated {
					rf.nextIndex[server] = rf.nextIndex[server] - 1
					if rf.nextIndex[server] <= 0 {
						rf.nextIndex[server] = 1
						rf.mu.Unlock()
						return
					}
					role = rf.role
					rf.mu.Unlock()
					continue
				}

				log.Printf("[%d] got success AppendEntry reply from %d in term:[%d]", rf.me, server, term)
				if term != rf.currentTerm || rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				rf.matchIndex[server] = prevLogIndex + len(entries)
				rf.nextIndex[server] = prevLogIndex + len(entries) + 1
				
				rf.mu.Unlock()
				break
			}
		} (server)
	}

}

func (rf *Raft) checkCommitTask(term int) {
	// A leader is not allowed to update commitIndex to somewhere in a previous term 
	count := 0
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		lastLogIndex := rf.getLastLogIndex()
		leaderCommitIndex := rf.commitIndex
		lastAppliedIndex := rf.lastApplied
		matchIndex := rf.matchIndex
		
		if role != Leader {
			rf.mu.Unlock()
			break
		}
		
		if lastLogIndex <= leaderCommitIndex { // already update commitIndex
			rf.mu.Unlock()
			time.Sleep(150 * time.Millisecond)
			continue
		}

		// Binary Search
		left := leaderCommitIndex + 1
		right := lastLogIndex
		count++
		log.Printf("[%d] start the %d the Binary Search in term:[%d]", rf.me, count, term)
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
					log.Printf("[%d] commitIndex update to %d in term:[%d] as Leader", rf.me, mid, term)
					rf.commitIndex = mid
					for i := lastAppliedIndex + 1; i<=mid; i++ {
						msg := ApplyMsg{
							CommandValid : true,
							Command : rf.getLog(i).Command,
							CommandIndex : i,
						}
						log.Printf("[applyLog] %v apply msg=%+v", rf.me, msg)
						rf.applyCh <- msg
					}
					rf.lastApplied = mid
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
		time.Sleep(150 * time.Millisecond)
	}
}
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// TBD
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		if role == Leader {
			time.Sleep(150 * time.Millisecond) // Leader don't have to check timeout
			continue
		}

		if rf.IsElectionTimeout() {
			rf.AttemptElection()
		}
		time.Sleep(150 * time.Millisecond)
	}
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
		rf.mu.Unlock()

		if role != Leader { // 如果不是leader, 退出循环
			log.Printf("[%d] is no longer Leader and stop sending heartbeat with term:[%d]", rf.me, term)
			break
		}

		log.Printf("[%d] starting heartbeat at term %d", rf.me, term)

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.CallAppendEntriesHeartBeat(server, term, leaderCommit)
			}(server)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) CallAppendEntriesHeartBeat(server int, term int, leaderCommit int) bool {
	log.Printf("[%d] sending heartbeat to %d", rf.me, server)
	// 调用 sendRequestVote
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
		LeaderCommit : leaderCommit,
	}

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok == false {
		return false
	}

	return reply.Success
}

func (rf *Raft) CallAppendEntries(server int, term int, leaderCommit int, entries []Entry, prevLogIndex int, prevLogTerm int) (bool, int) {
	log.Printf("[%d] sending append entries to %d in term:[%d]", rf.me, server, term)
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
		return false, 0
	}

	return reply.Success, reply.Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// Heartbeats
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Heartbeat if args.Entries is empty; AppendEntries otherwise

	if len(args.Entries) == 0 {
		// receive heart beats, update the time
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("[%d] received heart beat from %d in term:[%d]", rf.me, args.LeaderId, rf.currentTerm)
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
		if args.LeaderCommit > rf.commitIndex && rf.Match(args.LeaderCommit, args.Term){
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
			log.Printf("[%d] commitIndex update to %d in term:[%d] as Follower", rf.me, rf.commitIndex, rf.currentTerm)
			for i := oldCommitIndex + 1; i<=rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid : true,
					Command : rf.getLog(i).Command,
					CommandIndex : i,
				}
				log.Printf("[%d] apply msg=%+v as Follower", rf.me, msg)
				rf.applyCh <- msg
			}
		}

		rf.ResetElectionTimeout()
		rf.ResetLastReceiveRpcTime()
		reply.Success = true
		reply.Term = rf.currentTerm
	} else {
		
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("[%d] received append entries from %d in term:[%d]", rf.me, args.LeaderId, rf.currentTerm)
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

		// Reply false if don't have match log
		rf.logmu.Lock()
		defer rf.logmu.Unlock()
		if !rf.Match(args.PrevLogIndex, args.PrevLogTerm) {
			// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}

		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		// Append any new entries not already in the log
		rf.TakeNewEntries(args.PrevLogIndex, args.Entries)
		log.Printf("[%d] Take New Entries from %d, now has %d entires", rf.me, args.LeaderId, len(rf.logs))

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
			lastAppliedIndex := rf.lastApplied
			log.Printf("[%d] commitIndex update to %d in term:[%d] as Follower", rf.me, rf.commitIndex, rf.currentTerm)
			for i := lastAppliedIndex + 1; i<=rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid : true,
					Command : rf.getLog(i).Command,
					CommandIndex : i,
				}
				log.Printf("[%d] apply msg=%+v as Follower", rf.me, msg)
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
		}
		
		rf.ResetElectionTimeout()
		rf.ResetLastReceiveRpcTime()
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.cond = sync.NewCond(&rf.mu)
	rf.me = me
	rf.currentTerm = 0
	rf.applyCh = applyCh
	// Initialize as follower
	rf.role = Follower
	// Initialize election timeout
	rf.ResetLastReceiveRpcTime()
	rf.ResetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
