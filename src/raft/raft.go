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
	"bytes"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"

	"6.824/labgob"
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

type SnaphotInMem struct {
	Snapshot []byte
    LastIncludedIndex int
    LastIncludedTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
	rwmu	  sync.RWMutex
	cond      *sync.Cond          //
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	role uint32
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent State
	currentTerm int
	votedFor    int
	// log []Entry
	logs []Entry
	// snapshot
	snapshotInMem SnaphotInMem

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
	atomic.StoreUint32(&rf.role, Follower)
	// save Raft's persistent state to stable storage
	rf.persist()
}

//
// rf.role = Follower
func (rf *Raft) MeetLeaderWinCurrentTerm(leaderId int, term int) {
	if rf.votedFor != leaderId {
		rf.votedFor = leaderId
		// save Raft's persistent state to stable storage
		rf.persist()
	}
	atomic.StoreUint32(&rf.role, Follower)
	
}

//
// become leader
// rf.role = Leader; initialize nextIndex and matchIndex
func (rf *Raft) BecomeLeader() {
	// initialize necessary variable
	atomic.StoreUint32(&rf.role, Leader)
	lastLogIndexPlusOne := rf.getLastLogIndex() + 1
	rf.nextIndex = make([]int, len(rf.peers))
	for server, _ := range rf.peers {
		// Initialized to leader last log index + 1
		rf.nextIndex[server] = lastLogIndexPlusOne
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

//
// rf.lastRpcTime = time.Now()
func (rf *Raft) ResetLastReceiveRpcTime() { // 重新计时
	rf.lastRpcTime = time.Now()
}

//
// 设置一个随机超时时间，
// 500~756 ms
func (rf *Raft) ResetElectionTimeout() { // 重新制定 timeout 时长
	rf.electionTimeout = time.Duration((rand.Int63n(256) + 500)) * time.Millisecond
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
	// save Raft's persistent state to stable storage
	rf.persist()
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

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 0{
		return 0
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getFirstLogIndex() int {
	if len(rf.logs) == 0{
		return 0
	}
	return rf.logs[0].Index
}

// Get the term of log who has the same index
func (rf *Raft) getLogTerm(index int) int {
	//DPrintf("[getLogTerm] %v logs=%+v, index=%v", rf.me, rf.logs, index)
	if index <= 0 {
		return 0
	}
	offset := rf.logs[0].Index
	return rf.logs[index-offset].Term
}

//
//	offset := rf.logs[0].Index
//	return rf.logs[index-offset]
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
// true if the server is more up-to-date than the candidate
func (rf *Raft) MoreUpToDate(lastLogIndex int, lastLogTerm int) bool {
	haveSnapshot, snapshotLastIncludedIndex, snapshotLastIncludedTerm := rf.haveSnapshot()
	
	if len(rf.logs) == 0{
		
		if !haveSnapshot{
			// if this server have empty logs, he can vote for anyone
			return false
		}
		
		if snapshotLastIncludedTerm != lastLogTerm {
			return snapshotLastIncludedTerm > lastLogTerm
		}
		// if same term, the lastLogIndex need to >= len(rf.logs)
		return lastLogIndex < snapshotLastIncludedIndex
	}

	term := rf.getLastLogTerm()
	
	if term != lastLogTerm {
		return term > lastLogTerm
	}

	// if same term, the lastLogIndex need to >= len(rf.logs)
	return lastLogIndex < rf.getLastLogIndex()
}

// 
// 
func (rf *Raft) haveIndex(logIndex int) bool {
	if logIndex <= 0 {
		return false
	}

	if (len(rf.logs)==0) {
		return false
	}

	return rf.logs[0].Index <= logIndex && logIndex <= rf.getLastLogIndex()
}

// 
// True if rf.logs have an entry with the same log index
func (rf *Raft) haveLogIndex(logIndex int) bool {
	if logIndex <= 0 || len(rf.logs) == 0{
		return false
	}

	return rf.logs[0].Index <= logIndex && logIndex <= rf.getLastLogIndex()
}

// If there is not any snaphost, return false, 0, 0
func (rf *Raft) haveSnapshot() (bool, int, int) {
	haveSnapshot := rf.snapshotInMem.LastIncludedIndex > 0
	if !haveSnapshot {
		return haveSnapshot, 0, 0
	}

	return haveSnapshot, rf.snapshotInMem.LastIncludedIndex, rf.snapshotInMem.LastIncludedTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// voteFor, currentTerm, logs
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	
	w_snap := new(bytes.Buffer)
	e_snap := labgob.NewEncoder(w_snap)
	e_snap.Encode(rf.snapshotInMem)

	// rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, w_snap.Bytes())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Entry
	var currentTerm int
	var votedFor int

	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || d.Decode(&logs) != nil{
		// TBD
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotInMem SnaphotInMem

	if d.Decode(&snapshotInMem) != nil{
		// TBD
	} else {
		rf.snapshotInMem = snapshotInMem
	}
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
	
	
	rf.snapshotInMem.Snapshot = snapshot
	rf.snapshotInMem.LastIncludedIndex = index
	rf.snapshotInMem.LastIncludedTerm = rf.getLogTerm(index)
	// TBD, rf.logs = rf.logs[i+1:]
	i := rf.getIndexFromZero(index)
	rf.logs = rf.logs[i+1:]
	rf.persist()
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

	// help the follower to find the last index of New Entries,
	// used in Headbeat RPC Handler
	MatchIndex   int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// include the term of the conflicting entry and the first index it stores for that term
	ConflictTerm int
	FirstIndexOfTerm int
}

type InstallSnapshotArgs struct {
	Term		int
	LeaderId 	int
	LastIncludedIndex 	int
	LastIncludedTerm	int
	Offset		int
	Data		[]byte
	Done		bool
}

type InstallSnapshotReply struct {
	// currentTerm, for leader to update itself
	Term		int
}

type RaftServerRole uint32

const (
	Follower  uint32 = 1
	Candidate uint32 = 2
	Leader    uint32 = 3
)

func roleString (role uint32) string {
	// Follower  RaftServerRole = 1"Follower"
	// Candidate RaftServerRole = 2"Candidate"
	// Leader    RaftServerRole = 3"Leader"
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unkown"
	}
}

func (rf *Raft) AttemptElection() {
	// 创建 local variable 记录 自己的票数
	// 在这个函数里，通过发送rpc请求，确定自己是否能成为Learder
	// Update shared viriables
	rf.mu.Lock()
	// increment currentTerm
	rf.currentTerm++
	// convert to candidate
	atomic.StoreUint32(&rf.role, Candidate)
	// vote for self
	rf.votedFor = rf.me
	DPrintf("[%d] attempting an election at term %d", rf.me, rf.currentTerm)
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

	if lastLogIndex == 0 {
		if haveSnapshot, snapshotLastIncludedIndex, snapshotLastIncludedTerm := rf.haveSnapshot(); haveSnapshot {
			lastLogIndex = snapshotLastIncludedIndex
			lastLogTerm = snapshotLastIncludedTerm
		}
	}

	// save Raft's persistent state to stable storage
	rf.persist()
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
			DPrintf("[%d] got vote from %d", rf.me, server)
			if done || votes <= len(rf.peers)/2 {
				rf.mu.Unlock()
				return
			}
			done = true
			if rf.role != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
				DPrintf("[%d] we got enough votes, but not my term (currentTerm=%d, state=%v)!", rf.me, rf.currentTerm, roleString(rf.role))
				return
			}
			rf.BecomeLeader()
			DPrintf("[Peer %d] we got enough votes, we are now the leader (currentTerm=%d, state=%v)!", rf.me, rf.currentTerm, roleString(rf.role))
			rf.mu.Unlock()
			go rf.heartbeat()
			go rf.checkCommitTask(term)
		} (server)
	}
}

func (rf *Raft) CallSendRequestVote(server int, term int, lastLogIndex int, lastLogTerm int) bool {
	DPrintf("[%d] sending request vote to %d", rf.me, server)
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
	// Implement Atomic
	DPrintf("[%d] received request vote from [%d](Term:%d)", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock() // 不可重入
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("[RV:smallTerm] %d reject the RV from %d, term:[%d] > [%d]", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		// for candidate to update itself
		reply.Term = rf.currentTerm
		return
	}

	// when receiving any rpc request with term larger than current turn
	if args.Term > rf.currentTerm {
		// step down first
		rf.MeetHigherTerm(args.Term) // votedFor = -1; role = Follower; currentTerm = args.term
	}

	// currentTerm == args.term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// election restriction,
		if rf.MoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
			DPrintf("[RV:UpToDate] %d reject the RV from %d, Candidate is not MoreUpToDate", rf.me, args.CandidateId)
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
		// save Raft's persistent state to stable storage
		rf.persist()
	} else {
		// has voted before，deny vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		
		// the use of atomic avoids the need for a lock.
		isLeader := rf.isLeader()
		sleepTime := time.Duration((rand.Int63n(64) + 64)) * time.Millisecond
		if isLeader {
			time.Sleep(sleepTime) // Leader don't have to check timeout
			continue
		}

		if rf.IsElectionTimeout() {
			rf.AttemptElection()
		}
		time.Sleep(sleepTime)
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
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

// atomic.LoadInt32(&rf.dead)
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	role := atomic.LoadUint32(&rf.role)
	return role == Leader
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
	
	rf.readSnapshot(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}