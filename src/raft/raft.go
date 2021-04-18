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
	"sync"
	"sync/atomic"
	"log"
	"math/rand"
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
// A Go object implementing a single Raft peer.
//
type VotedPeer struct{
	candidateId int
	term int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cond	  *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role RaftServerRole
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// Persistent State
	currentTerm int
	votedFor int
	// log[]

	lastRpcTime time.Time
	
	// reset many times
	electionTimeout time.Duration
	// Volatile state on all servers

	// Volatile state on leaders
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
func (rf *Raft) MeetLeaderWinCurrentTerm(leaderId int, term int){
	// TBD, not sure here
	if rf.votedFor == -1 {
		rf.votedFor = leaderId
	}
	rf.role = Follower
}
// 

func (rf *Raft) VoteFor(term int, candidateId int){
	rf.UpdateTurnAndState(term, Follower)
	
	rf.votedFor = candidateId
}

func (rf *Raft) UpdateTurnAndState(term int, role RaftServerRole) {
	rf.currentTerm = term
	rf.role = role
}

//
// rf.lastRpcTime = time.Now()
func (rf *Raft) ResetLastReceiveRpcTime() { // 重新计时
	rf.lastRpcTime = time.Now()
}

//
// 设置一个随机超时时间，
// 500~700 ms
func (rf *Raft) ResetElectionTimeout(){ // 重新制定 timeout 时长
	rf.electionTimeout = time.Duration((rand.Int63n(200)+500)) * time.Millisecond
}

//
// return time.Since(rf.lastRpcTime) > rf.electionTimeout
func (rf *Raft) IsElectionTimeout() bool{
	// 判断有没有 超过
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastRpcTime) > rf.electionTimeout
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
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

type RaftServerRole string

const(
	Follower RaftServerRole = "Follower"
	Candidate RaftServerRole = "Candidate"
	Leader RaftServerRole = "Leader"
)

func (rf *Raft) AttemptElection(){
	// 创建 local variable 记录 自己的票数

	// 在这个函数里，通过发送rpc请求，确定自己是否能成为Learder
	// Update shared viriables
	rf.mu.Lock()
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
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int){
			voteGranted := rf.CallSendRequestVote(server, term)
			if !voteGranted{
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			votes++
			log.Printf("[%d] got vote from %d", rf.me, server)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			log.Printf("[%d] we got enough votes, we are now the leader (currentTerm=%d, state=%v)!", rf.me, rf.currentTerm, rf.role)
			rf.role = Leader
			// go rf.heartbeat()
		}(server)
	}
}

func (rf *Raft) CallSendRequestVote(server int, term int) bool{
	log.Printf("[%d] sending request vote to %d", rf.me, server)
	// 调用 sendRequestVote 
	args := RequestVoteArgs{
		Term : term,
		CandidateId: rf.me,
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
	log.Printf("[%d] received request vote from %d", rf.me, args.CandidateId)
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
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		// grant vote
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		
		rf.ResetElectionTimeout()
		rf.ResetLastReceiveRpcTime()
	}else{
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
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
			time.Sleep(150*time.Millisecond) // Leader don't have to 
			continue
		}

		if rf.IsElectionTimeout() {
			rf.AttemptElection()
		}
		// Sleep
		time.Sleep(150*time.Millisecond)
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
		rf.mu.Unlock()

		if role != Leader { // 如果不是leader, 退出
			time.Sleep(150*time.Millisecond)
			continue;
		}
		
		log.Printf("[%d] starting heartbeat at term %d", rf.me, term)

		for server, _  := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int){
				rf.CallAppendEntries(server, term)
			}(server)
		}
		time.Sleep(150*time.Millisecond)
	}
}

func(rf *Raft) CallAppendEntries(server int, term int) bool {
	log.Printf("[%d] sending heartbeat to %d", rf.me, server)
	// 调用 sendRequestVote 
	args := AppendEntriesArgs{
		Term : term,
		LeaderId : rf.me,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok == false {
		return false
	}

	return reply.Success
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 
// Heartbeats
func(rf *Raft) AppendEntries(args *AppendEntriesArgs , reply *AppendEntriesReply) {
	log.Printf("[%d] received heart beat from %d", rf.me, args.LeaderId)
	// receive heart beats, update the time
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	rf.ResetElectionTimeout()
	rf.ResetLastReceiveRpcTime()
	reply.Success = true
	reply.Term = rf.currentTerm
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
	rf.me = me
	rf.currentTerm = 0
	// Initialize as follower
	rf.role = Follower
	rf.cond = sync.NewCond(&rf.mu)
	// Initialize election timeout
	rf.ResetLastReceiveRpcTime()
	rf.ResetElectionTimeout()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
