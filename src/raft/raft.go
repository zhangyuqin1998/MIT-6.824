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
//	"bytes"
	"sync"
	"sync/atomic"
	"fmt"
	"time"
	"math/rand"
	"strconv"
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


type NodeState int
const (
	Follower 	NodeState = iota
	Canditater
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	lastActiveTime 	time.Time
	state			NodeState
	hBI	int

	currentTerm	int
	votedFor	int				// this voted peer's index into peers[]
	log			[]string		// commands

	commitIndex	int				// 最高的log的索引
	lastApplied	string			// 最高的log的命令

	nextIndex	[]int
	matchIndex	[]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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


type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		int
	CanditateId	int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoterId		int
	VoteGranted	bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term		int
	LeaderId	int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term		int
	Succeed		bool
	FollowerId	int
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	reply.Term = rf.currentTerm
	reply.VoterId = rf.me

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1) {
		fmt.Printf(rf.DebugString() + "received a vote request from %d, term %d, negetive\n", args.CanditateId, args.Term)
		reply.Term = rf.currentTerm
		return
	} else {
		fmt.Printf(rf.DebugString() + "received a vote request from %d, term %d, positive\n", args.CanditateId, args.Term)
		reply.VoteGranted = true 
		
		rf.state = Follower
		rf.votedFor = args.CanditateId
		rf.lastActiveTime = time.Now()

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		return
	}
}

func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply) {
	succeed := true
	reply.Term = rf.currentTerm
	if args.LeaderId == rf.me || args.Term < rf.currentTerm{
		succeed = false
		reply.Succeed = false
		reply.FollowerId = rf.me
	}
	
	if succeed {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		rf.state = Follower	//只要收到了心跳包，这个node就要变成follower
		reply.Term = rf.currentTerm
		reply.Succeed = true
		reply.FollowerId = rf.me
	}
	rf.lastActiveTime = time.Now()
}

func (rf *Raft) DebugString() string {
	var s string
	switch rf.state {
	case Leader:
		s = "Leader"
	case Canditater:
		s = "Canditater"
	case Follower:
		s = "Follower"
	}
	return "[Node " + strconv.Itoa(rf.me) + " term " + strconv.Itoa(rf.currentTerm) + " " + s + " ]:"
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

func (rf *Raft) SetState(s NodeState){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = s
}


//-----------------------------------------------------------------
type AppendEntriesRst struct {
	Reply 	AppendEntriesReply
	Succeed	bool
}

type RequestVoteRst struct {
	Reply 	RequestVoteReply
	Succeed	bool
}

func (rf *Raft) sendAppendEntriesSync(server int,  AppendEntriesRstCh chan AppendEntriesRst) {
	args := AppendEntriesArgs{
		Term:		rf.currentTerm,
		LeaderId:	rf.me,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)
	if reply.Succeed {
		AppendEntriesRstCh <- AppendEntriesRst{Reply: reply, Succeed: false}
	} else {
		AppendEntriesRstCh <- AppendEntriesRst{Reply: reply, Succeed: true}
	}
}

func (rf *Raft) sendRequestVoteSync(server int,  VoteRstCh chan RequestVoteRst) {
	args := RequestVoteArgs{
		Term:	rf.currentTerm,
		CanditateId:	rf.me,
	}
	reply := RequestVoteReply{}
	rf.sendRequestVote(server, &args, &reply)
	VoteRstCh <- RequestVoteRst{Reply: reply, Succeed: reply.VoteGranted}
}

func (rf *Raft)detectTimeOutSync(TimeOutCh chan bool){
	time.Sleep(time.Duration(int(float32(rf.hBI) * 0.9)) * time.Millisecond)
	TimeOutCh <- true
}
//-----------------------------------------------------------------

func (rf *Raft) HeartBeat() {
	cnt := 1
	timeOut := false
	AppendEntriesRstCh := make(chan AppendEntriesRst)
	TimeOutCh := make(chan bool)
	go rf.detectTimeOutSync(TimeOutCh)
	for p, _ := range rf.peers {
		if p == rf.me {
			continue
		} else {
			go rf.sendAppendEntriesSync(p, AppendEntriesRstCh)
		}
	}
	rst := AppendEntriesRst{}
	rf.lastActiveTime = time.Now()
	select {
	case rst = <- AppendEntriesRstCh:
		if rst.Succeed {
			cnt++
		}
		if rst.Reply.Term > rf.currentTerm {
			fmt.Printf(rf.DebugString() + "heart beat, but turn to foller, new term %d\n", rst.Reply.Term)
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
			return
		}
	case timeOut = <- TimeOutCh:
		fmt.Printf(rf.DebugString() + "heart beat, succeed cnt %d, time out %v\n", cnt, timeOut)
		return
	}
	fmt.Printf(rf.DebugString() + "heart beat, succeed cnt %d\n", cnt)
}


func (rf *Raft) BroadCast() {
	votes := 1
	timeOut := false
	VoteRstCh := make(chan RequestVoteRst)
	TimeOutCh := make(chan bool)
	go rf.detectTimeOutSync(TimeOutCh)
	for p, _ := range rf.peers {
		if p == rf.me {
			continue
		} else {
			go rf.sendRequestVoteSync(p, VoteRstCh)
		}
	}
	rst := RequestVoteRst{}
	rf.lastActiveTime = time.Now()
	for {
		select {
		case rst = <- VoteRstCh:
			if rst.Succeed {
				votes++
			} 
			if votes > len(rf.peers) / 2 {
				rf.mu.Lock()
				flag := rf.state == Canditater
				rf.mu.Unlock()
				if flag {
					rf.SetState(Leader)
					fmt.Printf(rf.DebugString() + "Canditate succeed, Votes:%d\n", votes)
					rf.HeartBeat()	//当选后立即发送heartbeat，通知其它人自己当选
					return
				} 
			}
		case timeOut = <- TimeOutCh:
			fmt.Printf(rf.DebugString() + "Canditate fail, time out %v, Votes:%d\n", timeOut, votes)
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
			return
		}
	}

}
//-----------------------------------------------------------------


func (rf *Raft) TimeOut() bool {
	return time.Now().Sub(rf.lastActiveTime) >= time.Duration(200+rand.Int31n(250)) * time.Millisecond
}

func (rf *Raft) ActFollower() {
	if rf.TimeOut() {
		rf.SetState(Canditater)
		fmt.Printf(rf.DebugString() + "time out, become a Canditator!\n")
	}
}

func (rf *Raft) ActCaditatern() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.lastActiveTime = time.Now()
	fmt.Printf(rf.DebugString() + "start a vote broadcast!\n")
	rf.BroadCast()
}

func (rf *Raft) ActLeader() {
	rf.lastActiveTime = time.Now()
	rf.HeartBeat()
}

//-----------------------------------------------------------------


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		s := rf.state
		rf.mu.Unlock()
		switch s {
		case Leader:
			rf.ActLeader()
		case Canditater:
			rf.ActCaditatern()
		case Follower:
			rf.ActFollower()
		}
		time.Sleep(time.Duration(rf.hBI)*time.Millisecond)
	}
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
	rf := &Raft{
		state:		Follower,
		votedFor : 	-1,
		hBI: 100,
	}
	rf.lastActiveTime = time.Now()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.supervise()

	return rf
}
