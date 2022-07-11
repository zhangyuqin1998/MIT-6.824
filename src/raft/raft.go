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
	"bytes"
	// "unsafe"
	"6.824/labgob"
	"6.824/labrpc"
)

const DEBUG bool = false

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

type Log struct {
	Command interface{}
	Term	int
}

type SnapShot struct {
    BaseLog 	[]byte
    BaseIndex   int
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu       	 sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()
	applyCh     chan ApplyMsg

	Uuid		int64

	lastActiveTime 	time.Time
	state		NodeState
	hBI			int		// aka heartBeatInterval		

	currentTerm	int
	votedFor	int		// this voted peer's index into peers[]

	log			[]Log	// commands
	commitIndex	int		// 最高的log的索引
	lastApplied	int		// 最高的apply的log的索引

	nextIndex	[]int	//information of followers
	matchIndex	[]int	//information of followers

	snapShot	SnapShot	//存储快照
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) InitLeader() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range(rf.nextIndex) {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// e.Encode(rf.log[:rf.commitIndex])
	data := w.Bytes()
	LOG(rf.DebugString() + "persist starts, voteFor %d \n", rf.votedFor)
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// rf.commitIndex = len(rf.log)
	LOG(rf.DebugString() + "readPersist starts, voteFor%d \n", rf.votedFor)
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
	if index <= rf.snapShot.BaseIndex {
        return
    }

	//传入的index表示lastIncludeIndex
    rf.log = rf.log[index - rf.snapShot.BaseIndex :]
    rf.snapShot.BaseIndex = index
    rf.snapShot.BaseLog = snapshot
    // rf.saveStateAndSnapshot()
}


type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CanditateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoterId		int
	VoteGranted	bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term			int
	LeaderId		int

	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Log

	LeaderCommit	int		//Leader's commitIndex
	Uuid		    int64
}

type AppendEntriesState int
const (
	NoResponse AppendEntriesState = iota
	Succeed 
	FollowerFail
	FollowerFail_SU
	LeaderFail
)

type AppendEntriesReply struct {
	// Your data here (2A).
	Term		 int
	Succeed		 AppendEntriesState
	FollowerId	 int
	// LeaderCommit int
	FirstMatchIndex int
	Uuid		 int64
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	reply.Term = rf.currentTerm
	reply.VoterId = rf.me
	reply.VoteGranted = false
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 || args.LastLogTerm < lastLogTerm) {
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 ) {
		LOG(rf.DebugString() + "received a vote request from %d, term %d, negetive\n", args.CanditateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
		return
	} 
	
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	myLastLogTerm := -1
	if len(rf.log) > 0 {
		myLastLogTerm = rf.log[len(rf.log) - 1].Term
	}
	// if rf.votedFor == -1 && args.LastLogTerm >= myLastLogTerm &&  args.LastLogIndex >= rf.lastApplied{
	if rf.votedFor == -1 && (args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= len(rf.log))){
		LOG(rf.DebugString() + "received a vote request from %d, term %d, args.LastLogTerm %d, my LastLogTerm %d positive\n", 
					args.CanditateId, args.Term, args.LastLogTerm, myLastLogTerm)
		reply.VoteGranted = true 
		rf.votedFor = args.CanditateId

		rf.lastActiveTime = time.Now()
		rf.persist()
		return
	}
	LOG(rf.DebugString() + "received a vote request from %d, term %d, args.LastLogTerm %d, my LastLogTerm %d , votedFor %d, args.LastLogIndex %d, my LastLogIndex %d, negetive\n", 
				args.CanditateId, args.Term, args.LastLogTerm, myLastLogTerm, rf.votedFor, args.LastLogIndex, rf.lastApplied)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// LOG(rf.DebugString() + " received a Entries, leader %d term %d\n", args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Uuid = args.Uuid
	// reply.LeaderCommit = args.LeaderCommit
	if args.Term < rf.currentTerm{
		reply.Succeed = LeaderFail
		reply.FollowerId = rf.me
		LOG(rf.DebugString() + " received a Entries, LeaderFail commitIndex, leader %d term %d\n", args.LeaderId, args.Term)
		rf.persist()
		return
	}
	
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = Follower	//只要收到了心跳包，这个node就要变成follower
	reply.Term = rf.currentTerm
	reply.Succeed = Succeed
	reply.FollowerId = rf.me

	rf.lastActiveTime = time.Now()
	
	//即使Entries == nil, 也要进行一致性检验，不应该为hear beat设计单独的逻辑👇
	// if args.Entries == nil {
	// 	if args.LeaderCommit > rf.commitIndex {
	// 		rf.commitIndex = args.LeaderCommit
	// 		if len(rf.log) < rf.commitIndex {
	// 			rf.commitIndex = len(rf.log)
	// 		}
	// 	}
	// 	LOG(rf.DebugString() + " received a Entries, success, entries is nil, leader id %d, Entries len %d,  succeed\n", args.LeaderId, len(args.Entries))
	// 	return	//心跳包，之间return了
	// }

	// LOG(rf.DebugString() + " received a Entries, term %d, leaderId %d, PrevLogIndex %d, PrevLogTerm %d, LeaderCommit %d\n", 
	// 				args.Term, args.LeaderId, args.PrevLogIndex, 
	// 				args.PrevLogTerm, args.LeaderCommit)

	//重新看了下论文，不该有这个逻辑👇
	// if args.LeaderCommit < rf.commitIndex {
	// 	reply.Succeed = LeaderFail
	// 	LOG(rf.DebugString() + " received a Entries, LeaderFail commitIndex, leader id %d, PrevLogIndex %d, PrevLogTerm %d, Entries len %d\n", 
	// 				args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	// 	rf.persist()
	// 	return
	// }

	if len(rf.log) < args.PrevLogIndex {
		reply.Succeed = FollowerFail_SU
		reply.FirstMatchIndex = len(rf.log)

		LOG(rf.DebugString() + " received a Entries, FollowerFail PrevLogIndex, leader id %d, PrevLogIndex %d, PrevLogTerm %d, Entries len %d\n", 
					args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
		rf.persist()
		return
	}
	// 如果本地有前一个日志的话，那么term必须相同，否则false
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
		reply.Succeed = FollowerFail_SU
		FirstMatchIndex := args.PrevLogIndex - 1
		for idx, log := range(rf.log) {
			if log.Term == rf.log[args.PrevLogIndex - 1].Term {
				FirstMatchIndex = idx + 1
				break
			}
		}
		reply.FirstMatchIndex = FirstMatchIndex

		LOG(rf.DebugString() + " received a Entries, FollowerFail complex, leader id %d, PrevLogIndex %d, PrevLogTerm %d, my last log term %d, Entries len %d, FirstMatchIndex %d\n", 
					args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex - 1].Term, len(args.Entries), FirstMatchIndex)
		return
	}
	for i, logEntry := range args.Entries {
		curIndex := args.PrevLogIndex + i + 1
		if curIndex > len(rf.log) {
			rf.log = append(rf.log, logEntry)
		} else {	// 重叠部分
			if rf.log[curIndex - 1].Term != logEntry.Term {
				rf.log = rf.log[:curIndex - 1]		// 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry)	// 把新log加入进来
			}	// term一样啥也不用做，继续向后比对Log
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}
	
	LOG(rf.DebugString() + " received a Entries, leader id %d, Entries len %d, PrevLogIndex %d, PrevLogTerm %d,  succeed\n", args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)
	rf.persist()
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
	return "[Node " + strconv.Itoa(rf.me) + " term " + strconv.Itoa(rf.currentTerm) + " " + s + " LL " + strconv.Itoa(len(rf.log)) + " CI " + strconv.Itoa(rf.commitIndex) + " AI " + strconv.Itoa(rf.lastApplied) +" ]:"
}

func LOG(s string, a ...interface{}) {
	if DEBUG {
		fmt.Printf(s, a...)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if  rf.state != Leader {
		return len(rf.log), rf.currentTerm, false
	} else {
		// rf.commitIndex++
		log := Log{Command:command, Term:rf.currentTerm}
		// rf.lastApplied = log
		rf.log = append(rf.log, log)

		LOG(rf.DebugString() + "received from client, %s\n", command)
		// LOG(rf.DebugString() + "received from client\n")
		rf.persist()
		return len(rf.log), rf.currentTerm, true
	}
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
	Reply 		AppendEntriesReply
	Args		AppendEntriesArgs
	Succeed		AppendEntriesState
	ServerId	int
}

type RequestVoteRst struct {
	Reply 		RequestVoteReply
	Succeed		bool
}

// PrevLogIndex	int
// PrevLogTerm		int
// Entries			[]Log

// LeaderCommit	int		//Leader's commitIndex

func (rf *Raft) sendAppendEntriesSync(server int,  AppendEntriesRstCh chan AppendEntriesRst, Uuid int64) {
	args := AppendEntriesArgs{}
	// LOG(rf.DebugString() + "log len of leader is %d\n", len(rf.log))
	if len(rf.log) > 0 {
		Entries := make([]Log, 0)
		for i := rf.nextIndex[server] - 1; i < len(rf.log); i++ {
			Entries = append(Entries, rf.log[i])
		}
		args = AppendEntriesArgs{
			Term:		  rf.currentTerm,
			LeaderId:	  rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			Entries:	  Entries,
			LeaderCommit: rf.commitIndex,
			Uuid:		  Uuid,
		}
		if rf.nextIndex[server] - 1 > 0 {
			args.PrevLogTerm = rf.log[rf.nextIndex[server] - 1 - 1].Term
		} else {
			args.PrevLogTerm = -1
		}
	} else {
		args = AppendEntriesArgs{
			Term:		rf.currentTerm,
			LeaderId:	rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  -1,
			Entries:	  nil,
			LeaderCommit: 0,
			Uuid:		  Uuid,
		}
	}
	reply := AppendEntriesReply{}
	// LOG(rf.DebugString() + "sendAppendEntriesSync to %d, PrevLogIndex %d, PrevLogTerm %d, Entries len %d\n", server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	rf.sendAppendEntries(server, &args, &reply)
	AppendEntriesRstCh <- AppendEntriesRst{Reply: reply, Args: args, Succeed: reply.Succeed, ServerId: server}
}

func (rf *Raft) sendRequestVoteSync(server int,  VoteRstCh chan RequestVoteRst) {
	args := RequestVoteArgs{}
	// if rf.lastApplied > 0 && rf.lastApplied <= len(rf.log){
	if len(rf.log) > 0 {
		args = RequestVoteArgs{
			Term:			rf.currentTerm,
			CanditateId:	rf.me,
			LastLogIndex:	len(rf.log),
			LastLogTerm:	rf.log[len(rf.log) - 1].Term,
		}
	} else {
		args = RequestVoteArgs{
			Term:			rf.currentTerm,
			CanditateId:	rf.me,
			LastLogIndex:	0,
			LastLogTerm:	-1,
		}
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

func (rf *Raft) HeartBeat() bool {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.Uuid++
	Uuid := rf.Uuid
	cnt := 1
	timeOut := false
	AppendEntriesRstCh := make(chan AppendEntriesRst)
	TimeOutCh := make(chan bool)
	go rf.detectTimeOutSync(TimeOutCh)
	for p, _ := range rf.peers {
		if p == rf.me {
			continue
		} else {
			go rf.sendAppendEntriesSync(p, AppendEntriesRstCh, Uuid)
		}
	}
	rst := AppendEntriesRst{}
	rf.lastActiveTime = time.Now()
	for{
		select {
		case rst = <- AppendEntriesRstCh:
			if rst.Succeed == Succeed && rst.Reply.Uuid == Uuid{ //排除rpc失败的情况
				cnt++
				//日志同步成功
				LOG(rf.DebugString() + "heart beat, reach agreement with %d\n", rst.ServerId)
				rf.nextIndex[rst.ServerId] += len(rst.Args.Entries)
				rf.matchIndex[rst.ServerId] = rf.nextIndex[rst.ServerId] - 1
			} else  if rst.Succeed == FollowerFail && rst.Reply.Uuid == Uuid{
				//日志同步失败, next数组回退1
				LOG(rf.DebugString() + "heart beat, failed to reach agreement with %d, FollowerFail\n", rst.ServerId)
				rf.nextIndex[rst.ServerId]--
				if rf.nextIndex[rst.ServerId] < 1 {
					rf.nextIndex[rst.ServerId] = 1
				}
			} else if rst.Succeed == FollowerFail_SU && rst.Reply.Uuid == Uuid{
				//日志同步失败, 加速next数组回退
				rf.nextIndex[rst.ServerId] = rst.Reply.FirstMatchIndex
				if rf.nextIndex[rst.ServerId] < 1 {
					rf.nextIndex[rst.ServerId] = 1
				}
				LOG(rf.DebugString() + "heart beat, failed to reach agreement with %d, nextIndex turn to %d, FollowerFail_SU\n", rst.ServerId, rf.nextIndex[rst.ServerId])
			} else  if rst.Succeed == LeaderFail && rst.Reply.Uuid == Uuid{
				LOG(rf.DebugString() + "heart beat, failed to reach agreement with %d, LeaderFail, turn to follower\n", rst.ServerId)
				rf.state = Follower
				return false
			}
			if rst.Reply.Term > rf.currentTerm {
				LOG(rf.DebugString() + "heart beat, but trun to foller, new term %d\n", rst.Reply.Term)
				rf.state = Follower
				rf.currentTerm = rst.Reply.Term
				rf.persist()
				return false
			}
			if cnt > len(rf.peers) / 2 && rf.Uuid == Uuid{
				if len(rf.log) > 0 && rf.log[len(rf.log) - 1].Term == rf.currentTerm {
					rf.commitIndex = len(rf.log)
					LOG(rf.DebugString() + "heart beat, succeed early, cnt %d, commit %d succeed, uuid %d\n", cnt, rf.commitIndex, Uuid)
					rf.CommitAgreement()
				}
				return true
			}
		case timeOut = <- TimeOutCh:
			timeOut = false
			LOG(rf.DebugString() + "heart beat, succeed cnt %d, time out %v\n", cnt, timeOut)
			return false
		}
	}
}

func (rf *Raft) CommitAgreement() {
	AppendEntriesRstCh := make(chan AppendEntriesRst)
	for p, _ := range rf.peers {
		if p == rf.me {
			continue
		} else {
			go rf.sendAppendEntriesSync(p, AppendEntriesRstCh, -1)
		}
	}
}

func (rf *Raft) BroadCast() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	votes := 1
	reject := 0
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
			} else {
				reject++
				if (rst.Reply.Term > rf.currentTerm){
					rf.mu.Lock()
					rf.state = Follower
					rf.currentTerm = rst.Reply.Term
					rf.mu.Unlock()
					LOG(rf.DebugString() + "Canditate fail, bigger term %d, Votes:%d\n", rst.Reply.Term, votes)
					rf.persist()
					return 
				} else {
					LOG(rf.DebugString() + "get negetive vote from node %d, term %d\n", rst.Reply.VoterId, rst.Reply.Term)
				}
				// return
			}
			if votes > len(rf.peers) / 2 {
				flag := rf.state == Canditater
				if flag {
					rf.mu.Lock()
					rf.state = Leader
					rf.mu.Unlock()
					LOG(rf.DebugString() + "Canditate succeed, Votes:%d\n", votes)
					rf.InitLeader()

					// rf.mu.Unlock()
					rf.HeartBeat()	//当选后立即发送heartbeat，通知其它人自己当选
					// rf.mu.Lock()
					return
				} 
			} 
			if reject >= len(rf.peers) / 2 {
				rf.mu.Lock()
				rf.state = Follower
				rf.mu.Unlock()
				LOG(rf.DebugString() + "Canditate fail, Rejects:%d\n", reject)
				return
			}

		case timeOut = <- TimeOutCh:
			LOG(rf.DebugString() + "Canditate fail, time out %v, Votes:%d\n", timeOut, votes)
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
		// LOG(rf.DebugString() + "time out, become a Canditator!\n")
		rf.ActCaditatern()
	}
}

func (rf *Raft) ActCaditatern() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()
	rf.lastActiveTime = time.Now()
	LOG(rf.DebugString() + "start a vote broadcast!\n")
	rf.BroadCast()
}

func (rf *Raft) ActLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastActiveTime = time.Now()
	rf.HeartBeat()
}

//-----------------------------------------------------------------


// The TickerLoop go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) TickerLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		s := rf.state
		rf.mu.Unlock()
		switch s {
		case Leader:
			rf.ActLeader()
		// case Canditater:
		// 	rf.ActCaditatern()
		case Follower:
			rf.ActFollower()
		}
		time.Sleep(time.Duration(rf.hBI)*time.Millisecond)
	}
}

func (rf *Raft) ApplyLogLoop() {
	for !rf.killed(){
		// changed := false
		for rf.commitIndex > rf.lastApplied{
			// changed = true
			rf.lastApplied++
			msg := ApplyMsg{CommandValid:true, Command:rf.log[rf.lastApplied-1].Command, CommandIndex:rf.lastApplied}
			rf.persist()
			rf.applyCh <- msg
			LOG(rf.DebugString() + " ApplyLog %d, lastApplied %d, Command %s\n", msg.CommandIndex, rf.lastApplied, msg.Command)
			// switch msg.Command.(type) {
			// case int:
			// 	LOG(rf.DebugString() + " ApplyLog %d %d\n", msg.CommandIndex, 4)
			// case string:
			// 	LOG(rf.DebugString() + " ApplyLog %d %d\n", msg.CommandIndex, len(msg.Command.(string)))
			// }
		}
		time.Sleep(10 * time.Millisecond)
		// if changed {
			// rf.persist()
			// if rf.state == Leader {
			// 	rf.CommitAgreement()
			// }
		// }
	}
} 

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		state:		 Follower,
		votedFor : 	 -1,
		hBI: 		 80,
		commitIndex: 0,
		log:		make([]Log, 0),
		applyCh:	applyCh,
		lastApplied: 0,
		Uuid:		 0,
	}
	rf.lastActiveTime = time.Now()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start TickerLoop goroutine to start elections
	go rf.TickerLoop()
	go rf.ApplyLogLoop()
	// go rf.supervise()

	return rf
}
