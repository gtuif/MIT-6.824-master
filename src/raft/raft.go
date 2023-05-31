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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"time"
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
type status int

type VoteTstatus int

type AppendEntriesState int

const (
	Leader status = iota
	Follower
	Candidate
)

const (
	HeartBeatOvertime = 35
	AppliedOvertime   = 15

	MinVoteTime  = 75
	PlusVoteTime = 100
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               //c set by Kill()

	currentterm int
	votedFor    int
	logs        []logEntry

	commitIndex int //提交的日志的下标
	lastapplied int

	LastLogIndex int
	LastLogTerm  int
	nextIndex    []int
	matchIndex   []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     status
	voteTimer time.Time
	votoNum   int

	applyChan chan ApplyMsg
}
type logEntry struct {
	Term    int
	Command interface{}
}

//-------------------------------------------------RPC调用参数---------------------------

type AppendEntriesArgs struct {
	Term         int        ///追加日志的任期
	LeaderId     int        // 追加日志的Lead
	PrevLogIndex int        //追加日志之前的logEntry的下标
	PrevLogTerm  int        //.....的任期
	Entries      []logEntry //追加的日志内容
	LeadCommit   int        // Leader的提交的操作的下标
}
type AppendEntriesReply struct {
	Term        int
	Successful  bool
	UpNextIndex int //更新的nextIndex
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term      int
	VoteGrand bool
}

type InstallSnapShotArgs struct {
	Term         int
	LeadId       int
	LastLogIndex int
	LastLogTerm  int
	Data         []byte
}

type InstallSnapShotReply struct {
	Term int
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.applyChan = applyCh

	rf.currentterm = 0
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastapplied = 0
	rf.votedFor = -1

	rf.LastLogTerm = 0
	rf.LastLogIndex = 0
	rf.logs = []logEntry{}
	rf.logs = append(rf.logs, logEntry{})
	rf.votoNum = 0

	// initialize from state persisted before a crash
	rf.mu.Unlock()
	rf.readPersist(persister.ReadRaftState())
	if rf.LastLogIndex > 0 {
		rf.lastapplied = rf.LastLogIndex
	}
	// start ticker goroutine to start elections
	go rf.electionTicker()

	go rf.appendTicker()

	go rf.commitTicker()

	return rf
}

//--------------------------------------------------ticker部分--------------
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowtime := time.Now()
		time.Sleep(time.Duration(generateOvertime(int64(rf.me))) * time.Millisecond)
		rf.mu.Lock()
		if rf.voteTimer.Before(nowtime) && rf.state != Leader {
			rf.state = Candidate
			rf.currentterm += 1
			rf.votedFor = rf.me
			rf.votoNum = 1

			rf.persist()

			rf.sendelection()
			rf.voteTimer = time.Now()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartBeatOvertime * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}

	}
}
func (rf *Raft) commitTicker() {
	for rf.killed() == false {
		time.Sleep(AppliedOvertime * time.Millisecond)
		rf.mu.Lock()

		//如果已经提交的日志的下标 大于 要提交的日志的下标
		if rf.lastapplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		Message := make([]ApplyMsg, 0)
		for rf.lastapplied < rf.commitIndex && rf.lastapplied < rf.getLastLogIndex() {
			rf.lastapplied++
			Message = append(Message, ApplyMsg{
				CommandValid:  true,
				Command:       rf.restorgeLog(rf.lastapplied).Command,
				CommandIndex:  rf.lastapplied,
				SnapshotValid: false,
			})
		}
		rf.mu.Unlock()

		for _, msg := range Message {
			rf.applyChan <- msg
		}
	}
}

//------------------------------------------------leader选举--------------------------------------------------------

func (rf *Raft) sendelection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentterm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)
			if res == true {
				rf.mu.Lock()
				if args.Term < rf.currentterm || rf.state != Candidate {
					rf.mu.Unlock()
					return
				}

				if reply.Term > args.Term {
					if reply.Term > rf.currentterm {
						rf.currentterm = reply.Term
					}
					rf.state = Follower
					rf.votedFor = -1
					rf.votoNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.VoteGrand == true && rf.currentterm == args.Term {
					rf.votoNum++
					if rf.votoNum >= len(rf.peers)/2+1 {
						rf.votoNum = 0
						rf.state = Leader
						rf.votedFor = -1
						rf.voteTimer = time.Now()
						rf.persist()
						rf.nextIndex = make([]int, len(rf.peers))
						for i, _ := range rf.nextIndex {
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastLogIndex()

						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentterm {
		reply.Term = rf.currentterm
		reply.VoteGrand = false
		return
	}

	reply.Term = rf.currentterm

	if args.Term > rf.currentterm {
		rf.currentterm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.votoNum = 0
		rf.persist()
	}

	if !rf.UptoDate(args.LastLogIndex, args.LastLogTerm) || args.Term == reply.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentterm
		reply.VoteGrand = false
		return
	} else {
		rf.votedFor = args.CandidateId
		rf.currentterm = args.Term
		rf.voteTimer = time.Now()

		reply.Term = rf.currentterm
		reply.VoteGrand = true

		rf.persist()

		return
	}
	return
}

//--------------------------------------------追加日志------------------------------------------

func (rf *Raft) leaderAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[server]-1 < rf.LastLogIndex {
				go rf.leadersendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			PrevLogIndex, PreLogTerm := rf.getPreLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentterm,
				LeaderId:     rf.me,
				PrevLogIndex: PrevLogIndex,
				PrevLogTerm:  PreLogTerm,
				LeadCommit:   rf.commitIndex,
			}

			if rf.getLastLogIndex() >= rf.nextIndex[server] {
				entries := make([]logEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.LastLogIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []logEntry{}
			}

			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			res := rf.sendAppendEntries(server, &args, &reply)

			if res == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader {
					return
				}

				if reply.Term > rf.currentterm {
					rf.currentterm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.votoNum = 0
					rf.persist()
					rf.voteTimer = time.Now()
					return
				}

				if reply.Successful == true {
					rf.commitIndex = rf.LastLogIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					for index := rf.getLastLogIndex(); index >= rf.LastLogIndex+1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						if sum >= len(rf.peers)/2+1 && rf.restorgeLogTerm(index) == rf.currentterm {
							rf.commitIndex = index
							break
						}
					}
				} else {
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}
		}(i)

	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentterm {
		reply.Successful = false
		reply.Term = rf.currentterm
		reply.UpNextIndex = -1
		return
	}

	reply.Successful = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.state = Follower
	rf.currentterm = args.Term
	rf.votoNum = 0
	rf.votedFor = -1
	rf.persist()
	rf.voteTimer = time.Now()

	if rf.LastLogIndex > args.PrevLogIndex {
		reply.Successful = false
		reply.UpNextIndex = rf.getLastLogIndex() + 1
		return
	}
	//	fmt.Printf("%v %v\n", rf.getLastLogIndex(), args.PrevLogIndex)
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.Successful = false
		reply.UpNextIndex = rf.getLastLogIndex()
		return
	} else {
		if rf.restorgeLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Successful = false
			tempTerm := rf.restorgeLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.LastLogIndex; index-- {
				if rf.restorgeLogTerm(index) != tempTerm {
					reply.UpNextIndex = index + 1
					break
				}
			}
			return
		}
	}
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.LastLogIndex], args.Entries...)
	rf.persist()

	if args.LeadCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeadCommit, rf.getLastLogIndex())
	}
	return
}

//------------------------------------持久化部分--------------------------------
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) PersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentterm)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.LastLogIndex)
	e.Encode(rf.LastLogTerm)
	data := w.Bytes()
	return data
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.PersistData()
	rf.persister.SaveRaftState(data)
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
	var currentTerm int
	var logs []logEntry
	var votefor int
	var LastlogIndex int
	var LastlogTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&logs) != nil || d.Decode(&votefor) != nil || d.Decode(&LastlogIndex) != nil || d.Decode(&LastlogTerm) != nil {
		fmt.Println("Decode error")
	} else {
		rf.currentterm = currentTerm
		rf.votedFor = votefor
		rf.logs = logs
		rf.LastLogIndex = LastlogIndex
		rf.LastLogTerm = LastlogTerm
	}
}

//------------------------------日志快照-------------------------------
func (rf *Raft) leadersendSnapShot(server int) {
	rf.mu.Lock()

	args := InstallSnapShotArgs{
		Term:         rf.currentterm,
		LeadId:       rf.me,
		LastLogIndex: rf.LastLogIndex,
		LastLogTerm:  rf.LastLogTerm,
		Data:         rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapShotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()

		if rf.state != Leader || rf.currentterm != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentterm {
			rf.state = Follower
			rf.votedFor = -1
			rf.votoNum = 0
			rf.persist()
			rf.voteTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastLogIndex
		rf.nextIndex[server] = args.LastLogIndex + 1

		rf.mu.Unlock()
		return
	}

}
func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()

	if rf.currentterm > args.Term {
		reply.Term = rf.currentterm
		rf.mu.Unlock()
		return
	}

	rf.currentterm = args.Term
	reply.Term = args.Term

	rf.state = Follower
	rf.votedFor = -1
	rf.votoNum = 0
	rf.persist()
	rf.voteTimer = time.Now()

	if rf.LastLogIndex >= args.LastLogIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastLogIndex
	templog := make([]logEntry, 0)
	templog = append(templog, logEntry{})

	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		templog = append(templog, rf.restorgeLog(i))
	}
	rf.logs = templog

	rf.LastLogIndex = args.LastLogIndex
	rf.LastLogTerm = args.LastLogTerm

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastapplied {
		rf.lastapplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.PersistData(), args.Data)
	Msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastLogTerm,
		SnapshotIndex: args.LastLogIndex,
	}
	rf.mu.Unlock()
	rf.applyChan <- Msg
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
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastLogIndex || index > rf.commitIndex {
		return
	}

	slogs := make([]logEntry, 0)
	slogs = append(slogs, logEntry{})

	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		slogs = append(slogs, rf.restorgeLog(i))
	}

	if index == rf.getLastLogIndex()+1 {
		rf.LastLogTerm = rf.getLastLogTerm()
	} else {
		rf.LastLogTerm = rf.restorgeLogTerm(index)
	}

	rf.LastLogIndex = index
	rf.logs = slogs

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastapplied {
		rf.lastapplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.PersistData(), snapshot)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentterm, rf.state == Leader
}

// example RequestVote RPC handler.
//

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastLogIndex() + 1
		term := rf.currentterm
		rf.logs = append(rf.logs, logEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		return index, term, true
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

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
