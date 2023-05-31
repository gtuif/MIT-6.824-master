package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func min(num1 int, num2 int) int {
	if num1 > num2 {
		return num2
	} else {
		return num1
	}

}
func generateOvertime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(PlusVoteTime) + MinVoteTime
}

func (rf *Raft) UptoDate(index int, term int) bool {
	LastLogTerm := rf.getLastLogTerm()
	LastLogIndex := rf.getLastLogIndex()
	return term > LastLogTerm || (term == LastLogTerm && index >= LastLogIndex) //leader任期大于follwer或者任期相同但日志更长的
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs)-1 == 0 {
		return rf.LastLogTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1 + rf.LastLogIndex
}

func (rf *Raft) restorgeLog(curIndex int) logEntry {
	return rf.logs[curIndex-rf.LastLogIndex]
}

func (rf *Raft) restorgeLogTerm(curIndex int) int {
	if rf.LastLogIndex == curIndex {
		return rf.LastLogTerm
	} else {
		//	fmt.Printf("[GET] curIndex:%v,rf.lastLogIndex:%v\n", curIndex, rf.LastLogIndex)
		return rf.logs[curIndex-rf.LastLogIndex].Term
	}
}

func (rf *Raft) getPreLogInfo(server int) (int, int) {
	AppendEntriesIndex := rf.nextIndex[server] - 1
	LastIndex := rf.getLastLogIndex()
	if AppendEntriesIndex == LastIndex+1 {
		AppendEntriesIndex = LastIndex
	}
	return AppendEntriesIndex, rf.restorgeLogTerm(AppendEntriesIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
