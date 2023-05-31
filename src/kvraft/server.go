package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	SeqId    int
	ClientId int64
	Index    int
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap        map[int64]int   //ClientId / SeqId
	waitRaftChMsg map[int]chan Op //Index / Op
	kvMap         map[string]string

	lastIncludeIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:   "Get",
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getmapCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.waitRaftChMsg, op.Index)
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}
func (kv *KVServer) getmapCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitRaftChMsg[index]
	if !ok {
		kv.waitRaftChMsg[index] = make(chan Op, 1)
		ch = kv.waitRaftChMsg[index]
	}
	return ch
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:   args.Op,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
		Key:      args.Key,
		Value:    args.Value,
	}
	lastindex, _, _ := kv.rf.Start(op)

	ch := kv.getmapCh(lastindex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitRaftChMsg, op.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.SeqId != op.SeqId || replyOp.ClientId != op.ClientId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.waitRaftChMsg = make(map[int]chan Op)
	kv.kvMap = make(map[string]string)

	kv.lastIncludeIndex = -1

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsgHandLoop()
	return kv
}
func (kv *KVServer) applyMsgHandLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex
				if index <= kv.lastIncludeIndex {
					return
				}
				op := msg.Command.(Op)
				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvMap[op.Key] = op.Value
					case "Append":
						kv.kvMap[op.Key] += op.Value
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}
				if kv.maxraftstate != -1 && kv.rf.GetRaftstateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(index, snapshot)
				}
				kv.getmapCh(index) <- op
			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) ifDuplicate(ClientId int64, SeqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastindex, exist := kv.seqMap[ClientId]
	if !exist {
		return false
	}
	return SeqId <= lastindex
}
func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvMap) != nil && d.Decode(&seqMap) != nil {
		kv.kvMap = kvMap
		kv.seqMap = seqMap
	}
}
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}
