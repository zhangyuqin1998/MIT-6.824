package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func LOG(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Command  string
	Key		 string
	Value	 string
	ClientId int64
	Seq		 int
}

type KVServer struct {
	mu      sync.Mutex
	wrmu1 	sync.RWMutex
	wrmu3	sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	dataMap			map[string]string
	chanMap 		map[int64](chan Op)
	clientSeqMap	map[int64]int
}

func(kv *KVServer) GetData(k string) string{
	kv.wrmu1.Lock()
	defer kv.wrmu1.Unlock()
	v, ok := kv.dataMap[k]
	if !ok {
		return  ""
	} else {
		return v
	}
}

func (kv *KVServer) SetData(k,v string) {
	kv.wrmu1.Lock()
	defer kv.wrmu1.Unlock()
	kv.dataMap[k]=v
}

func(kv *KVServer) GetChan(idx int64) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.chanMap[idx]
	if !ok {
		return  nil
	} else {
		return v
	}
}

func (kv *KVServer) SetChan(idx int64, v chan Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.chanMap[idx]= v
}

func(kv *KVServer) GetClientSeq(idx int64) int{
	kv.wrmu3.Lock()
	defer kv.wrmu3.Unlock()
	v, ok := kv.clientSeqMap[idx]
	if !ok {
		return  -1
	} else {
		return v
	}
}

func (kv *KVServer) SetClientSeq(idx int64, v int) {
	kv.wrmu3.Lock()
	defer kv.wrmu3.Unlock()
	kv.clientSeqMap[idx]= v
}



func (kv *KVServer) SendOp(op Op) {
	kv.rf.Start(op)
	LOG("Server %d, Send %s Op from %d key %s value %s, Seq %d\n", kv.me, op.Command, op.ClientId, op.Key, op.Value, op.Seq)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.ServerId = kv.me
	reply.Value = ""

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.GetChan(args.ClientId) == nil {
		kv.SetChan(args.ClientId, make(chan Op))
	}
	op := Op {
		Command: "Get",
		Key:	 args.Key,
		ClientId:args.ClientId,
		Seq:	args.Seq,
	}
	kv.SendOp(op)
	select {
	case replyOp := <- kv.GetChan(args.ClientId) :
		if replyOp.Seq == args.Seq {
			reply.Err = OK
			reply.Value = kv.dataMap[args.Key]
			return
		}
	case <- time.After(1000 * time.Millisecond) :
		reply.Err = ErrWrongLeader
		LOG("Server %d timeout", kv.me)
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.ServerId = kv.me

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.GetChan(args.ClientId) == nil {
		kv.SetChan(args.ClientId, make(chan Op))
	}
	// kv.mu.Unlock()
	op := Op {
		Command: args.Op,
		Key:	 args.Key,
		Value:	 args.Value,
		ClientId:args.ClientId,
		Seq:	args.Seq,
	}
	kv.SendOp(op)
	select {
	case replyOp := <- kv.GetChan(args.ClientId) :
		if replyOp.Seq == args.Seq {
			reply.Err = OK
			return
		}
	case <- time.After(1000 * time.Millisecond) :
		reply.Err = ErrWrongLeader
		LOG("Server %d timeout", kv.me)
		return
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
	LOG("Server %d is killed\n", kv.me)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ticker() {
	for {
		msg := raft.ApplyMsg{}
		select {
		case msg = <- kv.applyCh:
			if !kv.killed() {
				kv.mu.Lock()
				op := msg.Command
				key := op.(Op).Key
				value := op.(Op).Value
				command := op.(Op).Command
				seq := op.(Op).Seq
				_, f := kv.rf.GetState()
				if seq > kv.GetClientSeq(op.(Op).ClientId) {
					LOG("Server %d get a op back from raft, op %s, key %s, value %s, ClientId %d, Seq %d\n", 
						kv.rf.GetMe(), command, key, value, op.(Op).ClientId, seq)
					kv.SetClientSeq(op.(Op).ClientId, seq)
					if command == "Put" {
						kv.SetData(key, value)
						if f {
							LOG("KEY %s, VALUE %s", key, kv.dataMap[key])
						}
					} else if command == "Append" {
						kv.SetData(key, kv.GetData(key) + value)
						if f {
							LOG("KEY %s, VALUE %s", key, kv.dataMap[key])
						}
					} else if command == "Get" {
						
					}
				} else {
					LOG("Server %d get a op back from raft, but Fail, op %s, key %s, value %s, ClientId %d, Seq %d\n", 
						kv.rf.GetMe(), command, key, value, op.(Op).ClientId, seq)
				}
				kv.mu.Unlock()
				// LOG("Server %d Beg channel to client %d\n", kv.me, op.(Op).ClientId)
				go func() {
					ch := kv.GetChan(op.(Op).ClientId)
					// kv.wrmu2.Lock()
					if ch != nil {
						ch <- op.(Op)
					}
				}()
				// kv.wrmu2.Unlock()
				// LOG("Server %d End channel to client %d\n", kv.me, op.(Op).ClientId)
			}
		}
		
	}
}

func (kv *KVServer) readFromLog() {
	// for 
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.chanMap = make(map[int64](chan Op))
	kv.dataMap = make(map[string]string)
	kv.clientSeqMap = make(map[int64]int)

	_, f := kv.rf.GetState()

	LOG("Server %d start, is Leader: %v!\n", me, f)

	kv.readFromLog()

	go kv.ticker()

	return kv
}
