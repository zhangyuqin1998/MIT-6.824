package kvraft

import(
	"6.824/labrpc"
 	"crypto/rand"
 	"math/big"
 	"time"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	me		int64
	mu      sync.Mutex

	leaderIdCache	int

	seq		int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.me = nrand()
	ck.servers = servers
	// You'll have to add code here.
	return ck
}


func (ck *Clerk) SendGetRequest(serverId int, 
								replyCh chan GetReply, 
								args GetArgs, 
								reply GetReply) {
	ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
	if ok {
		replyCh <- reply
	}
}

func (ck *Clerk) SendPutAppendRequest(serverId int, 
	replyCh chan PutAppendReply, 
	args PutAppendArgs, 
	reply PutAppendReply) {
	// LOG("client %d sent seq %d to server %d", ck.me, ck.seq, serverId)
	ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
	if ok {
	replyCh <- reply
	}
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{key, ck.me, ck.seq}
	serverId := ck.leaderIdCache
	
	replyCh := make(chan GetReply)

	LOG("Client %d Send Op %s , key %s, seq %d", ck.me, "Get", key, ck.seq)
	for {
		reply := GetReply{}
		go ck.SendGetRequest(serverId, replyCh, args, reply)

		select {
		case <- time.After(1000 * time.Millisecond) :
			serverId = (serverId + 1) % len(ck.servers)
			LOG("Client %d time out, waiting for seq %d, change server from %d to %d!", ck.me, ck.seq, serverId, (serverId + 1) % len(ck.servers))
			break
		case reply = <- replyCh:
			if reply.Err == OK{
				ck.leaderIdCache = reply.ServerId
				ck.seq++
				LOG("Client %d receive Op %s from %d, key %s, value %s", ck.me, "Get", reply.ServerId, key, reply.Value)
				return reply.Value
				} else {
				LOG("Client %d receive Fail %s from %d, key %s, seq %d, change server from %d to %d", ck.me, "Get", reply.ServerId, key, ck.seq, serverId, (serverId + 1) % len(ck.servers))
				serverId = (serverId + 1) % len(ck.servers)
				break
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{key, value, ck.me, op, ck.seq}
	serverId := ck.leaderIdCache
	LOG("Client %d Send Op %s , key %s, value %s, seq %d", ck.me, op, key, value, ck.seq)

	replyCh := make(chan PutAppendReply)
	for {
		reply := PutAppendReply{}
		go ck.SendPutAppendRequest(serverId, replyCh, args, reply)

		select {
		case <- time.After(1000 * time.Millisecond) :
			LOG("Client %d time out, waiting for seq %d, change server from %d to %d!", ck.me, ck.seq, serverId, (serverId + 1) % len(ck.servers))
			serverId = (serverId + 1) % len(ck.servers)
			break
		case reply = <- replyCh:
			if reply.Err == OK{
				ck.leaderIdCache = reply.ServerId
				ck.seq++
				LOG("Client %d receive Op %s from %d, key %s, value %s", ck.me, op, reply.ServerId, key, value)
				return
			} else {
				LOG("Client %d receive Fail %s from %d, key %s, seq %d, change server from %d to %d", ck.me, "Get", reply.ServerId, key, ck.seq, serverId, (serverId + 1) % len(ck.servers))
				serverId = (serverId + 1) % len(ck.servers)
				break
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
