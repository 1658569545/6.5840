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

// ========================== Imports ============================

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	//"github.com/linkdata/deadlock"
)

// ========================== Structs ============================
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	leader State = iota
	follower State = iota
	candidate State = iota
)

type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Server state
	state State

	// Persistent state on all servers
	// 当前任期
	currentTerm int
	// 给哪个候选人投票
	votedFor    int
	// 日志
	log		 []LogEntry

	// 快照包含的最后一个条目索引。
	lastIncludedIndex int
	// 快照包含的最后一条日志的任期。
	lastIncludedTerm int

	// Volatile state on all servers
	// 提交
	commitIndex int
	// 应用到状态机
	lastApplied int

	// Volatile state on leaders
	// 对于每个服务器，下一条要发送的日志条目的索引（初始化为领导者日志的最后一条索引 + 1）
	nextIndex []int
	// 对于每个服务器，已知复制到该服务器的最高日志条目的索引（初始化为 0，单调递增）
	matchIndex []int

	// Channel
	applyCh chan ApplyMsg
	applyCond *sync.Cond

	// 最后一次心跳时间
	lastHeartbeat time.Time

	// snapshot
	// 快照数据
	snapshot []byte
	applySnapshot bool
}

// ========================== Lib Function ============================
func max(int1 int, int2 int) int {
	if int1 > int2 {
		return int1
	} else {
		return int2
	}
}

func min(int1 int, int2 int) int {
	if int1 < int2 {
		return int1
	} else {
		return int2
	}
}

func (rf *Raft) resetTimer() {
	rf.lastHeartbeat = time.Now()
}

/*

在发生状态的时候要进行持久化，从而符合3C的要求
状态转变：
	1. 转变为follower
		需要重置votedFor and state
	2. 转变为candidate
		currentTerm + 1, votedFor = me 
	3. 转变为leader
		初始化nextIndex and matchIndex
*/
func (rf *Raft) stateChange(newState State) {
	rf.resetTimer()
	if rf.state == newState {
		return
	} 
	if newState == leader {
		rf.state = leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		newIndex := len(rf.log)
		for i := range rf.peers {
			// 初始化为领导者日志的最后一条索引+1，因为log数组里面有个空项，因此无需+1了。
			rf.nextIndex[i] = newIndex
			rf.matchIndex[i] = 0
		}
	} else if newState == candidate {
		rf.state = candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist(nil)
	} else {
		rf.state = follower
		rf.votedFor = -1
		rf.persist(nil)
	}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == leader
}

/*
主要是获取日志条目的头尾条目的任期和索引，主要为日志压缩进行服务。
	同时在初始化的时候，会放一个空项，放空项的一个好处是因为有prev遍历，不需要特殊判断前缀
*/
func (rf *Raft) getFirstLogIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) getFirstLogTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

/* 
更新已经提交的日志索引	
	具体逻辑为：从最后一个日志条目开始判断，如果条目任期与当前任期相同，且被能与大多数match，因此就更新commitIndex
	注意，在这里判断的时候，需要判断任期，因为leader不能直接提交旧任期日志条目，只能提交当前任期，详细见论文图8。
*/
func (rf *Raft) updateCommitIndex() bool {
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N - rf.getFirstLogIndex()].Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers) / 2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			return true
		}
	}
	return false
}

/*
比较谁更新：
	任期大的更新，任期相同索引更大的更新。
	传进来的新，就返回true
	论文Section 5.4
*/
func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	return term > lastLogTerm || (term == lastLogTerm && index >= lastLogIndex)
}

/*
日志裁剪：
	主要用于日志压缩，丢弃索引在index之前的日志。
	注意：只能对已经提交的日志进行裁剪
*/
func (rf *Raft) trimLog(index int, term int) {
	if index >= rf.getLastLogIndex() {
		// 当前日志太过落后，因此直接将当前日志全部减掉
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Index: index, Term: term}
	} else if index >= rf.getFirstLogIndex() {
		// 创建一个长度为1的数组，然后log[4:]代表将索引4和之后索引的元素放入新的数组中
		// 在这里就是将index+1及其之后的索引放入数组中
		rf.log = append(make([]LogEntry, 1), rf.log[index + 1 - rf.getFirstLogIndex():]...)
		rf.log[0] = LogEntry{Index: index, Term: term}
	} else {
		return
	}
	// 更新应用提交相关
	rf.lastApplied = index
	rf.commitIndex = index
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// should be in lock
/*
日志持久化：
	snapshot是新生成的快照数据
*/ 
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// 使用labgob编码器保存数据
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	if snapshot != nil {
		// 新的快照数据来了，重新保存
		rf.persister.Save(raftstate, snapshot)
	} else {
		// 快照数据不变，直接读取已经保存的旧快照，然后保存
		rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	}
}


// restore previously persisted state.
// should be in lock
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil || d.Decode(&rf.lastIncludedIndex) != nil || d.Decode(&rf.lastIncludedTerm) != nil {
		log.Fatalf("Server %v %p (Term: %v) readPersist error", rf.me, rf, rf.currentTerm)
	} else {
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
/*
快照：
	snapshot：是快照数据
	index：快照包含的最后一个数据的索引
*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		// 快照落后了
		return
	}
	// 计算快照在log中的位置
	logIndex := index - rf.getFirstLogIndex()
	rf.lastIncludedIndex = rf.log[logIndex].Index
	rf.lastIncludedTerm = rf.log[logIndex].Term
	// 现在保留index及之后的数据
	rf.log = rf.log[logIndex:]
	rf.snapshot = snapshot
	rf.persist(snapshot)
}

func (rf *Raft) SnapshotWithoutLock(index int, snapshot []byte) {
	// Your code here (3D).
	DPrintf("Server %v %p (Term: %v) Snapshot: %v", rf.me, rf, rf.currentTerm, index)
	if index <= rf.lastIncludedIndex {
		// 快照落后了
		return
	}
	logIndex := index - rf.getFirstLogIndex()
	rf.lastIncludedIndex = rf.log[logIndex].Index
	rf.lastIncludedTerm = rf.log[logIndex].Term
	rf.log = rf.log[logIndex:]
	rf.snapshot = snapshot
	rf.persist(snapshot)
}

/*
leader创建快照
*/
func (rf *Raft) SnapshotLeader(index int, snapshot []byte) {
	// Your code here (3D).
	logIndex := index - rf.getFirstLogIndex()
	rf.lastIncludedIndex = rf.log[logIndex].Index
	rf.lastIncludedTerm = rf.log[logIndex].Term
	rf.log = rf.log[logIndex:]
	rf.snapshot = snapshot
	rf.persist(snapshot)
}

// ========================== RPC responsers ============================
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// 候选人任期
	Term int
	// 候选人的 Id
	CandidateId int
	// 最后日志的索引
	LastLogIndex int
	// 最后日志的任期
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (3A).
	// 该服务器任期
	Term int
	// 该服务器是否投票给候选人ID
	VoteGranted bool
}

/*
服务器收到 RequestVote RPC 请求之后进行处理的函数
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader && rf.currentTerm >= args.Term {
		// leader不响应
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if (args.Term < rf.currentTerm) || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId && rf.votedFor != rf.me) {
		// 候选者任期落后、我已经投票，不响应
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	} else if args.Term > rf.currentTerm && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 候选者更高任期且日志较新​​：
		// 更新本节点任期并重置投票状态（votedFor=-1），为后续投票做准备。
		rf.currentTerm, rf.votedFor = args.Term, -1
	} else if rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) == false {
		// 候选者日志不够新​，拒绝投票
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.stateChange(follower)
	rf.persist(nil)
}

// --- AppendEntries ---
// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	FirstIndex int
}

/*
服务器接受AppendEntries RPC请求之后进行处理的函数
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetTimer()
	// 判断任期
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, rf.getLastLogIndex() + 1
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist(nil)
	}

	rf.stateChange(follower)

	// 判断日志索引是否匹配
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, rf.getLastLogIndex() + 1
		return
	}

	if args.PrevLogIndex < rf.getFirstLogIndex() {
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, -1
		return
	}

	// 一致性检查
	match_term := rf.log[args.PrevLogIndex - rf.getFirstLogIndex()].Term
	if args.PrevLogTerm != match_term {
		for i := args.PrevLogIndex - 1; i >= rf.getFirstLogIndex(); i-- {
			if rf.log[i - rf.getFirstLogIndex()].Term != match_term {
				reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, i + 1
				return
			}
		}
	}

	rf.log = rf.log[:args.PrevLogIndex - rf.getFirstLogIndex() + 1]
	rf.log = append(rf.log, args.Entries...)
	reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, true, args.PrevLogIndex + len(args.Entries)
	
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}
	rf.persist(nil)
}

// --- Install Snapshot ---
type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.stateChange(follower)
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist(nil)
	}
	reply.Term = rf.currentTerm
	rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.SnapshotWithoutLock(args.LastIncludedIndex, args.Data)
	rf.applySnapshot = true
	rf.applyCond.Signal()
}

// ========================== RPC Callers ============================

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// shall be called within lock: at least we can promise that the state is not changed until all goroutines launchedd
// broadcast requestvote to all peers
func (rf *Raft) broadcastRequestVote() {
	DPrintf("Server %v %p (Term: %v) broadcast RequestVote", rf.me, rf, rf.currentTerm)
	if rf.state != candidate {
		DPrintf("Invalid state for broadcastRequestVote: %v", rf.state)
		return
	}
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	// votes counter, including itself
	receivedVotes := 1
	for i := range rf.peers {
		if i != rf.me {
			// send to all other peers
			go func (peer int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != candidate {
						return
					}
					if rf.currentTerm == args.Term {
						// make sure request valid
						if reply.VoteGranted {
							// someone vote us
							receivedVotes++
							DPrintf("Server %v %p (Term: %v) received vote from %v, receivedVotes: %v", rf.me, rf, rf.currentTerm, peer, receivedVotes)
							if receivedVotes > len(rf.peers) / 2 {
								rf.stateChange(leader)
							}
						} else {
							// someone newer than us
							if reply.Term > rf.currentTerm {
								rf.currentTerm, rf.votedFor = reply.Term, -1
								rf.stateChange(follower)
								rf.persist(nil)
							}
						}
					}
				}
			} (i)
		}
	}
}

// shall be called within lock: at least we can promise that the state is not changed until all goroutines launched
// broadcast heartbeat to all peers
// broadcast heartbeat also do send append entry work
func (rf *Raft) broadcastHeartBeat() {
	DPrintf("Server %v %p (Term: %v) broadcast HeartBeat", rf.me, rf, rf.currentTerm)
	if rf.state != leader {
		DPrintf("Server %v %p (Term: %v) Invalid state for broadcastHeartBeat: %v", rf.me, rf, rf.currentTerm, rf.state)
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			go func (peer int) {
				reply := &AppendEntriesReply{}
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				if rf.lastIncludedIndex > 0 && rf.nextIndex[peer] <= rf.lastIncludedIndex {
					go rf.sendSnapshotTo(peer)
					rf.mu.Unlock()
					return
				}
				args := &AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
				}
				DPrintf("Server %v %p (Term: %v) getFirstLogIndex: %v nextIndex: %v rf.log: %v", rf.me, rf, rf.currentTerm, rf.getFirstLogIndex(), rf.nextIndex[peer], rf.log)
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.PrevLogTerm = rf.log[rf.nextIndex[peer] - rf.getFirstLogIndex() - 1].Term
				args.Entries = rf.log[rf.nextIndex[peer] - rf.getFirstLogIndex():]
				args.LeaderCommit = rf.commitIndex
				DPrintf("Server %v %p (Term: %v) send AppendEntries to %v, args: %v", rf.me, rf, rf.currentTerm, peer, args)
				rf.mu.Unlock()
				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()
					if rf.state == leader && args.Term == rf.currentTerm {
						if reply.Term > rf.currentTerm {
							rf.currentTerm, rf.votedFor = reply.Term, -1
							rf.stateChange(follower)
							rf.persist(nil)
						} else {
							if reply.Success {
								DPrintf("Server %v %p (Term: %v) heartbeat success to %v", rf.me, rf, rf.currentTerm, peer)
								if len(args.Entries) > 0 {
									rf.nextIndex[peer] = args.Entries[len(args.Entries) - 1].Index + 1
									rf.matchIndex[peer] = rf.nextIndex[peer] - 1
									DPrintf("Server %v %p (Term: %v) peer: %v, nextIndex: %v matchIndex: %v", rf.me, rf, rf.currentTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer])
								}
								rf.updateCommitIndex()
							} else {
								rf.nextIndex[peer] = min(reply.FirstIndex, rf.getLastLogIndex())
							}
						}
					}
					rf.mu.Unlock()
				}
			} (i)
		}
	
	}
}

// shall be called as go routine
func (rf *Raft) sendSnapshotTo(peer int) {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()
	if rf.state != leader {
		DPrintf("Server %v %p (Term: %v) Invalid state for broadcastSnapshot: %v", rf.me, rf, rf.currentTerm, rf.state)
		return
	}
	args := &InstallSnapshotArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Offset: 0,
		Data: rf.snapshot,
		Done: true,
	}
	DPrintf("Server %v %p (Term: %v) send Snapshot to %v, args: %v", rf.me, rf, rf.currentTerm, peer, args)
	rf.mu.Unlock()
	if rf.sendInstallSnapshot(peer, args, reply) {
		rf.mu.Lock()
		if rf.state == leader && args.Term == rf.currentTerm {
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.stateChange(follower)
				rf.persist(nil)
			} else if args.LastIncludedIndex > rf.matchIndex[peer] {
				DPrintf("Server %v %p (Term: %v) Snapshot to %v done, args.LastIncludedIndex: %v, rf.matchIndex[peer]: %v", rf.me, rf, rf.currentTerm, peer, args.LastIncludedIndex, rf.matchIndex[peer])
				rf.matchIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.updateCommitIndex()
			}
		}
		rf.mu.Unlock()
	}
}

// ========================== Start ============================

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		isLeader = false
	} else {
		// append log
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist(nil)
		// rf.broadcastHeartBeat()
	}

	return index, term, isLeader
}

func (rf *Raft) CheckEmptyTermLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) < 1 || rf.currentTerm == rf.getLastLogTerm() {
		return true
	} else {
		// log.Printf("Server %v %p (Term: %v) CheckEmptyTermLog: %v %v", rf.me, rf, rf.currentTerm, rf.currentTerm, rf.getLastLogTerm())
		return false
	}
}

// ========================== Extra ============================

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ========================== go routines ============================

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == follower {
			// if the server is follower
			if time.Now().Sub(rf.lastHeartbeat) > time.Duration(200 + (rand.Int63() % 300)) * time.Millisecond {
				// we shall detect the time gap since we receive leader's last heartbeat, too long means the leader is dead, we should start an election
				DPrintf("Server %v %p (Term: %v) is now a candidate, duration: %v", rf.me, rf, rf.currentTerm, time.Now().Sub(rf.lastHeartbeat))
				rf.stateChange(candidate)
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		} else if rf.state == leader {
			// if the server is leader, broadcast heartbeat time by time
			rf.broadcastHeartBeat()
		} else {
			if time.Now().Sub(rf.lastHeartbeat) > time.Duration(500 + (rand.Int63() % 300)) * time.Millisecond {
				// Way 1:
				// if the server is candidate but disconnected, when it come back it should return to normal state
				// successfully
				// rf.stateChange(follower)
				// rf.resetTimer()
				// Way 2:
				// if the server is candidate but disconnected, when it come back it should continue the election
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + rand.Int63() % 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		DPrintf("Server %v %p (Term: %v) rf.applySnapshot: %v; applyLog: %v, %v, %v", rf.me, rf, rf.currentTerm, rf.applySnapshot, rf.lastApplied, rf.commitIndex, rf.log)
		if rf.applySnapshot {
			rf.applySnapshot = false
			msg := ApplyMsg{
				CommandValid: false,
				SnapshotValid: true,
				Snapshot: rf.snapshot,
				SnapshotTerm: rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex {
			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command: rf.log[i - rf.getFirstLogIndex()].Command,
					CommandIndex: i,
				})
			}
			DPrintf("Server %v %p (Term: %v) applyLog: %v, %v, %v, %v", rf.me, rf, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.log, msgs)
			commitIndex := rf.commitIndex
			server, term := rf.me, rf.currentTerm
			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- msg
				DPrintf("Server %v %p (Term %v) apply to %v", server, rf, term, msg)
			}
			rf.mu.Lock()
			DPrintf("Server %v %p (Term %v) successfly commit to %v", rf.me, rf, rf.currentTerm, commitIndex)
			rf.lastApplied = max(rf.lastApplied, commitIndex)
		}
		rf.mu.Unlock()
	}
}

// We cannot use this, other tests not support snapshot mechanism
func (rf *Raft) monitorLogSize(threshold int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if len(rf.log) > threshold {
			DPrintf("Server %v %p (Term: %v) log size: %v, threshold: %v", rf.me, rf, rf.currentTerm, len(rf.log), threshold)
			rf.SnapshotWithoutLock(rf.lastApplied, rf.persister.ReadSnapshot())
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// ========================== Main ============================

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 0)
	// we use a bubble log to simplify the initial case, since the requests have a prev index argument
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil})

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
	
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.applySnapshot = false

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

	// go rf.monitorLogSize(100)

	return rf
}
