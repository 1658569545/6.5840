package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int 	// 数值上和 logEntry的索引值相同

	// For 3D:
	SnapshotValid bool
	
	Snapshot      []byte 	//快照中kv数据
	SnapshotTerm  int 	//快照所包含的最后一个日志的任期
	SnapshotIndex int 	//快照所包含的最后一个日志的索引
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

	// 基本尊重Raft算法中的状态机
	// Server state
	state State

	// ------------------------------Persistent state on all servers
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


	// ------------------------------Volatile state on all servers
	// 提交日志的索引
	commitIndex int
	// 应用到状态机最后一条日志的索引
	lastApplied int

	// Volatile state on leaders
	// 对于每个follower，下一条要发送的日志条目的索引（初始化为领导者日志的最后一条索引 + 1）
	nextIndex []int
	// 对于每个follower，已知复制到该服务器的最高日志条目的索引（初始化为 0，单调递增）
	matchIndex []int

	// 与上层状态机联通的通道，用来应用Raft日志
	applyCh chan ApplyMsg
	applyCond *sync.Cond

	// 最后一次心跳时间
	lastHeartbeat time.Time

	// snapshot
	// 快照数据
	snapshot []byte
	// 是否使用了快照
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
		// 在leader中不需要进行持久化，因为voteFor、log、currentTerm已经在candidate中初始化过了
		rf.state = leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		length := len(rf.log)
		for i := range rf.peers {
			// 初始化nextIndex，初始化为leader的日志最后一条索引+1，其实就是长度
			rf.nextIndex[i] = length
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

// 给上层状态机用的
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
	return term, isleader
}
// +1
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == leader
}

/*
主要是获取日志条目的头尾条目的任期和索引，主要为日志压缩进行服务。
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
	更新已经提交的日志索引，只能提交当前任期的日志
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
		// 当前日志太过落后，因此直接将当前日志全部裁掉
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



/*
日志持久化：
	snapshot是新生成的快照数据，
	序列化
*/ 
func (rf *Raft) persist(snapshot []byte) {
	
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//依次编码，写入缓冲区
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	// 获取这一次持久化状态的大小
	raftstate := w.Bytes()

	if snapshot != nil {
		// 新的快照数据来了，重新保存
		rf.persister.Save(raftstate, snapshot)
	} else {
		// 快照数据不变，直接读取已经保存的旧快照，然后保存
		rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	}
}

/*
读取快照信息：
	反序列化
*/ 
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { 
		return
	}

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

快照应用：
	snapshot：是快照数据
	index：快照包含的最后一个数据的索引
*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 服务器的快照状态更新，因此无需更新快照状态
	if index <= rf.lastIncludedIndex {
		return
	}
	// 计算快照的相对位置
	logIndex := index - rf.getFirstLogIndex()
	rf.lastIncludedIndex = rf.log[logIndex].Index
	rf.lastIncludedTerm = rf.log[logIndex].Term
	// 现在保留index及之后的数据
	rf.log = rf.log[logIndex:]
	rf.snapshot = snapshot
	rf.persist(snapshot)
}

/*
	无锁的快照应用
*/
func (rf *Raft) SnapshotWithoutLock(index int, snapshot []byte) {
	// 服务器的快照状态更加新，因此无需更新快照状态
	if index <= rf.lastIncludedIndex {
		return
	}
	// 计算快照的相对位置
	logIndex := index - rf.getFirstLogIndex()
	// 更新快照状态
	rf.lastIncludedIndex = rf.log[logIndex].Index
	rf.lastIncludedTerm = rf.log[logIndex].Term
	// 保留logIndex及之后的数据
	rf.log = rf.log[logIndex:]
	rf.snapshot = snapshot
	rf.persist(snapshot)
}

/*
	快照应用给leader
*/
func (rf *Raft) SnapshotLeader(index int, snapshot []byte) {
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
1. 不投票的情况：
	1.1 我是leader
	1.2 我的任期高
	1.3 我已经投过票了
	1.4 候选人的日志没有我新
2. 投票的情况
	我的任期小，且候选人的日志比我的新
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader && rf.currentTerm >= args.Term {
		// 有一个有效的leader
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if (args.Term < rf.currentTerm) || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId && rf.votedFor != rf.me) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	} else if args.Term > rf.currentTerm && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 一定是任期更高的节点才能给他投票，因为candidate节点要自增term
		// 更新本节点任期并重置投票状态（votedFor=-1），为后续投票做准备。
		rf.currentTerm, rf.votedFor = args.Term, -1
	} else if rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) == false {
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
	// 冲突任期的第一个日志的索引
	FirstIndex int
}

/*
follower接受AppendEntries RPC请求之后进行处理的函数
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 收到心跳之后，重置计时器
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

	// 收到来自leader的AppendEntries请求，因此需要转为follower
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
	// 记录follower日志的任期
	follower_term := rf.log[args.PrevLogIndex - rf.getFirstLogIndex()].Term
	if args.PrevLogTerm != follower_term {
		// 以任期为单位进行回退
		for i := args.PrevLogIndex - 1; i >= rf.getFirstLogIndex(); i-- {
			if rf.log[i - rf.getFirstLogIndex()].Term != follower_term {
				reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, i + 1
				return
			}
		}
	}

	// 日志匹配成功，开始追加

	// 首先对follower的日志进行截断，只保留前PrevLooIndex个日志，这里使用getFirstLogIndex是解决快照
	rf.log = rf.log[:args.PrevLogIndex - rf.getFirstLogIndex() + 1]
	// 追加
	rf.log = append(rf.log, args.Entries...)
	reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, true, args.PrevLogIndex + len(args.Entries)
	// 更新commitIndex
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
	// 该快照包含的最后一个日志条目的索引值	
	LastIncludedIndex int
	// 该快照包含的最后一个日志条目的任期
	LastIncludedTerm int
	// 当前块在快照中的字节偏移量
	Offset int
	// 快照数据，从offset开始
	Data []byte
	// 如果这是最后一个块，则设置为true
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

/*
  follower接受InstallSnapshot RPC请求之后进行处理的函数
*/
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
	// 截断
	rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
	// 应用快照
	rf.SnapshotWithoutLock(args.LastIncludedIndex, args.Data)
	rf.applySnapshot = true
	rf.applyCond.Signal()
}

// ========================== RPC Callers ============================
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

/*
对每个服务器发送RequestVote RPC 来进行投票选举
*/
func (rf *Raft) broadcastRequestVote() {
	if rf.state != candidate {
		return
	}
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	receivedVotes := 1
	for i := range rf.peers {
		if i != rf.me {
			// 并发的发送RequestVote RPC
			go func (peer int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != candidate {
						return
					}
					if rf.currentTerm == args.Term {
						// 在这个过程中，任期没有改变
						if reply.VoteGranted {
							receivedVotes++
							if receivedVotes > len(rf.peers) / 2 {
								rf.stateChange(leader)
							}
						} else {
							// 发现任期更大的，那么就转为follower
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

/*
leader广播AppendEntries RPC
*/
func (rf *Raft) broadcastHeartBeat() {
	if rf.state != leader {
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			// 并行发送
			go func (peer int) {
				reply := &AppendEntriesReply{}
				rf.mu.Lock()
				// 状态检查
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				// 检查是否需要发送快照（leader有快照、且服务器日志落后于快照的时候，可以发送）
				if rf.lastIncludedIndex > 0 && rf.nextIndex[peer] <= rf.lastIncludedIndex {
					go rf.sendSnapshotTo(peer)
					rf.mu.Unlock()
					return
				}
				args := &AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
				}
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.PrevLogTerm = rf.log[rf.nextIndex[peer] - rf.getFirstLogIndex() - 1].Term
				// rf.log[2:] 表示从数组索引2开始到最后
				// 准备要复制的日志
				args.Entries = rf.log[rf.nextIndex[peer] - rf.getFirstLogIndex():]
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()
					// 状态并未改变
					if rf.state == leader && args.Term == rf.currentTerm {
						// 发现任期更大的，因此转为follower
						if reply.Term > rf.currentTerm {
							rf.currentTerm, rf.votedFor = reply.Term, -1
							rf.stateChange(follower)
							rf.persist(nil)
						} else {
							if reply.Success {
								if len(args.Entries) > 0 {
									// 更新nextIndex和matchIndex
									rf.nextIndex[peer] = args.Entries[len(args.Entries) - 1].Index + 1
									rf.matchIndex[peer] = rf.nextIndex[peer] - 1
								}
								// 尝试更新提交
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

/*
	leader发送快照数据给服务器peer
*/
func (rf *Raft) sendSnapshotTo(peer int) {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()
	if rf.state != leader {
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
	rf.mu.Unlock()
	if rf.sendInstallSnapshot(peer, args, reply) {
		rf.mu.Lock()
		if rf.state == leader && args.Term == rf.currentTerm {
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.stateChange(follower)
				rf.persist(nil)
			} else if args.LastIncludedIndex > rf.matchIndex[peer] {
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
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist(nil)
	}

	return index, term, isLeader
}

// 检查当前leader是否有当前任期的日志条目
// 有就返回true
func (rf *Raft) CheckEmptyTermLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) < 1 || rf.currentTerm == rf.getLastLogTerm() {
		return true
	} else {
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
/*
选举
*/
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.state == follower {
			// follower超时选举
			if time.Now().Sub(rf.lastHeartbeat) > time.Duration(200 + (rand.Int63() % 300)) * time.Millisecond {
				rf.stateChange(candidate)
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		} else if rf.state == leader {
			rf.broadcastHeartBeat()
		} else {
			// candidate超时选举
			if time.Now().Sub(rf.lastHeartbeat) > time.Duration(500 + (rand.Int63() % 300)) * time.Millisecond {
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		}
		rf.mu.Unlock()
		
		ms := 50 + rand.Int63() % 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

/*
	将日志提交到ApplyCh中，然后上层服务器读取管道中的日志，解析命令，执行相关操作。
*/
func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
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
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			//使用commitIndex而不是rf.commitIndex，因为rf.commitIndex可能在Unlock（）和Lock（）期间发生变化。
			//使用Max(rf；lastApplied, commitIndex)而不是直接使用commitIndex，以避免并发的InstallSnapshot rpc导致lastApplied回滚
			rf.lastApplied = max(rf.lastApplied, commitIndex)
		}
		rf.mu.Unlock()
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
	
	// 添加一个空项，来满足日志的一致性前驱检查
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

	

	return rf
}
