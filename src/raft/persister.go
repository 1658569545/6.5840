package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	// 保存Raft协议的持久化状态（例如日志、当前任期等）。
	raftstate []byte
	// 保存kv数据
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

// 复制一个切片
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

// 浅拷贝
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

// 读取raftstate信息
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

// 返回Raft协议的持久化状态的大小
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// 保存
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

// 读取snapshot信息
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

// 返回snapshot的大小
func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
