package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
/*
func min(a,b int) int {
	if a>b{
		return b
	}
	return a
}
func max(a,b int) int {
	if a>b{
		return a
	}
	return b
}*/