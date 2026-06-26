package node

import (
	"time"

	"go.etcd.io/raft/v3"
)

const (
	defaultTickInterval   = 100 * time.Millisecond
	defaultElectionTick   = 10
	defaultHeartbeatTick  = 1
	defaultMaxSizePerMsg  = 1024 * 1024
	defaultMaxInflightMsg = 256
)

// Node runs a single raft peer with local storage and an in-process transport (Step loopback).

type Node struct {
	id       uint64
	dataDir  string
	storage  *Storage
	raftNode raft.Node
	stopc    chan struct{}
	donec    chan struct{}
}
