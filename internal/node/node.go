package node

import (
	"context"
	"fmt"
	"time"

	"sync/atomic"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
)

const (
	defaultTickInterval   = 100 * time.Millisecond
	defaultElectionTick   = 10
	defaultHeartbeatTick  = 1
	defaultMaxSizePerMsg  = 1024 * 1024
	defaultMaxInflightMsg = 256
	snapshotThreshold     = 64
)

// Node runs a single raft peer with local storage and optional gRPC peer transport.

type Options struct {
	// PeerAddrs maps raft node ID to gRPC dial address. If empty, runs single-node loopback.
	PeerAddrs map[uint64]string
}

type Node struct {
	id          uint64
	dataDir     string
	storage     *Storage
	engine      *engine.Engine
	raftNode    raft.Node
	transport   *Transport
	peerAddrs   map[uint64]string
	stopc       chan struct{}
	donec       chan struct{}
	lastApplied atomic.Uint64
}

// Opens storage and starts the raft node, tick loop, and Ready loop.
func NewNode(dataDir string, id uint64, opts Options) (*Node, error) {
	peerAddrs := opts.PeerAddrs
	if len(peerAddrs) == 0 {
		peerAddrs = map[uint64]string{id: ""}
	}

	storage, err := OpenStorage(dataDir, id)
	if err != nil {
		return nil, fmt.Errorf("node: open storage: %w", err)
	}

	engCfg := engine.DefaultConfig(dataDir)
	engCfg.SyncPolicy = engine.SyncAlways
	eng, err := engine.Open(engCfg)
	if err != nil {
		_ = storage.Close()
		return nil, fmt.Errorf("node: open engine: %w", err)
	}
	storage.SetEngine(eng)

	hs, _, err := storage.InitialState()
	if err != nil {
		_ = eng.Close()
		_ = storage.Close()
		return nil, fmt.Errorf("node: initial state: %w", err)
	}
	if hs.Commit > 0 {
		storage.SetAppliedIndex(hs.Commit)
	}

	cfg := raft.Config{
		ID:              id,
		ElectionTick:    defaultElectionTick,
		HeartbeatTick:   defaultHeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   defaultMaxSizePerMsg,
		MaxInflightMsgs: defaultMaxInflightMsg,
	}

	last, err := storage.LastIndex()
	if err != nil {
		_ = eng.Close()
		_ = storage.Close()
		return nil, fmt.Errorf("node: last index: %w", err)
	}

	var rn raft.Node
	if last == 0 {
		rn = raft.StartNode(&cfg, raftPeers(peerAddrs))
	} else {
		rn = raft.RestartNode(&cfg)
	}

	var transport *Transport
	if isCluster(peerAddrs) {
		transport = NewTransport(id, peerAddrs)
	}

	n := &Node{
		id:        id,
		dataDir:   dataDir,
		storage:   storage,
		engine:    eng,
		raftNode:  rn,
		transport: transport,
		peerAddrs: peerAddrs,
		stopc:     make(chan struct{}),
		donec:     make(chan struct{}),
	}
	if hs.Commit > 0 {
		n.lastApplied.Store(hs.Commit)
	}

	go n.tickLoop()
	go n.runReadyLoop()

	return n, nil
}

// Step applies an inbound raft message from a peer (or loopback).
func (n *Node) Step(ctx context.Context, msg raftpb.Message) error {
	return n.raftNode.Step(ctx, msg)
}

// Propose appends a command to the raft log. Data must be non-empty for RaftLog.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.raftNode.Propose(ctx, data)
}

func (n *Node) Engine() *engine.Engine {
	return n.engine
}

func (n *Node) LeaderID() uint64 {
	return n.Status().Lead
}

// LeaderAddr returns the gRPC address of the current leader, if known.
func (n *Node) LeaderAddr() string {
	lead := n.LeaderID()
	if lead == 0 {
		return ""
	}
	return n.peerAddrs[lead]
}

// Status returns the current raft status (leader, term, etc.).
func (n *Node) Status() raft.Status {
	return n.raftNode.Status()
}

// Storage returns the underlying raft storage.
func (n *Node) Storage() *Storage {
	return n.storage
}

func (n *Node) ID() uint64 { return n.id }

// Stop shuts down tick + Ready loops and closes storage.
func (n *Node) Stop() {
	select {
	case <-n.stopc:
	default:
		close(n.stopc)
	}

	n.raftNode.Stop()
	<-n.donec

	if n.transport != nil {
		_ = n.transport.Close()
	}
	_ = n.engine.Close()
	_ = n.storage.Close()
}
