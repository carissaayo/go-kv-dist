package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"sync/atomic"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
	"github.com/carissaayo/go-kv-dist/internal/kv"
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

// tick loop for maintaining leadership and for election
func (n *Node) tickLoop() {
	ticker := time.NewTicker(defaultTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopc:
			return
		case <-ticker.C:
			n.raftNode.Tick()
		}
	}
}

// The main raft loop where etcd sends work on Ready()
func (n *Node) runReadyLoop() {
	defer close(n.donec)

	for {
		select {
		case <-n.stopc:
			return
		case rd := <-n.raftNode.Ready():
			if err := n.processReady(rd); err != nil {
				log.Printf("node %d: process ready: %v", n.id, err)
				return
			}
			n.raftNode.Advance()
		}
	}
}

// Handles one raft.Ready batch — persist, apply, send messages.
func (n *Node) processReady(rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.installSnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("install snapshot: %w", err)
		}
	}

	if len(rd.Entries) > 0 {
		if err := n.storage.Append(rd.Entries); err != nil {
			return fmt.Errorf("append entries: %w", err)
		}
	}

	if !raft.IsEmptyHardState(rd.HardState) {
		if err := n.storage.SaveHardState(rd.HardState); err != nil {
			return fmt.Errorf("save hard state: %w", err)
		}
	}

	// Phase 1: committed entries are not applied to the KV engine yet.
	for _, ent := range rd.CommittedEntries {
		if err := n.applyCommitted(ent); err != nil {
			return fmt.Errorf("apply committed entry %d: %w", ent.Index, err)
		}
	}

	for _, msg := range rd.Messages {
		if err := n.sendRaftMessage(msg); err != nil {
			return fmt.Errorf("send message: %w", err)
		}
	}

	n.reportSnapshots(rd.Messages)

	if err := n.maybeCompact(); err != nil {
		return fmt.Errorf("compact: %w", err)
	}

	return nil
}

func (n *Node) installSnapshot(snap raftpb.Snapshot) error {
	data, err := kv.DecodeState(snap.Data)
	if err != nil {
		return fmt.Errorf("decode snapshot state: %w", err)
	}
	if err := engine.RestoreSnapshot(n.engine, data); err != nil {
		return fmt.Errorf("restore engine: %w", err)
	}
	if err := n.storage.ApplySnapshot(snap); err != nil {
		return err
	}
	n.lastApplied.Store(snap.Metadata.Index)
	n.storage.SetAppliedIndex(snap.Metadata.Index)
	return nil
}

func (n *Node) reportSnapshots(msgs []raftpb.Message) {
	for _, msg := range msgs {
		if msg.Type == raftpb.MsgSnap {
			n.raftNode.ReportSnapshot(msg.To, raft.SnapshotFinish)
		}
	}
}

func (n *Node) maybeCompact() error {
	if n.Status().Lead != n.id {
		return nil
	}

	first, err := n.storage.FirstIndex()
	if err != nil {
		return err
	}
	last, err := n.storage.LastIndex()
	if err != nil {
		return err
	}
	if last <= first || last-first < snapshotThreshold {
		return nil
	}

	applied := n.lastApplied.Load()
	if applied < first {
		return nil
	}

	if err := n.storage.Compact(applied); err != nil {
		return err
	}
	return nil
}

func (n *Node) sendRaftMessage(msg raftpb.Message) error {
	if msg.To == n.id {
		return n.raftNode.Step(context.Background(), msg)
	}

	if n.transport == nil {
		return n.raftNode.Step(context.Background(), msg)
	}

	if err := n.transport.Send(context.Background(), msg); err != nil {
		logSendError(n, msg.To, err)
	}

	return nil
}

// Step applies an inbound raft message from a peer (or loopback).
func (n *Node) Step(ctx context.Context, msg raftpb.Message) error {
	return n.raftNode.Step(ctx, msg)
}

// Appies committed entries to the application
func (n *Node) applyCommitted(ent raftpb.Entry) error {
	switch ent.Type {
	case raftpb.EntryNormal:
		if len(ent.Data) > 0 {
			if err := kv.Apply(n.engine, ent.Data); err != nil {
				return err
			}
		}
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err != nil {
			return fmt.Errorf("unmarshal conf change: %w", err)
		}
		cs := n.raftNode.ApplyConfChange(cc)
		if cs != nil {
			if err := n.storage.SaveConfState(*cs); err != nil {
				return err
			}
		}
	}

	n.lastApplied.Store(ent.Index)
	n.storage.SetAppliedIndex(ent.Index)

	return nil
}

// Propose appends a command to the raft log (Task 6). Data must be non-empty for RaftLog.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.raftNode.Propose(ctx, data)
}

func (n *Node) Set(ctx context.Context, key string, value []byte) error {
	//Only leader may propose
	if n.Status().Lead != n.id {
		return fmt.Errorf("node %d: not leader (leader=%d)", n.id, n.Status().Lead)
	}

	data, err := kv.EncodeSet(key, value)
	if err != nil {
		return err
	}

	beforeLast, err := n.storage.LastIndex()
	if err != nil {
		return err
	}

	if err := n.Propose(ctx, data); err != nil {
		return err
	}

	return n.waitUntilCaughtUp(ctx, beforeLast)
}

func (n *Node) Delete(ctx context.Context, key string) error {
	//Only leader may propose
	if n.Status().Lead != n.id {
		return fmt.Errorf("node %d: not leader (leader=%d)", n.id, n.Status().Lead)
	}

	data, err := kv.EncodeDelete(key)
	if err != nil {
		return err
	}

	beforeLast, err := n.storage.LastIndex()
	if err != nil {
		return err
	}

	if err := n.Propose(ctx, data); err != nil {
		return err
	}

	return n.waitUntilCaughtUp(ctx, beforeLast)
}

func (n *Node) Get(key string) ([]byte, bool, error) {
	return n.engine.Get(key)
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

func (n *Node) waitUntilCaughtUp(ctx context.Context, prevLast uint64) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			last, err := n.storage.LastIndex()
			if err != nil {
				return err
			}
			applied := n.lastApplied.Load()
			if last > prevLast && applied >= last {
				return nil
			}
		}
	}
}

// Status returns the current raft status (leader, term, etc.).
func (n *Node) Status() raft.Status {
	return n.raftNode.Status()
}

// Returns the underlying storage
func (n *Node) Storage() *Storage {
	return n.storage
}

func (n *Node) ID() uint64 { return n.id }

// Stop shuts down tick + Ready loops and closes storage.
func (n *Node) Stop() {
	select {
	case <-n.stopc:
		// already stopping
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
