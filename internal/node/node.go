package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
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

// NewNode opens storage and starts the raft node, tick loop, and Ready loop.
func NewNode(dataDir string, id uint64) (*Node, error) {
	storage, err := OpenStorage(dataDir, id)
	if err != nil {
		return nil, fmt.Errorf("node: open storage: %w", err)
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
		_ = storage.Close()
		return nil, fmt.Errorf("node: last index: %w", err)
	}
	var rn raft.Node
	if last == 0 {
		rn = raft.StartNode(cfg, []raft.Peer{{ID: id}})
	} else {
		rn = raft.RestartNode(cfg)
	}
	n := &Node{
		id:       id,
		dataDir:  dataDir,
		storage:  storage,
		raftNode: rn,
		stopc:    make(chan struct{}),
		donec:    make(chan struct{}),
	}
	go n.tickLoop()
	go n.runReadyLoop()
	return n, nil
}

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
func (n *Node) processReady(rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		// Phase 5: install snapshot into storage + state machine.
		return fmt.Errorf("unexpected snapshot at index %d", rd.Snapshot.Metadata.Index)
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
		if err := n.raftNode.Step(context.Background(), msg); err != nil {
			return fmt.Errorf("step message: %w", err)
		}
	}
	return nil
}
func (n *Node) applyCommitted(ent raftpb.Entry) error {
	switch ent.Type {
	case raftpb.EntryNormal:
		if len(ent.Data) == 0 {
			return nil
		}
		// Phase 2: decode command and call engine Set/Delete.
		return nil
	case raftpb.EntryConfChange:
		// Phase 3+: update ConfState in raft_meta when membership changes.
		return nil
	default:
		return nil
	}
}

// Propose appends a command to the raft log (Task 6). Data must be non-empty for RaftLog.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.raftNode.Propose(ctx, data)
}

// Status returns the current raft status (leader, term, etc.).
func (n *Node) Status() raft.Status {
	return n.raftNode.Status()
}

// Storage returns the underlying storage (tests / inspection).
func (n *Node) Storage() *Storage {
	return n.storage
}

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
	_ = n.storage.Close()
}
