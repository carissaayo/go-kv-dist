package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

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
