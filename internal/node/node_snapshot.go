package node

import (
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
	"github.com/carissaayo/go-kv-dist/internal/kv"
)

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

	return n.storage.Compact(applied)
}
