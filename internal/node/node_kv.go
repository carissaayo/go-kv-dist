package node

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-kv-dist/internal/kv"
)

// Applies committed entries to the KV engine and raft conf state.
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
	n.logEntryApplied(ent)

	return nil
}

func (n *Node) proposeWrite(ctx context.Context, data []byte) error {
	if n.Status().Lead != n.id {
		err := fmt.Errorf("node %d: not leader (leader=%d)", n.id, n.Status().Lead)
		n.recordProposalFailed("not_leader", err)
		return err
	}

	beforeLast, err := n.storage.LastIndex()
	if err != nil {
		n.recordProposalFailed("storage", err)
		return err
	}

	if err := n.Propose(ctx, data); err != nil {
		n.recordProposalFailed("propose", err)
		return err
	}

	if err := n.waitUntilCaughtUp(ctx, beforeLast); err != nil {
		n.recordProposalFailed("timeout", err)
		return err
	}

	n.recordProposalOK()
	return nil
}

func (n *Node) Set(ctx context.Context, key string, value []byte) error {
	data, err := kv.EncodeSet(key, value)
	if err != nil {
		return err
	}
	return n.proposeWrite(ctx, data)
}

func (n *Node) Delete(ctx context.Context, key string) error {
	data, err := kv.EncodeDelete(key)
	if err != nil {
		return err
	}
	return n.proposeWrite(ctx, data)
}

func (n *Node) Get(key string) ([]byte, bool, error) {
	return n.engine.Get(key)
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
