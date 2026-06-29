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

	return nil
}

func (n *Node) Set(ctx context.Context, key string, value []byte) error {
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
