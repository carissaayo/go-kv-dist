package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
	"github.com/carissaayo/go-kv-dist/internal/kv"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestStorage_SnapshotAndCompact(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const nodeID uint64 = 1

	engCfg := engine.DefaultConfig(dir)
	engCfg.SyncPolicy = engine.SyncAlways
	eng, err := engine.Open(engCfg)
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	defer eng.Close()

	s, err := OpenStorage(dir, nodeID)
	if err != nil {
		t.Fatalf("OpenStorage: %v", err)
	}
	defer s.Close()
	s.SetEngine(eng)

	for i := uint64(1); i <= 5; i++ {
		ent := raftpb.Entry{Index: i, Term: 1, Type: raftpb.EntryNormal, Data: []byte("x")}
		if err := s.Append([]raftpb.Entry{ent}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := eng.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	s.SetAppliedIndex(5)

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Metadata.Index != 5 {
		t.Fatalf("snap index = %d, want 5", snap.Metadata.Index)
	}
	data, err := kv.DecodeState(snap.Data)
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	if string(data["k"]) != "v" {
		t.Fatalf("snap data = %q, want v", data["k"])
	}

	if err := s.Compact(5); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	first, _ := s.FirstIndex()
	if first != 6 {
		t.Fatalf("FirstIndex = %d, want 6", first)
	}
	if _, err := s.Entries(1, 6, 0); err != raft.ErrCompacted {
		t.Fatalf("Entries(1,6) err = %v, want ErrCompacted", err)
	}
}

func TestNode_InstallSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	n, err := NewNode(dir, 1, Options{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer n.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := waitForLeader(ctx, n); err != nil {
		t.Fatalf("waitForLeader: %v", err)
	}

	payload, err := kv.EncodeState(map[string][]byte{"snap-key": []byte("snap-val")})
	if err != nil {
		t.Fatalf("EncodeState: %v", err)
	}

	snap := raftpb.Snapshot{
		Data: payload,
		Metadata: raftpb.SnapshotMetadata{
			Index: 10,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1},
			},
		},
	}

	if err := n.installSnapshot(snap); err != nil {
		t.Fatalf("installSnapshot: %v", err)
	}

	val, found, err := n.Get("snap-key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(val) != "snap-val" {
		t.Fatalf("Get(snap-key) = (%q, %v), want snap-val", val, found)
	}

	first, _ := n.storage.FirstIndex()
	if first != 11 {
		t.Fatalf("FirstIndex = %d, want 11", first)
	}
}

func TestCluster_LaggingFollowerSnapshot(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	leader := c.waitForLeader(ctx)

	follower := c.findFollower()
	if follower == nil {
		t.Fatal("no follower")
	}
	followerID := follower.ID()
	c.stopMember(followerID)

	// Enough writes to trigger leader compaction (snapshotThreshold=64).
	for i := 0; i < 80; i++ {
		key := fmt.Sprintf("lag-%d", i)
		if err := leader.Set(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	c.restartMember(followerID)
	rejoined := c.node(followerID)

	// Wait for leader election again after member returns.
	if err := waitForLeader(ctx, rejoined); err != nil {
		t.Fatalf("follower rejoined raft: %v", err)
	}

	waitUntilGet(ctx, t, rejoined, "lag-0", "v")
	waitUntilGet(ctx, t, rejoined, "lag-79", "v")
}
